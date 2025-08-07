import contextlib
import sys
import os
import urllib.parse
import zipfile
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from mcp.server.sse import SseServerTransport
from starlette.requests import Request
from starlette.routing import Mount, Route
from starlette.types import Receive, Scope, Send
from mcp.server import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from markitdown import MarkItDown
import uvicorn

# Initialize FastMCP server for MarkItDown (SSE)
mcp = FastMCP("markitdown")


@mcp.tool()
async def convert_to_markdown(uri: str) -> str:
    """Convert a resource described by an http:, https:, file: or data: URI to markdown"""
    return MarkItDown(enable_plugins=check_plugins_enabled()).convert_uri(uri).markdown


@mcp.tool()
async def save_uri_as_markdown(uri: str, output_path: str) -> str:
    """Convert a resource (http/https/file/data URI) to Markdown and save it to output_path (.txt or .md)."""
    markdown_text = (
        MarkItDown(enable_plugins=check_plugins_enabled()).convert_uri(uri).markdown
    )
    # Ensure output directory exists
    abs_output_path = os.path.abspath(output_path)
    output_dir = os.path.dirname(abs_output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Write UTF-8 with LF newlines for portability
    with open(abs_output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(markdown_text)

    byte_len = len(markdown_text.encode("utf-8"))
    return f"Saved {byte_len} bytes to {abs_output_path}"


def _file_path_from_uri(uri: str) -> str | None:
    """Return local filesystem path from a file: URI. Windows-friendly."""
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme != "file":
        return None
    path = urllib.parse.unquote(parsed.path)
    # On Windows file:///C:/... becomes "/C:/..."; strip leading slash
    if os.name == "nt" and path.startswith("/") and len(path) > 3 and path[2] == ":":
        path = path[1:]
    return os.path.normpath(path)


@mcp.tool()
async def save_spreadsheet_with_formulas(
    uri: str,
    output_path: str,
    sheets: str | None = None,
) -> str:
    """Convert a spreadsheet to Markdown, appending a section that includes Excel formulas and cached values.

    - Supports file: URIs pointing to .xlsx/.xlsm/.xltx/.xltm
    - For non-spreadsheet URIs, saves normal Markdown only
    """
    md = MarkItDown(enable_plugins=check_plugins_enabled()).convert_uri(uri).markdown

    local_path = _file_path_from_uri(uri)
    formulas_section = ""

    if local_path and os.path.splitext(local_path)[1].lower() in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        sections: list[str] = ["\n\n---\n\n## Formulas"]
        sheet_filter: set[str] | None = None
        if sheets:
            sheet_filter = {s.strip() for s in sheets.split(",") if s.strip()}

        # Stream-parse the .xlsx to extract formulas and cached values without loading full sheets
        with zipfile.ZipFile(local_path, "r") as zf:
            # Map sheet name -> xml path
            name_to_xml = _map_sheet_names_to_xml_paths(zf)

            # Load shared strings once (for string cached results)
            shared_strings = _load_shared_strings(zf)

            for sheet_name, xml_path in name_to_xml.items():
                if sheet_filter and sheet_name not in sheet_filter:
                    continue

                lines: list[str] = []
                lines.append("| Cell | Formula | Cached Value |")
                lines.append("| --- | --- | --- |")

                for coord, formula, cached_val in _iter_sheet_formulas(zf, xml_path, shared_strings):
                    ft = (formula or "").replace("|", "\\|")
                    cv = ("" if cached_val is None else str(cached_val)).replace("|", "\\|")
                    lines.append(f"| {coord} | `{ft}` | {cv} |")

                if len(lines) > 2:
                    sections.append(f"\n### Sheet: {sheet_name}\n" + "\n".join(lines))

        if len(sections) > 1:
            formulas_section = "".join(sections)

    final_markdown = md + formulas_section

    # Ensure output directory exists and write
    abs_output_path = os.path.abspath(output_path)
    output_dir = os.path.dirname(abs_output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    with open(abs_output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(final_markdown)

    return f"Saved {len(final_markdown.encode('utf-8'))} bytes to {abs_output_path}"


def check_plugins_enabled() -> bool:
    return os.getenv("MARKITDOWN_ENABLE_PLUGINS", "false").strip().lower() in (
        "true",
        "1",
        "yes",
    )


# XML namespaces used in .xlsx files
MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
OFFICE_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _map_sheet_names_to_xml_paths(zf: zipfile.ZipFile) -> dict[str, str]:
    """Return mapping of sheet name -> zip path for that sheet's XML."""
    result: dict[str, str] = {}
    try:
        with zf.open("xl/workbook.xml") as f:
            tree = ET.parse(f)
    except KeyError:
        return result

    root = tree.getroot()
    sheets = root.find(f"{{{MAIN_NS}}}sheets")
    if sheets is None:
        return result

    # Build rId -> Target from relationships
    rels: dict[str, str] = {}
    try:
        with zf.open("xl/_rels/workbook.xml.rels") as rf:
            rtree = ET.parse(rf)
            rroot = rtree.getroot()
            for rel in rroot.findall(f"{{{PKG_REL_NS}}}Relationship"):
                rid = rel.attrib.get("Id")
                target = rel.attrib.get("Target")
                if rid and target:
                    # Normalize path under xl/
                    target_path = target.replace("\\", "/")
                    if not target_path.startswith("/"):
                        target_path = f"xl/{target_path}"
                    else:
                        target_path = target_path.lstrip("/")
                    rels[rid] = target_path
    except KeyError:
        pass

    for sheet in sheets.findall(f"{{{MAIN_NS}}}sheet"):
        name = sheet.attrib.get("name")
        rid = sheet.attrib.get(f"{{{OFFICE_REL_NS}}}id")
        if not name or not rid:
            continue
        target = rels.get(rid)
        if target:
            result[name] = target
    return result


def _load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    """Parse sharedStrings.xml into a list of strings (by index)."""
    strings: list[str] = []
    try:
        with zf.open("xl/sharedStrings.xml") as f:
            # iterparse to reduce memory
            for event, elem in ET.iterparse(f, events=("end",)):
                if elem.tag == f"{{{MAIN_NS}}}si":
                    text = "".join(elem.itertext())
                    strings.append(text)
                    elem.clear()
    except KeyError:
        pass
    return strings


def _decode_cell_value(value_text: str | None, t_attr: str | None, shared_strings: list[str]) -> str | None:
    if value_text is None:
        return None
    if t_attr == "s":
        try:
            idx = int(float(value_text))
            if 0 <= idx < len(shared_strings):
                return shared_strings[idx]
        except Exception:
            return value_text
    # boolean
    if t_attr == "b":
        return "TRUE" if value_text in ("1", "true", "True") else "FALSE"
    # inline string or formula string
    if t_attr in ("str", "inlineStr"):
        return value_text
    # default: numeric or general
    return value_text


def _iter_sheet_formulas(
    zf: zipfile.ZipFile, xml_path: str, shared_strings: list[str]
) -> tuple[str, str, str | None]:
    """Yield (cell_ref, formula_text, cached_value) for each formula cell in sheet XML.

    This uses a streaming XML parser to avoid loading entire sheets.
    """
    try:
        f = zf.open(xml_path)
    except KeyError:
        return iter(())  # type: ignore

    def gen():
        current_ref: str | None = None
        current_type: str | None = None
        current_formula: str | None = None
        current_value: str | None = None
        for event, elem in ET.iterparse(f, events=("start", "end")):
            if event == "start" and elem.tag == f"{{{MAIN_NS}}}c":
                current_ref = elem.attrib.get("r")
                current_type = elem.attrib.get("t")
                current_formula = None
                current_value = None
            elif event == "end":
                if elem.tag == f"{{{MAIN_NS}}}f":
                    # Formula text
                    txt = elem.text or ""
                    if txt and not txt.startswith("="):
                        txt = "=" + txt
                    current_formula = txt
                    elem.clear()
                elif elem.tag == f"{{{MAIN_NS}}}v":
                    current_value = elem.text
                    elem.clear()
                elif elem.tag == f"{{{MAIN_NS}}}c":
                    if current_ref and current_formula is not None:
                        yield (
                            current_ref,
                            current_formula,
                            _decode_cell_value(current_value, current_type, shared_strings),
                        )
                    elem.clear()
                    current_ref = None
        try:
            f.close()
        except Exception:
            pass

    return gen()


def create_starlette_app(mcp_server: Server, *, debug: bool = False) -> Starlette:
    sse = SseServerTransport("/messages/")
    session_manager = StreamableHTTPSessionManager(
        app=mcp_server,
        event_store=None,
        json_response=True,
        stateless=True,
    )

    async def handle_sse(request: Request) -> None:
        async with sse.connect_sse(
            request.scope,
            request.receive,
            request._send,
        ) as (read_stream, write_stream):
            await mcp_server.run(
                read_stream,
                write_stream,
                mcp_server.create_initialization_options(),
            )

    async def handle_streamable_http(
        scope: Scope, receive: Receive, send: Send
    ) -> None:
        await session_manager.handle_request(scope, receive, send)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        """Context manager for session manager."""
        async with session_manager.run():
            print("Application started with StreamableHTTP session manager!")
            try:
                yield
            finally:
                print("Application shutting down...")

    return Starlette(
        debug=debug,
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/mcp", app=handle_streamable_http),
            Mount("/messages/", app=sse.handle_post_message),
        ],
        lifespan=lifespan,
    )


# Main entry point
def main():
    import argparse

    mcp_server = mcp._mcp_server

    parser = argparse.ArgumentParser(description="Run a MarkItDown MCP server")

    parser.add_argument(
        "--http",
        action="store_true",
        help="Run the server with Streamable HTTP and SSE transport rather than STDIO (default: False)",
    )
    parser.add_argument(
        "--sse",
        action="store_true",
        help="(Deprecated) An alias for --http (default: False)",
    )
    parser.add_argument(
        "--host", default=None, help="Host to bind to (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=None, help="Port to listen on (default: 3001)"
    )
    args = parser.parse_args()

    use_http = args.http or args.sse

    if not use_http and (args.host or args.port):
        parser.error(
            "Host and port arguments are only valid when using streamable HTTP or SSE transport (see: --http)."
        )
        sys.exit(1)

    if use_http:
        starlette_app = create_starlette_app(mcp_server, debug=True)
        uvicorn.run(
            starlette_app,
            host=args.host if args.host else "127.0.0.1",
            port=args.port if args.port else 3001,
        )
    else:
        mcp.run()


if __name__ == "__main__":
    main()
