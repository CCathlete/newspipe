"""feedparser.pyi"""

from typing import Any, Dict

class FeedParserDict(Dict[str, Any]):
    """Stub for FeedParserDict"""

def parse(
    url_file_stream_or_string: str | bytes | Any,
    etag: str | None = None,
    modified: str | None = None,
    agent: str | None = None,
    referrer: str | None = None,
    handlers: Any | None = None,
    request_headers: Dict[str, str] | None = None,
    response_headers: Dict[str, str] | None = None,
    resolve_relative_uris: bool | None = None,
    sanitize_html: bool | None = None,
) -> FeedParserDict:
    """Stub for parse"""
