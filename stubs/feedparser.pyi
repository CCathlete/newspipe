# stubs/feedparser.pyi

from typing import Any, List, Dict, TypedDict, Optional, Union, Tuple

# --- 1. FeedEntry Definition (The article/item structure) ---
# We use the normalized names from the RIGHT side of the keymap.
# We set total=False because not every entry will have every field.
class FeedEntry(TypedDict, total=False):
    # Core Fields
    id: Optional[str]  # Normalized from 'guid'
    title: Optional[str]
    link: Optional[str]

    # Content/Summary Fields
    summary: Optional[str]  # Normalized from 'description'
    subtitle: Optional[str]  # Normalized from 'tagline', part of 'description'

    # Date Fields
    updated: Optional[str]  # Normalized from 'date', 'modified'
    updated_parsed: Optional[
        Tuple[int, ...]
    ]  # Normalized from 'date_parsed', 'modified_parsed' (time.struct_time)
    published: Optional[str]  # Normalized from 'issued'
    published_parsed: Optional[Tuple[int, ...]]  # Normalized from 'issued_parsed'

    # Detail Objects (Nested dictionaries containing type, language, value)
    summary_detail: Optional[Dict[str, Any]]
    subtitle_detail: Optional[Dict[str, Any]]

    # Other common fields
    tags: List[Dict[str, Any]]  # Categories
    authors: List[Dict[str, Any]]

# --- 2. FeedParserDict Definition (The top-level container) ---
# This shadows the original class, explicitly defining the key attributes.
class FeedParserDict(Dict[str, Any]):
    # The crucial attribute for accessing the list of articles:
    entries: List[FeedEntry]  # Normalized from 'items'

    # The channel information, often needed for metadata:
    feed: Dict[str, Any]  # Normalized from 'channel'

    # Other common top-level attributes
    version: str  # The feed type/version string
    encoding: str

# --- 3. The Parse Function Signature ---
# Tells the type checker what the function takes and what it returns.
def parse(
    url_file_stream_or_string: Union[str, bytes, Any],
    etag: Optional[str] = None,
    modified: Optional[str] = None,
    agent: Optional[str] = None,
    referrer: Optional[str] = None,
    handlers: Optional[List[Any]] = None,
    request_headers: Optional[Dict[str, str]] = None,
    response_headers: Optional[Dict[str, str]] = None,
    resolve_relative_uris: Optional[bool] = None,
    sanitize_html: Optional[bool] = None,
) -> FeedParserDict: ...
