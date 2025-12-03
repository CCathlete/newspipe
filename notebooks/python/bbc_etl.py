"""
Spark job for pulling BBC RSS data into the bronze layer.
"""

import json
from datetime import datetime
from typing import List, Dict, Any
import requests
import feedparser
from feedparser import FeedParserDict
import doggopyr


# --- Configuration ---
RSS_FEED_URL = "http://feeds.bbci.co.uk/news/world/rss.xml"
FEED_SOURCE = "BBC_World_News"
# This will be the file saved to MinIO
OUTPUT_FILENAME = f"{FEED_SOURCE}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"


def fetch_and_parse_feed(url: str, source_name: str) -> List[Dict[str, Any]]:
    """Fetches an RSS feed, parses it, and returns a list of structured items."""
    print(f"Fetching feed from: {url}")

    try:
        # Use requests to fetch the content
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise exception for bad status codes (4xx or 5xx)

        # Use feedparser to parse the XML content
        assert response.content, "Response content is empty"
        feed: FeedParserDict = feedparser.parse(response.content)

        parsed_items: List[Dict[str, Any]] = []
        for entry in feed.entries:
            # 1. Standardize the date format (important for Spark schema consistency)
            # feedparser returns a time.struct_time object
            published_dt = entry.get("published_parsed")
            pub_date_str = (
                datetime(*published_dt[:6], tzinfo=None).strftime(
                    "%Y-%m-%d %H:%M:%S",
                )
                if published_dt
                else None
            )

            # 2. Extract and structure the data
            item: Dict[str, Any] = {
                "feed_source": source_name,
                "title": entry.get("title"),
                "link": entry.get("link"),
                "description": entry.get("summary"),  # Often contains the main snippet
                "category": [
                    tag["term"] for tag in entry.get("tags", [])
                ],  # Handle multiple tags
                "published_at": pub_date_str,
                "ingestion_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            parsed_items.append(item)

        print(f"Successfully parsed {len(parsed_items)} items.")
        return parsed_items

    except requests.exceptions.RequestException as e:
        print(f"Error fetching RSS feed: {e}")
        return []


def main():
    """
    Entry point for the Spark job.
    """
    # 1. Fetch and Parse
    news_items = fetch_and_parse_feed(RSS_FEED_URL, FEED_SOURCE)

    # 2. Save the data (in a real Airflow task, this would use a hook to upload to MinIO)
    # For a local test, we'll save it to a local JSON file.

    if news_items:
        with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f:
            # Write each JSON object on a new line (JSON Lines format) for easy reading by PySpark
            for item in news_items:
                f.write(json.dumps(item) + "\n")

        print(f"\n✅ Data saved to: {OUTPUT_FILENAME}")
        print("Next step: Use PySpark to read this file from MinIO and process it.")


if __name__ == "__main__":
    main()
