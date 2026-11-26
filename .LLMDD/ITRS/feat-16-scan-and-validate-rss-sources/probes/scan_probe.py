"""Evidence probes for the RSS source-scanning/validation feature hypothesis.

Each subcommand prints exactly one JSON object: {"passed": bool, "detail": str}.
No ledger writes: the Hypothesis Engine owns the ledger.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Sequence

ROOT: Path = Path(__file__).resolve().parents[4]
INGEST: Path = ROOT / "pipelines" / "ingestion"
MAIN: Path = INGEST / "src" / "main" / "scala" / "newspipe"
PORTS: Path = MAIN / "application" / "ports" / "Ports.scala"
SERVICE: Path = MAIN / "application" / "services" / "IngestionService.scala"
CLIENT: Path = MAIN / "infrastructure" / "rss" / "RomeRssClient.scala"
BUILD: Path = INGEST / "build.sbt"
FEEDS: Path = ROOT / "conf" / "feeds.conf"
ENRICH_CONF: Path = ROOT / "conf" / "enrichment.conf"
DOTENV: Path = ROOT / ".env"
LAUNCHER: Path = ROOT / "scripts" / "newspipe"
ENRICH_CLI: Path = ROOT / "pipelines" / "enrichment" / "src" / "main" / "scala" / "newspipe" / "infrastructure" / "llm-data-enricher" / "OpenCodeCliClient.scala"


def emit(passed: bool, detail: str) -> int:
    """Print the structured result and return the process exit code."""
    sys.stdout.write(json.dumps({"passed": passed, "detail": detail}) + "\n")
    return 0 if passed else 1


def text(path: Path) -> str:
    """Read a UTF-8 file, or return empty text when it is absent."""
    return path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""


def probe_port_extension_point() -> int:
    """The collection path already enters through an application-layer port."""
    ports: str = text(PORTS)
    service: str = text(SERVICE)
    passed: bool = "trait RssSource" in ports and "def fetch(): List[RssItem]" in ports and "rss.fetch()" in service
    return emit(
        passed,
        "RssSource.fetch() feeds IngestionService; validated feeds can be supplied through the existing port"
        if passed
        else "RssSource port or IngestionService call missing",
    )


def probe_deterministic_parser_reuse() -> int:
    """The existing Rome/HttpClient adapter already does deterministic fetch+parse."""
    client: str = text(CLIENT)
    passed: bool = all(
        token in client
        for token in ("SyndFeedInput", "HttpClient", "followRedirects", "timeout", "getEntries")
    )
    return emit(
        passed,
        "RomeRssClient already uses HttpClient (redirects/timeout) plus SyndFeedInput entry parsing"
        if passed
        else "RomeRssClient lacks one of HttpClient/redirect/timeout/SyndFeedInput/getEntries",
    )


def probe_dependencies_already_present() -> int:
    """Manifest/result handling needs no new queue, database or JSON library."""
    build: str = text(BUILD)
    needed: Sequence[str] = ("circe-parser", "circe-core", "\"com.typesafe\" % \"config\"")
    passed: bool = all(token in build for token in needed)
    return emit(
        passed,
        "circe JSON and Typesafe config already on the ingestion classpath"
        if passed
        else "missing dependency tokens: " + ",".join(token for token in needed if token not in build),
    )


def probe_manifest_gap() -> int:
    """The current feeds.conf is a flat list: manifest ingestion is genuinely new."""
    feeds: str = text(FEEDS)
    passed: bool = "feeds = [" in feeds and "publisher" not in feeds and "provenance" not in feeds
    return emit(
        passed,
        "conf/feeds.conf holds a flat feed list with no publisher/provenance structure"
        if passed
        else "feeds.conf shape unexpected or already carries provenance",
    )


def probe_lifecycle_in_process() -> int:
    """NewsPipe runs pipelines in-process; no external scheduler is present."""
    launcher: str = text(LAUNCHER)
    forbidden: re.Pattern[str] = re.compile(r"cron|APScheduler|schedule\.|Timer\(|while True")
    hits: Sequence[str] = tuple(match.group(0) for match in forbidden.finditer(launcher))
    passed: bool = '"sbt", "run"' in launcher and not hits
    return emit(
        passed,
        "launcher starts pipelines directly with sbt run; no cron/scheduler/while-True loop"
        if passed
        else "sbt run missing or scheduler tokens found: " + ",".join(hits[:5]),
    )


def probe_determinism_and_models() -> int:
    """Scanning can stay LLM-free while enrichment uses non-opencode models."""
    src: str = ""
    scala_file: Path
    for scala_file in MAIN.rglob("*.scala"):
        src += text(scala_file)
    ingest_llm: bool = any(token in src for token in ("opencode", "ProcessBuilder", "OpenCode"))
    conf: str = text(ENRICH_CONF)
    env: str = "\n".join(line for line in text(DOTENV).splitlines() if not line.lstrip().startswith("#"))
    models: Sequence[str] = (
        "nvidia/nvidia/nemotron-3.5-lightning-30b-a3b",
        "nvidia/poolside/laguna-xs-2.1",
    )
    conf_ok: bool = all(model in conf for model in models) and "opencode/" not in conf
    env_ok: bool = "opencode/" not in env
    cli: str = text(ENRICH_CLI)
    passed: bool = not ingest_llm and conf_ok and env_ok and 'ProcessBuilder(binary, "run"' in cli
    return emit(
        passed,
        "no LLM call in ingestion; enrichment chain is nvidia/* in conf and .env; per-item opencode run remains in enrichment only"
        if passed
        else f"ingest_llm={ingest_llm} conf_ok={conf_ok} env_ok={env_ok} per_item_cli={'ProcessBuilder(binary, \"run\"' in cli}",
    )


def probe_status_types_absent() -> int:
    """No validation status model exists yet, so new domain types are creation work."""
    src: str = ""
    scala_file: Path
    for scala_file in MAIN.rglob("*.scala"):
        src += text(scala_file)
    found: Sequence[str] = tuple(
        token for token in ("ValidationStatus", "SourceValidation", "FeedStatus") if token in src
    )
    passed: bool = not found
    return emit(
        passed,
        "no validation status type in ingestion main sources; new domain model is creation work"
        if passed
        else "status types already present: " + ",".join(found),
    )


COMMANDS: dict[str, object] = {
    "port": probe_port_extension_point,
    "parser": probe_deterministic_parser_reuse,
    "deps": probe_dependencies_already_present,
    "manifest": probe_manifest_gap,
    "lifecycle": probe_lifecycle_in_process,
    "models": probe_determinism_and_models,
    "status": probe_status_types_absent,
}


def main(argv: Sequence[str]) -> int:
    """Dispatch one subcommand; unknown names fail closed."""
    if len(argv) != 2 or argv[1] not in COMMANDS:
        return emit(False, "usage: scan_probe.py <" + "|".join(COMMANDS.keys()) + ">")
    handler = COMMANDS[argv[1]]
    assert callable(handler)
    result: int = handler()
    return result


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
