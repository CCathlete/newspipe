"""Advisor approval-token gate (bugfix-feature-5).

Checks a Designer-issued token against a proposed code change.
Exit 0: token valid, unexpired, and in scope. Exit 1: denied, reason on
stderr. Exit 2: usage error.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TypeAlias

TokenFiles: TypeAlias = list[str]
TokenCommands: TypeAlias = list[str]

REQUIRED_KEYS: tuple[str, str, str, str, str, str] = ("id", "files", "commands", "reason", "granted_by", "expires")


@dataclass(slots=True, frozen=True)
class ApprovalToken:
    token_id: str
    files: TokenFiles
    commands: TokenCommands
    reason: str
    granted_by: str
    expires: date


def approvals_dir() -> Path:
    script_dir: Path = Path(__file__).resolve().parent
    repo_root: Path = script_dir.parent.parent
    result: Path = repo_root / ".LLMDD" / "approvals"
    return result


def resolve_token_path(approval: str) -> Path:
    direct: Path = Path(approval)
    if approval.endswith(".json") or "/" in approval:
        return direct
    result: Path = approvals_dir() / (approval + ".json")
    return result


def load_token(path: Path) -> ApprovalToken | str:
    raw_text: str
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError:
        return "token file not found: " + str(path)
    decoded: object
    try:
        decoded = json.loads(raw_text)
    except json.JSONDecodeError:
        return "token file is not valid JSON: " + str(path)
    if not isinstance(decoded, dict):
        return "token file must hold a JSON object: " + str(path)
    absent: list[str] = [key for key in REQUIRED_KEYS if key not in decoded]
    if absent:
        return "token file misses keys: " + ",".join(absent)
    files_value: object = decoded["files"]
    commands_value: object = decoded["commands"]
    id_value: object = decoded["id"]
    reason_value: object = decoded["reason"]
    granted_value: object = decoded["granted_by"]
    expires_value: object = decoded["expires"]
    checked_files: TokenFiles
    if isinstance(files_value, list) and all(isinstance(v, str) for v in files_value):
        checked_files = list(files_value)
    else:
        return "token files must be a list of strings"
    checked_commands: TokenCommands
    if isinstance(commands_value, list) and all(isinstance(v, str) for v in commands_value):
        checked_commands = list(commands_value)
    else:
        return "token commands must be a list of strings"
    if not isinstance(id_value, str) or not isinstance(reason_value, str):
        return "token id and reason must be strings"
    if not isinstance(granted_value, str) or not isinstance(expires_value, str):
        return "token granted_by and expires must be strings"
    expiry: date
    try:
        expiry = date.fromisoformat(expires_value)
    except ValueError:
        return "token expires must use YYYY-MM-DD: " + expires_value
    token: ApprovalToken = ApprovalToken(
        token_id=id_value,
        files=checked_files,
        commands=checked_commands,
        reason=reason_value,
        granted_by=granted_value,
        expires=expiry,
    )
    return token


def file_in_scope(path: str, scope: TokenFiles) -> bool:
    entry: str
    for entry in scope:
        if entry.endswith("/*"):
            stem: str = entry[:-2]
            if path == stem or path.startswith(stem + "/"):
                return True
        elif path == entry:
            return True
    return False


def command_in_scope(command: str, scope: TokenCommands) -> bool:
    entry: str
    for entry in scope:
        if entry.endswith(" *"):
            stem: str = entry[:-2]
            if command == stem or command.startswith(stem + " "):
                return True
        elif command == entry:
            return True
    return False


def check(token: ApprovalToken, files: list[str], commands: list[str]) -> str | None:
    today: date = date.today()
    if token.expires < today:
        return "token " + token.token_id + " expired " + token.expires.isoformat()
    candidate: str
    for candidate in files:
        if not file_in_scope(candidate, token.files):
            return "file out of scope: " + candidate
    for candidate in commands:
        if not command_in_scope(candidate, token.commands):
            return "command out of scope: " + candidate
    return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Validate an Advisor approval token against a proposed change."
    )
    parser.add_argument(
        "--approval",
        required=True,
        help="Token id under .LLMDD/approvals/ or a direct token file path.",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        default=[],
        help="Changed tracked files covered by the change.",
    )
    parser.add_argument(
        "--commands",
        nargs="*",
        default=[],
        help="Write commands covered by the change.",
    )
    args: argparse.Namespace = parser.parse_args(argv)
    return args


def main(argv: list[str] | None = None) -> int:
    args: argparse.Namespace = parse_args(argv)
    approval: str = str(args.approval)
    wanted_files: list[str] = [str(v) for v in args.files]
    wanted_commands: list[str] = [str(v) for v in args.commands]
    token_path: Path = resolve_token_path(approval)
    loaded: ApprovalToken | str = load_token(token_path)
    if isinstance(loaded, str):
        sys.stderr.write("DENIED: " + loaded + "\n")
        return 1
    denial: str | None = check(loaded, wanted_files, wanted_commands)
    if denial is not None:
        sys.stderr.write("DENIED: " + denial + "\n")
        return 1
    sys.stdout.write("ALLOWED: " + loaded.token_id + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
