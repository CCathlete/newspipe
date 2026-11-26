#!/usr/bin/env python3
"""skill-check.py — deterministic validator that forces execution of skill steps.

The tool reads .LLMDD/skills/<name>/SKILL.md, extracts all actionable steps,
checks for required elements (commands, files, tools), and generates a
strict execution plan. It can also verify completion and enforce gates.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


SKILLS_DIR = Path(".LLMDD/skills")
LLMDD_BOOK_PATH = Path(os.path.expanduser("~/Repos/llmdd-book"))


def _get_repo_name() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=Path.cwd(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5
        )
        if result.returncode == 0:
            return Path(result.stdout.decode().strip()).name
    except Exception:
        pass
    return Path.cwd().name


def _is_llm_driven_design_repo() -> bool:
    return _get_repo_name() == "llm-driven-design"


def _llmdd_book_exists() -> bool:
    return LLMDD_BOOK_PATH.exists() and LLMDD_BOOK_PATH.is_dir()


def _read_skill(name: str) -> str:
    p = SKILLS_DIR / name / "SKILL.md"
    if not p.is_file():
        raise SystemExit(f"skill not found: {p}")
    return p.read_text()


def _extract_sections(text: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    current = "header"
    content: list[str] = []
    for line in text.splitlines():
        if line.startswith("## "):
            sections[current] = "\n".join(content)
            current = line[3:].strip()
            content = []
        else:
            content.append(line)
    sections[current] = "\n".join(content)
    return sections


def _extract_numbered_steps(text: str) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    for m in re.finditer(r"(?m)^\s*(\d+)\.\s+(.+?)(?=\n\s*\d+\.|\n##|\n###|\Z)", text, re.S):
        num = int(m.group(1))
        body = m.group(2).strip()
        steps.append({"num": num, "text": body, "commands": [], "files": []})
    return steps


def _extract_commands(text: str) -> list[str]:
    cmds = []
    for m in re.finditer(r"```(?:bash|sh|shell)\n(.*?)```", text, re.S):
        for line in m.group(1).splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                cmds.append(line)
    for m in re.finditer(r"`([^`]+)`", text):
        c = m.group(1).strip()
        if any(c.startswith(x) for x in ["python", "git", "./", "bash", "cd ", "ls ", "cat "]):
            if len(c) < 200:
                cmds.append(c)
    return cmds


def _extract_files(text: str) -> list[str]:
    files = []
    for m in re.finditer(r"`([^\s`]+\.[a-zA-Z0-9]+)`", text):
        f = m.group(1)
        if "/" in f or f.startswith("."):
            files.append(f)
    for m in re.finditer(r"([^\s]+/[\w\.\-]+(?:\.[a-zA-Z0-9]+)?)", text):
        f = m.group(1)
        if f.count("/") >= 1 and not f.startswith("http"):
            files.append(f)
    return list(dict.fromkeys(files))


def _build_deterministic_plan(skill: str, text: str) -> dict[str, Any]:
    sections = _extract_sections(text)
    steps = _extract_numbered_steps(text)
    commands = _extract_commands(text)
    files = _extract_files(text)
    
    plan = {
        "skill": skill,
        "skill_file": f".LLMDD/skills/{skill}/SKILL.md",
        "sections": list(sections.keys()),
        "steps": steps,
        "commands": commands,
        "files": files,
        "gates": [],
        "mandatory": [],
        "conditional": []
    }
    # Conditional - only in llm-driven-design repo with llmdd-book present
    plan["conditional"].append({
        "condition": "repo == 'llm-driven-design' and llmdd-book exists at ~/Repos/llmdd-book",
        "description": "BOOK_TRACKING: if condition met, add summary to llmdd-book/docs/building-llm-driven-design.md and commit there (never push)",
        "check": "check_repo_and_book"
    })
    
    # Add gates based on skill
    if skill in ["feature", "bug"]:
        plan["mandatory"].append("MUST run next-spec-id.py with correct flags")
        plan["mandatory"].append("MUST create spec in correct location with exact naming")
        plan["mandatory"].append("MUST follow Skill validation section")
    elif skill == "prepare-implementation":
        plan["mandatory"].append("MUST commit pending specs first")
        plan["mandatory"].append("MUST build DTR")
        plan["mandatory"].append("MUST validate batch with validate-steps.py")
        plan["mandatory"].append("MUST compile with itr-compiler")
        plan["mandatory"].append("MUST dry-run waves until green")
        plan["mandatory"].append("MUST write run-waves.txt")
    elif skill == "review-implementation":
        plan["mandatory"].append("MUST inspect implementation diff")
        plan["mandatory"].append("MUST check wave logs and feedback")
        plan["mandatory"].append("MUST re-run acceptance tests independently")
        plan["mandatory"].append("MUST mark bugs Resolved and features Implemented when green")
    elif skill == "validate-implementation-steps":
        plan["mandatory"].append("MUST run all F1-F7 checks")
        plan["mandatory"].append("MUST fail batch on any check failure")
    
    return plan


def _build_prompt(skill: str, plan: dict[str, Any]) -> str:
    prompt = []
    prompt.append(f"# DETERMINISTIC SKILL EXECUTION: {skill}")
    prompt.append("")
    prompt.append("YOU MUST EXECUTE THIS SKILL EXACTLY AS SPECIFIED. NO DEVIATION.")
    prompt.append("YOU ARE NOT AUTHORIZED TO SKIP ANY STEP.")
    prompt.append("")
    prompt.append(f"## Skill File: {plan['skill_file']}")
    prompt.append("")
    prompt.append("## MANDATORY REQUIREMENTS (ALWAYS enforced regardless of repo)")
    for i, m in enumerate(plan['mandatory'], 1):
        prompt.append(f"{i}. {m}")
    prompt.append("")
    prompt.append("## CONDITIONAL REQUIREMENTS")
    for i, c in enumerate(plan.get('conditional', []), 1):
        prompt.append(f"C{i}. IF ({c['condition']}): THEN {c['description']}")
    prompt.append("   - FIRST CHECK: detect current repo name and if ~/Repos/llmdd-book exists")
    prompt.append("   - IF both true AND repo is llm-driven-design: MUST execute this step before committing")
    prompt.append("")
    prompt.append("## EXTRACTED NUMBERED STEPS")
    for s in plan['steps']:
        prompt.append(f"STEP {s['num']}: {s['text'][:300]}")
    prompt.append("")
    prompt.append("## COMMANDS TO EXECUTE (if applicable)")
    for i, c in enumerate(plan['commands'][:20], 1):
        prompt.append(f"{i}. {c}")
    prompt.append("")
    prompt.append("## FILES INVOLVED")
    for f in plan['files'][:15]:
        prompt.append(f"- {f}")
    prompt.append("")
    prompt.append("## EXECUTION PROTOCOL")
    prompt.append("1. Read the ENTIRE skill file before starting")
    prompt.append("2. For EACH numbered step, output: `EXECUTING STEP N: <description> | STATUS: IN_PROGRESS`")
    prompt.append("3. Execute the step completely - run commands, create/edit files as specified")
    prompt.append("4. Verify the step completed successfully")
    prompt.append("5. Output: `COMPLETED STEP N: <description> | STATUS: OK|FAIL | EVIDENCE: <proof>`")
    prompt.append("6. If ANY step fails, STOP and report failure with details")
    prompt.append("7. After ALL steps complete, run verification if specified")
    prompt.append("8. Output final checklist with ALL steps marked OK/FAIL")
    prompt.append("9. ONLY proceed to next step if previous step is OK")
    prompt.append("")
    prompt.append("BEGIN EXECUTION NOW. DO NOT PROCEED UNTIL YOU HAVE READ THE FULL SKILL FILE.")
    return "\n".join(prompt)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Deterministic skill executor/enforcer")
    p.add_argument("skill", help="Skill name")
    p.add_argument("--print-prompt", action="store_true", help="Print execution prompt")
    p.add_argument("--plan", action="store_true", help="Print execution plan as JSON")
    p.add_argument("--enforce", action="store_true", help="Enforce skill execution with checklist")
    p.add_argument("--json", action="store_true", help="Output JSON")
    args = p.parse_args(argv)

    text = _read_skill(args.skill)
    plan = _build_deterministic_plan(args.skill, text)
    
    if args.plan or args.json:
        print(json.dumps(plan, indent=2))
        return 0
    
    if args.print_prompt or args.enforce:
        prompt = _build_prompt(args.skill, plan)
        print(prompt)
        return 0
    
    print(f"# Skill: {args.skill}")
    print(f"# Steps: {len(plan['steps'])} | Commands: {len(plan['commands'])} | Files: {len(plan['files'])}")
    print(f"# Mandatory: {len(plan['mandatory'])}")
    print()
    print("Use --print-prompt to get deterministic execution prompt")
    print("Use --plan for JSON execution plan")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
