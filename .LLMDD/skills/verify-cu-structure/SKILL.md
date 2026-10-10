---
name: verify-cu-structure
description: Verify that compiled CU frames under .LLMDD/ITRS/<feature>/ are folders with all required component files, and auto-repair legacy cu.itr blobs by splitting their sections into per-section .itr files. Use after compile-itr and before handing frames to a Coder.
---

# verify-cu-structure

Enforces the repo's CU frame layout (Designer decision 2026-10-10):
every CU — including `arch` and `legend` — is a **folder**, never a flat
file, and every regular CU folder holds exactly the four component files.

## When to use

- After `compile-itr` produces `.LLMDD/ITRS/<feature-name>/`, before STEP5.
- Whenever a frame directory looks suspicious (flat `.itr` files, missing sections).
- The agent repairs what it finds; it never leaves a half-fixed tree.

## Expected layout

```
.LLMDD/ITRS/<feature-name>/
  arch/ARCH.itr
  legend/LEGEND.itr
  cu-001/COORDINATES.itr
  cu-001/REQUIREMENTS.itr
  cu-001/IMPLEMENTATION_STEPS.itr
  cu-001/ACCEPTANCE.itr
  cu-002/...
```

Rules:

- Every CU is a folder named `<cu-id>` (`arch`, `legend`, `cu-NNN`).
- Every regular CU folder holds exactly four files: `COORDINATES.itr`,
  `REQUIREMENTS.itr`, `IMPLEMENTATION_STEPS.itr`, `ACCEPTANCE.itr`.
- No `<cu-id>.itr` blob anywhere — neither inside a folder nor flat at root.
- No loose `.itr` files at the frame root.
- Each component file starts with `# <SECTION>: <cu-id>` followed by a
  blank line and the section body (the `itr-compiler` component format).

## Verify

Run from repo root with `<feature-name>` substituted:

```sh
# 1. No flat .itr files at root, no blobs anywhere
find .LLMDD/ITRS/<feature-name> -maxdepth 1 -name "*.itr" | grep . && echo "FAIL: flat files" || echo "OK: no flat files"
find .LLMDD/ITRS/<feature-name> -name "cu-*.itr" -o -name "arch.itr" -o -name "legend.itr" | grep . && echo "FAIL: blobs" || echo "OK: no blobs"
# 2. Every cu-NNN folder holds exactly the four components
for d in .LLMDD/ITRS/<feature-name>/cu-*/; do ls "$d" | sort | tr '\n' ' '; echo "<- $d"; done
# 3. arch/ and legend/ folders exist with their single frame
ls .LLMDD/ITRS/<feature-name>/arch/ARCH.itr .LLMDD/ITRS/<feature-name>/legend/LEGEND.itr
```

All three checks must pass. Any failure → Repair.

## Repair: split a cu.itr blob

If the agent finds a `<cu-id>.itr` blob (in a folder or flat at root):

1. Read the blob; take `<cu-id>` from its `# CU-ID:` header line and
   assert the file name matches (`<cu-id>.itr`), abort on mismatch.
2. Split the body on bare-word section lines. Exactly these four, each
   occurring exactly once: `REQUIREMENTS`, `COORDINATES`,
   `IMPLEMENTATION_STEPS`, `ACCEPTANCE`. Abort (do not write) otherwise.
3. Create the folder `.LLMDD/ITRS/<feature-name>/<cu-id>/` if missing.
4. Write one file per section — `# <SECTION>: <cu-id>`, blank line,
   section body, trailing newline:
   `COORDINATES.itr`, `REQUIREMENTS.itr`, `IMPLEMENTATION_STEPS.itr`,
   `ACCEPTANCE.itr`.
5. Verify each new body is identical to the corresponding blob section
   (modulo trailing newlines), then delete the blob.
6. Re-run Verify above; all checks must pass.

If the blob sits flat at the frame root, the same steps apply — the
components always land in `<cu-id>/`, and any enclosing flat file is
removed.

## Rules

- Split and move only — never edit section text, never invent sections.
- Abort the repair (leave the blob, escalate) when the header/section
  assertions fail; a malformed blob is a spec bug, not a layout bug.
- Always re-run Verify after repairing; report the before/after file list.

## Output

A frame directory where every CU is a folder with all required files and
no blobs remain — ready for STEP5 (IMPLEMENT_CUS).
