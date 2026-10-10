---
name: verify-cu-structure
description: Verify that compiled CU frames under .LLMDD/ITRS/<feature>/ are per-CU folders with all required component files and root ARCH.itr/LEGEND.itr globals, and auto-repair legacy cu.itr blobs by splitting their sections into per-section .itr files. Use after compile-itr and before handing frames to a Coder.
---

# verify-cu-structure

Enforces the CU frame layout (feat-1, enforced by `itr-compiler`): every
regular CU is a **folder** holding exactly the four component files, and
the globals `ARCH.itr` / `LEGEND.itr` (plus `SYNOPSIS.itr` for new-schema
batches) stay at the frame **root** — never in subfolders.

## When to use

- After `compile-itr` produces `.LLMDD/ITRS/<feature-name>/`, before STEP5.
- Whenever a frame directory looks suspicious (flat `.itr` files, missing sections).
- The agent repairs what it finds; it never leaves a half-fixed tree.

## Expected layout

```
.LLMDD/ITRS/<feature-name>/
  ARCH.itr
  LEGEND.itr
  SYNOPSIS.itr            # new-schema batches only
  cu-001/COORDINATES.itr
  cu-001/REQUIREMENTS.itr
  cu-001/IMPLEMENTATION_STEPS.itr
  cu-001/ACCEPTANCE.itr
  cu-002/...
```

Rules:

- Every regular CU is a folder named `<cu-id>` (`cu-NNN`).
- Every regular CU folder holds exactly four files: `COORDINATES.itr`,
  `REQUIREMENTS.itr`, `IMPLEMENTATION_STEPS.itr`, `ACCEPTANCE.itr`.
- Globals live at the frame root only: `ARCH.itr`, `LEGEND.itr`
  (`SYNOPSIS.itr` for new-schema batches). An `arch/` or `legend/`
  subfolder is a defect — move its frame back to the root (Repair below).
- No `<cu-id>.itr` blob anywhere — neither inside a folder nor flat at root.
- Each component file starts with `# <SECTION>: <cu-id>` followed by a
  blank line and the section body (the `itr-compiler` component format).

## Verify

Run from repo root with `<feature-name>` substituted:

```sh
# 1. Root holds globals only (plus non-itr companions like batch.json/waves.json)
find .LLMDD/ITRS/<feature-name> -maxdepth 1 -name "*.itr" | sort
# EXPECTED: ARCH.itr, LEGEND.itr, optionally SYNOPSIS.itr — nothing else
# 2. No blobs anywhere, no arch//legend/ subfolders
find .LLMDD/ITRS/<feature-name> -name "cu-*.itr" | grep . && echo "FAIL: blobs" || echo "OK: no blobs"
test -d .LLMDD/ITRS/<feature-name>/arch -o -d .LLMDD/ITRS/<feature-name>/legend && echo "FAIL: arch//legend/ folders" || echo "OK: globals at root"
# 3. Every cu-NNN folder holds exactly the four components
for d in .LLMDD/ITRS/<feature-name>/cu-*/; do ls "$d" | sort | tr '\n' ' '; echo "<- $d"; done
# 4. Root globals exist
ls .LLMDD/ITRS/<feature-name>/ARCH.itr .LLMDD/ITRS/<feature-name>/LEGEND.itr
```

All four checks must pass. Any failure → Repair.

## Repair: split a cu.itr blob

If the agent finds a `<cu-id>.itr` blob (in a folder or flat at root):

1. Read the blob; take `<cu-id>` from its `# CU-ID:` header line and
   assert the file name matches (`<cu-id>.itr`), abort on mismatch.
2. Split the body on the colon section labels, each occurring exactly
   once: `REQUIREMENTS:`, `COORDINATES:`, `IMPLEMENTATION_STEPS:`,
   `ACCEPTANCE:` (the `CU.sectionLabels` the compiler splits on; bare
   labels without colons are NOT recognized — see
   `bug-3-silent-blob-on-bare-section-labels`). Abort (do not write)
   otherwise.
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

## Repair: arch//legend/ subfolders

If `arch/` or `legend/` folders exist holding a single frame, move it back
to the root and remove the emptied folder:

```sh
mv .LLMDD/ITRS/<feature-name>/arch/ARCH.itr .LLMDD/ITRS/<feature-name>/ARCH.itr
mv .LLMDD/ITRS/<feature-name>/legend/LEGEND.itr .LLMDD/ITRS/<feature-name>/LEGEND.itr
rmdir .LLMDD/ITRS/<feature-name>/arch .LLMDD/ITRS/<feature-name>/legend
```

Then re-run Verify; all checks must pass.

## Rules

- Split and move only — never edit section text, never invent sections.
- Abort the repair (leave the blob, escalate) when the header/section
  assertions fail; a malformed blob is a spec bug, not a layout bug.
- Always re-run Verify after repairing; report the before/after file list.

## Output

A frame directory where every regular CU is a folder with all required
files, globals are at the root, and no blobs remain — ready for STEP5
(IMPLEMENT_CUS).
