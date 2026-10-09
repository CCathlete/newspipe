SYS=LLMDDv8.0
MODE=DTR_BASELINE_X_CU_COMPILE

# ──────────────────────────────────────────────
# TENSOR GRAMMAR
# ──────────────────────────────────────────────
TENSOR.GRAMMAR=KEY=VALUE or NAMESPACE.KEY=VALUE — one complete semantic unit per line
TENSOR.SEPARATORS== assignment, . namespace, , list, > flow, X exchange, ${} env expansion, -> relation arrow, | item separator, # comment (line-level)
TENSOR.SELF_CONTAINED=True — no external references, no config files, no docs needed
TENSOR.LEGEND=Every ITR directory must include a LEGEND file defining all symbols used, so the coder never needs external context

# ──────────────────────────────────────────────
# NODE DEFINITIONS
# ──────────────────────────────────────────────
NODE.X=EXTRACTOR
NODE.X_DESC=Runs dtr-builder to produce a baseline DTR from an existing codebase. Two modes: --root extracts FILES, CODEX, TYPE, REL, META sections; --create-baseline-dtr generates a skeleton DTR for greenfield projects.
NODE.DTR=DESIGN_TENSOR (Baseline)
NODE.DTR_DESC=Flat tensor file (KEY=VALUE) with sections: ARCH, META, FILE, CODEX, TYPE, REL. Produced by dtr-builder. Serves as the coordinate system for CUs.
NODE.DSG=DESIGNER
NODE.DSG_DESC=Human. Runs/obtains baseline DTR, decides CU decomposition, crafts CU content (JSON/YAML/raw), gives green light to compile, assigns CUs to coders.
NODE.ADV=ADVISOR
NODE.ADV_DESC=Receives baseline DTR, analyzes structure, brainstorms with Designer, drafts CU content. Never writes application code.
NODE.ITR_COMPILER=ITR_COMPILER
NODE.ITR_COMPILER_DESC=Tool: itr-compiler --compile. Takes a baseline DTR and CU content (single/JSON/YAML/JSONL batch), produces a directory of per-CU .itr frame files.
NODE.COD=CODER
NODE.COD_DESC=Receives compiled CU frames. Implements CUs in dependency order. Produces per-CU FEEDBACK files. If blocked: writes ESCALATION in feedback for code lead pickup. Commits once per task.
NODE.VER=CODE_LEAD (Verifier)
NODE.VER_DESC=Smarter model Coder acting as code lead. Runs in parallel with implementation Coders. Monitors CU feedback for escalations. Performs fixes on escalated CUs. After all CUs: runs e2e tests, repairs cross-CU issues until convergence.
NODE.OUT=OUTPUT
NODE.OUT_DESC=Produced code + per-CU FEEDBACK files. Updated DTR feeds the next iteration.

# ──────────────────────────────────────────────
# PIPELINE FLOW
# ──────────────────────────────────────────────
FLOW=X>DTR>ADV X DSG>ITR_COMPILER>COD(s)+VER X VER>E2E>DTR
FLOW.FEEDBACK=COD>PER_CU_FEEDBACK>ADV — per-CU feedback flows back to Advisor for ITR optimization
FLOW.ESCALATION=COD>FEEDBACK(ESCALATED)>VER>REPAIR>COMMIT
FLOW.E2E=COD(s)_DONE>VER>E2E_TESTS>VER(REPAIR_LOOP)>REPORT>DTR

# ──────────────────────────────────────────────
# WORKFLOW STEPS
# ──────────────────────────────────────────────
WORKFLOW.STEP1=OBTAIN_BASELINE_DTR
WORKFLOW.STEP1_DESC=DSG runs dtr-builder with --root for existing code or --create-baseline-dtr for greenfield. Result is a baseline DTR file.

WORKFLOW.STEP2=ANALYZE_AND_DECOMPOSE
WORKFLOW.STEP2_DESC=DSG shares baseline DTR with ADV. ADV analyzes structure (files, types, relations, layers), brainstorms with DSG. Together they decompose work into CUs.

WORKFLOW.STEP3=DRAFT_CU_CONTENT
WORKFLOW.STEP3_DESC=ADV drafts CU content. Each CU has: CU-ID, DTR-COORDINATES (traceability into DTR), CONTENT (implementation instructions). DSG reviews and approves.

WORKFLOW.STEP4=COMPILE_ITR
WORKFLOW.STEP4_DESC=DSG runs itr-compiler --compile with CU content and baseline DTR. Output: <app>.itr/ directory with per-CU frame files (cu-001.itr, cu-002.itr, ...) plus LEGEND.itr and ARCH.itr.

WORKFLOW.STEP5=IMPLEMENT_CUS
WORKFLOW.STEP5_DESC=COD receives compiled ITR directory. Implements CUs in dependency order. Writes per-CU FEEDBACK files. If blocked: writes ESCALATION in feedback. Commits code, CU frames, and feedback once per task.

WORKFLOW.STEP6=VERIFY_CUS
WORKFLOW.STEP7=E2E_VERIFY
WORKFLOW.STEP8=ITERATE
WORKFLOW.STEP6_DESC=VER (code lead) monitors CU feedback in real-time. On ESCALATED status: reads escalation detail, performs fix, commits. Runs in parallel with COD(s). Never waits for all CUs to complete.

WORKFLOW.STEP7_DESC=After all CUs implemented: VER runs full e2e test suite (written by ADV). If FAIL: analyzes cross-CU failures, repairs, re-runs. Loop until convergence or max iterations. Writes final verification report.

WORKFLOW.STEP8_DESC=Updated codebase is re-scanned by dtr-builder to produce a new baseline DTR. Cycle repeats.

# ──────────────────────────────────────────────
# CU — COMPUTATIONAL UNIT
# ──────────────────────────────────────────────
CU=Computational Unit — atomic work item in an ITR
CU.FIELDS=ID,DTR_COORDINATES,CONTENT,TESTS,VERIFICATION,TIMESTAMP
CU.ID_FORMAT=cu-<NNN> — zero-padded, three-digit sequence number (cu-001, cu-002, ..., cu-999)
CU.DTR_COORDINATES=String array of addresses into baseline DTR for traceability (e.g. TYPE.com.app.domain.Model, FILE.src/domain/Model.scala)
CU.CONTENT=Free-form text — implementation instructions, code specs, or design notes
CU.TIMESTAMP=Auto-generated by itr-compiler on compile

CU.TESTS=Advisor-written test code for CU verification. Actual executable tests, not descriptions. Verifier runs these.
CU.VERIFICATION=Verification metadata: unit_tests path, integration_points, smoke check command.

# ──────────────────────────────────────────────
# CU CONTENT INPUT FORMATS
# ──────────────────────────────────────────────
CONTENT.SINGLE=--raw-content <text> --cu-id <id> [--dtr-coordinates <c1,c2>] — one CU from CLI
CONTENT.JSON=--json-content <path> — batch of CUs from JSON array
CONTENT.JSONL=--jsonl-content <path> — one JSON object per line, same keys as JSON array
CONTENT.YAML=--yaml-content <path> — batch of CUs from YAML mapping

CONTENT.JSON_STRUCTURE=[{"cu-id":"cu-001","dtr-coordinates":["addr1","addr2"],"content":"..."},{"cu-id":"cu-002","dtr-coordinates":[],"content":"..."}]
CONTENT.JSONL_STRUCTURE={"cu-id":"cu-001","dtr-coordinates":["addr1"],"content":"..."}\n{"cu-id":"cu-002","dtr-coordinates":[],"content":"..."}
CONTENT.YAML_STRUCTURE=cu-001:{dtr-coordinates:[str],content:str}\ncu-002:{dtr-coordinates:[str],content:str}

# ──────────────────────────────────────────────
# BASELINE DTR GENERATION
# ──────────────────────────────────────────────
BASELINE.EXTRACT_CMD=dtr-builder --root <path> --out <file>
BASELINE.EXTRACT_DESC=Scans existing codebase, extracts FILE, CODEX, TYPE, REL, META sections. File is flat KEY=VALUE tensor format; chunks automatically if >1MB.

BASELINE.CREATE_CMD=dtr-builder --create-baseline-dtr --language <lang> --root <path> --out <file>
BASELINE.CREATE_DESC=Generates skeleton DTR from seed template for greenfield projects. Includes hexagonal ARCH, placeholder FILE, CODEX, TYPE, REL entries for canonical domain/application/infrastructure/control layers. App name is derived from the root directory basename; there is no --app flag.

BASELINE.SECTION.ARCH="ARCH=..." and "LAYER.ORDER=..." — architecture constraints
BASELINE.SECTION.META="META.GENERATOR=dtr-builder", "META.TIMESTAMP=...", etc. — extraction metadata
BASELINE.SECTION.FILE="FILE.<relpath>=SIZE:<n>,MIME:<type>,ENCODING:<enc>,LANG:<lang>,EXT:<ext>,LAYER:<layer>" — file entries; LAYER is required and is one of DOMAIN,APPLICATION,INFRASTRUCTURE,CONTROL,UNKNOWN
BASELINE.SECTION.CODEX="CODEX.<relpath>=<SigType>:<signature>" — code element entries
BASELINE.SECTION.TYPE="TYPE.<fqn>=KIND:<kind>,FILE:<relpath>" — type definitions
BASELINE.SECTION.REL="REL.<from>-><to>=<relation>:<description>" — dependency edges

# ──────────────────────────────────────────────
# ITR COMPILATION
# ──────────────────────────────────────────────
COMPILE.CMD=itr-compiler --compile --dtr <baseline.dtr> --out-folder <app.itr/> [--raw-content|--json-content|--yaml-content|--jsonl-content] [--force] [--dtr-coordinates <c1,c2>]
COMPILE.DESC=Takes baseline DTR and CU content, produces per-CU .itr frame files in <app.itr/> directory.

COMPILE.FRAME_FILE=cu-<id>.itr — one file per CU
COMPILE.FRAME_STRUCTURE=# CU-ID: <id>\n# TIMESTAMP: <ts>\n# DTR-COORDINATES: <addr1>,<addr2>\n\n<content>
COMPILE.OUTPUT_DIR=<app>.itr/ — directory per app containing all CU frame files
COMPILE.OUTPUT_CONTENT=LEGEND.itr,ARCH.itr,cu-001.itr,cu-002.itr,...

COMPILE.FLAG_DTR=--dtr <path> — path to baseline DTR file
COMPILE.FLAG_OUT=--out-folder <path> — output directory for compiled frames
COMPILE.FLAG_RAW=--raw-content <text> — single CU content from CLI
COMPILE.FLAG_CUID=--cu-id <id> — CU identifier (used with --raw-content)
COMPILE.FLAG_JSON=--json-content <path> — JSON array batch file
COMPILE.FLAG_JSONL=--jsonl-content <path> — JSONL batch file
COMPILE.FLAG_YAML=--yaml-content <path> — YAML mapping batch file
COMPILE.FLAG_FORCE=--force — overwrite existing CU frame files
COMPILE.FLAG_DTRCOORDS=--dtr-coordinates <c1,c2> — DTR coordinates for single CU mode

COMPILE.FORCE_BEHAVIOR=without-force:skip-existing-frame,with-force:overwrite-existing-frame
COMPILE.ERROR_HANDLING=per-cu:continue-on-error,report-warnings,partial-output-allowed

# ──────────────────────────────────────────────
# ARCHITECTURE CONSTRAINTS
# ──────────────────────────────────────────────
ARCH=HEX,DI,DIP,NO_CROSS_LAYER,PORT_FLOW_OUT_IN,DOTENV_WALKUP,CODER_FEEDBACK,SEVERITY,ITR_LIFECYCLE,HARD_FAIL
ARCH.HEX=Hexagonal architecture — domain innermost, application defines ports, infrastructure provides adapters
ARCH.DI=Dependency injection — all dependencies provided from outside, no service locators
ARCH.DIP=Dependency inversion — high-level modules own the interfaces, low-level modules implement them
ARCH.NO_CROSS_LAYER=Each layer depends only on the layer directly below it
ARCH.PORT_FLOW=Outbound ports defined in application layer, inbound adapters in infrastructure layer
ARCH.LAYER_ORDER=DOMAIN,APPLICATION,INFRASTRUCTURE,CONTROL
ARCH.LAYER.DOMAIN=MODELS_ONLY — entities, value objects, no logic, no infrastructure imports
ARCH.LAYER.APPLICATION=SERVICES_PORTS_USECASES — operational logic, business rules, port interfaces
ARCH.LAYER.INFRASTRUCTURE=ENVIRONMENT_SINGLETON_ADAPTERS_DOTENV — config, dotenv, IO, persistence, external APIs
ARCH.LAYER.CONTROL=CONTAINER_CONTROLLERS_CLI_ENTRYPOINT — DI wiring, controllers, CLI, main

ARCH.DOTENV=DotEnv walk-up discovery is mandatory for every application
ARCH.DOTENV.DISCOVERY=Walk up from application root toward filesystem root, check each directory for .env. First hit loads. None found = proceed with empty DotEnv.
ARCH.DOTENV.MERGE=SYSTEM_OVERRIDES — system environment variables take precedence over .env values
ARCH.DOTENV.EXPANSION=Pattern ${VAR_NAME} inside values is replaced with VAR_NAME value. Multiple ${} per value supported. Depth limit 10. Circular and self-references produce hard error.
ARCH.DOTENV.QUOTING=Unquoted and double-quoted values expand ${}. Single-quoted values are literal — no expansion.

ARCH.SEVERITY=CRITICAL,MAJOR,MINOR,TRIVIAL
ARCH.SEVERITY.CRITICAL=Stub instead of real implementation. Missing core feature. DIP violation. Exposing different DTR signature than declared. Hard fail — coder MUST stop and report.
ARCH.SEVERITY.MAJOR=Feature implemented but differs from spec significantly. Wrong layer placement. Incorrect DI wiring. Must fix but non-blocking — can proceed after DSG acknowledges.
ARCH.SEVERITY.MINOR=Functional but differs from spec in approach. Different library choice. Naming conventions. Acceptable with note in feedback.
ARCH.SEVERITY.TRIVIAL=Documentation, comments, formatting, whitespace. Nice to fix but not required.
ARCH.HARD_FAIL=Hard fail on any CRITICAL or MAJOR constraint violation — stop and report. CRITICAL requires DSG guidance before proceeding.

ARCH.ITR.LIFECYCLE=ONE_DIR_PER_APP,TRACKED_IN_GIT,OVERWRITTEN_ON_RECOMPILE
ARCH.ITR.LOCATION=itr-buffer/<app>.itr/ — directory per application containing LEGEND.itr, ARCH.itr, and compiled CU frame files
ARCH.ITR.LEGEND=LEGEND.itr — defines all symbols used in CU frames within the same directory
ARCH.ITR.ARCH=ARCH.itr — app-specific architecture config: ARCH, LAYER.ORDER, APP.NAME, APP.LANGUAGE, APP.VERSION

# ──────────────────────────────────────────────
# PER-CU FEEDBACK
# ──────────────────────────────────────────────
ARCH.FEEDBACK=PER_CU — feedback is per Computational Unit, not per app
ARCH.FEEDBACK.PARALLEL=True — feedback directory is a sibling of the ITR directory: <app>.feedback/ sits at the same level as <app>.itr/
ARCH.FEEDBACK.DIR=<app>.feedback/ — sibling directory to <app>.itr/ at the same level, mirrors the CU frame structure
ARCH.FEEDBACK.FILE_PATTERN=cu-<id>.feedback.txt — same basename as the CU frame, .feedback.txt extension
ARCH.FEEDBACK.DISCIPLINE=Codex OVERWRITES the feedback file on CU completion. Never appends. Git history preserves every version.
ARCH.FEEDBACK.FIELDS=CU_ID,CODER_NAME,DATE,CLARITY_RATING,AMBIGUOUS_LINES,MISSING_CONTEXT,TOO_MUCH_DETAIL,ARCHITECTURE_DEVIATION,ARCHITECTURE_DEVIATION.SEVERITY,TIME_TAKEN_MINUTES,AI_CREDITS_USED,COMMIT_MESSAGE,STATUS,ESCALATION_REASON,ESCALATION_DETAIL,VERIFICATION_RESULT,VERIFICATION_DETAILS
ARCH.FEEDBACK.FORMAT=ITR tensor format: KEY=VALUE per line, one semantic unit per line
ARCH.FEEDBACK.RATING_SCALE=1-5 — 1=impossible to follow, 5=crystal clear, no questions
ARCH.FEEDBACK.TIME_TAKEN_MINUTES=Duration to implement the CU from start to feedback creation, in minutes.
ARCH.FEEDBACK.AI_CREDITS_USED=Number of AI credits consumed during implementation of this CU.

# ──────────────────────────────────────────────
# RULES: DESIGN TENSOR
# ──────────────────────────────────────────────
RULE.DTR=POSITIONAL,FLAT,NO_TREE,EXPLICIT_RELATIONS,DETERMINISTIC

# ──────────────────────────────────────────────
# RULES: ADVISOR
# ──────────────────────────────────────────────
RULE.ADV=ANALYZE_ONLY,NO_IMPLEMENTATION
RULE.ADV.DRAFT_CUS=Advisor drafts CU content for Designer review. CU content includes: CU-ID (unique identifier), DTR-COORDINATES (traceability into baseline DTR), CONTENT (implementation instructions). Advisor specifies dependency order among CUs. Advisor never writes CU frame files directly — the itr-compiler produces them from approved CU content.
RULE.ADV.SEVERITY_BASELINE=Every CU drafted by the Advisor is implicitly SEVERITY:CRITICAL unless explicitly downgraded in the CU content.

# ──────────────────────────────────────────────
# RULES: DESIGNER
# ──────────────────────────────────────────────
RULE.DSG=DECIDE_ONLY,NO_CODE
RULE.DSG.APPROVE_CUS=Designer reviews and approves CU content before compilation. Runs itr-compiler to produce compiled frame files. Assigns compiled CUs to Coders.

# ──────────────────────────────────────────────
# RULES: CODER
# ──────────────────────────────────────────────
RULE.COD=EXECUTE_ITR_ONLY,NO_DESIGN_CHANGE,STRICT_CU_ORDER,COMMIT_PER_TASK
RULE.COD.IMPLEMENT=Codex implements assigned CUs in dependency order. Each CU frame file (cu-<id>.itr) contains: CU-ID, DTR-COORDINATES, TIMESTAMP, CONTENT.
RULE.COD.FEEDBACK_MANDATORY=Codex MUST write a per-CU FEEDBACK file for every CU implemented. File pattern: <app>.feedback/cu-<id>.feedback.txt. Must overwrite, never append. Self-assess SEVERITY of each ARCHITECTURE_DEVIATION. Print full feedback content in final message.
RULE.COD.COMMIT_SCOPE=PREPARE_ONLY — one commit per coder task, regardless of CU count. Commit includes: application code + CU frame files + per-CU FEEDBACK files + updated sys tensor.
RULE.COD.COMMIT_MESSAGE=Codex provides a descriptive COMMIT_MESSAGE in the feedback summarizing all changes in the task. The commit message MUST be this summary.
RULE.COD.SEVERITY_BASELINE=Every CU deliverable is implicitly SEVERITY:CRITICAL unless Advisor marked otherwise. Codex may not downgrade without DSG approval.
RULE.COD.HARD_FAIL=Any CRITICAL deviation MUST be reported to DSG before commit. MAJOR must be noted in feedback and acknowledged by DSG.

# ──────────────────────────────────────────────
# RULES: VERIFIER
# ──────────────────────────────────────────────
RULE.VER=CODE_LEAD,REPAIR_ESCALATIONS,E2E_CONVERGENCE
RULE.VER.MONITOR=Verifier monitors CU feedback files in real-time. On ESCALATED status: reads escalation detail, performs fix, commits. Never waits for all CUs to complete before acting.
RULE.VER.REPAIR=Verifier can modify any CU implementation. Never modifies test files - tests are written by ADV and are the contract. If a test appears wrong, Verifier notes it in report but does not modify it.
RULE.VER.E2E=After all CUs implemented: Verifier runs full e2e test suite. If FAIL: analyzes cross-CU failures, repairs, re-runs. Loop until convergence or max iterations.
RULE.VER.CONVERGENCE=Verifier tracks e2e repair iterations. On convergence: writes PASS report. On max iterations: writes DIAGNOSIS report. Report is informational - Designer is informed, not asked to act.
RULE.VER.REPORT=Final verification report includes: per-CU status (CONVERGED/REPAIRED/ESCALATED), e2e test results, repairs made, architecture compliance check, recommendation for next iteration.

# ──────────────────────────────────────────────
# CONVERGENCE
# ──────────────────────────────────────────────
CONVERGENCE.MAX_E2E_REPAIR_ITERATIONS=3
CONVERGENCE.DIVERGENCE_THRESHOLD=E2e tests get worse for 2 consecutive iterations -> STOP
CONVERGENCE.ESCALATION=On divergence or max iterations -> DIAGNOSIS report (informational, not actionable)
CONVERGENCE.STATE_FILE=<app>.convergence.json
