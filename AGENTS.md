# AGENTS.md

## Project Intent

This repository is a practical APS/planning prototype focused on:

- clean data flow,
- model consistency,
- route-level DAG scheduling behavior.

## Mandatory Pre-Change Policy

Any modification in this repository must review this file first.

### Enforcement Contract

- This rule applies to any repository change (code, data, docs, configs, reports).
- Review `AGENTS.md` before editing files.
- Direct edits without prior review are treated as non-compliant.

## Scheduling Logic Reference (Mandatory)

Scheduling domain rules are maintained in:

- `schedule_logic.md` (Chinese, root-level file)

Before changing any scheduling-related file, read `schedule_logic.md` first:

- `src/generate_schedule_from_db.py`
- `src/visualize_planning_data.py`
- `src/validate_and_import_data_v1_1.py` (when scheduling I/O or timing rules are involved)
- scheduling-related CSV contracts under `data/`
- scheduling outputs under `reports/schedule/`

If there is any scheduling-rule conflict:

- `schedule_logic.md` has higher priority than this file.

## Encoding Integrity Check (Required)

Before modifying key scripts/files, verify text files do not contain mojibake/private-use characters.

### UTF-8 Canonical Encoding (Mandatory)

- All repository text files must be stored in UTF-8.
- Applies to: `*.py`, `*.md`, `*.csv`, `*.html`, `*.txt`, and root governance docs.
- Do not commit files with mojibake artifacts or malformed replacement text.
- If mojibake is detected:
  - Repair text using a UTF-8-safe conversion workflow.
  - Re-run encoding checks and script compile checks before proceeding.

- Target files:
  - `src/models_v1_1.py`
  - `src/validate_and_import_data_v1_1.py`
  - `src/generate_schedule_from_db.py`
  - `AGENTS.md`
  - `schedule_logic.md`
- Fail rule:
  - If any line contains Unicode private-use characters (`U+E000` to `U+F8FF`) or obvious mojibake markers, stop and fix before execution.

## Code Guidelines

- Use Python 3.10 from: `C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe`.
- Prefer explicit and readable business naming.
- Add type hints for newly added public/helper functions where practical.
- Keep implementation simple and incremental.
- Avoid heavyweight frameworks/solvers for current-stage needs.

## UI/HTML Readability Rule (Mandatory)

For all UI/report HTML changes, readability is the first priority.

- Prefer clear structure: obvious page hierarchy, section titles, and navigation links.
- Prefer concise labels and user-facing wording over internal jargon.
- Prefer consistent table headers, units/time formats, and value formatting.
- Avoid dense or ambiguous layouts; improve spacing and scannability first.
- When a display tradeoff exists, choose the more readable option.
- For `src/visualize_planning_data.py`, audit and inspection content should be integrated into the generated HTML pages first.
- Do not add new `reports/data_audit/` outputs from `src/visualize_planning_data.py` unless the user explicitly asks for standalone audit files.

## Repo-Specific Guidance

- Primary code lives under `src/`.
- Data source files live under `data/`.
- Database file lives under `db/`.
- Visualization output lives under `reports/`.

Current key scripts:

- `src/models_v1_1.py`: ORM model and DB creation.
- `src/validate_and_import_data_v1_1.py`: validation + import pipeline.
- `src/generate_schedule_from_db.py`: scheduling generation from DB and report output.
- `src/visualize_planning_data.py`: planning visualization.
- `src/build_report_portal.py`: unified report portal generation (`reports/portal/`).

### Code Update Rules Supplement

- Default behavior for this repo:
  - Do **not** rely on CLI argument switching for order files.
  - Prefer script-level parameter/config updates first.

## Update Logic

Update this file when execution policy, workflow contracts, or repository governance rules change.

## README Sync Requirement (Mandatory)

After each change that affects repository structure, file contracts, or output artifacts,
`README.md` must be updated in the same run.

Scope that requires README update:

- Any add/remove/rename under `src/`, `data/`, `db/`, `reports/`, or root governance docs.
- Any CSV schema/column contract change in `data/`.
- Any script behavior change that affects run commands or generated outputs.

Minimum README update requirements:

- Reflect latest directory structure.
- Reflect latest `data/` table list and key fields.
- Reflect latest execution steps and output files.
