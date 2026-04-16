
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from loguru import logger
import pandas as pd
from sqlalchemy.orm import Session

from models_v1_1 import (
    ConsumptionMode,
    Employee,
    EmployeeCalendar,
    EmployeeStatus,
    EmployeeWeeklyCalendar,
    Inventory,
    Machine,
    MachineCalendar,
    MachineStatus,
    MachineType,
    Material,
    MaterialFormType,
    MaterialPurchase,
    MaterialType,
    Order,
    OrderStatus,
    ProcessRoute,
    RouteStep,
    RouteStepDependency,
    RouteStepInput,
    RouteStepMachineType,
    ScheduleTask,
    StepExecutionMode,
    StepQuantityConversion,
    TaskStatus,
    Workshop,
    create_database,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = PROJECT_ROOT / "db" / "planning_demo.sqlite"

ORDER_FILE_NAME = "orders_25.csv"
DROP_EXISTING_DB = True
IMPORT_SCHEDULE_TASKS = False

MOJIBAKE_MARKERS = [
    chr(0xFFFD),
    chr(0x951F),
    "".join(chr(c) for c in (0x00EF, 0x00BB, 0x00BF)),
    "".join(chr(c) for c in (0x00C3, 0x00A9)),
]
MANDATORY_TEXT_FILES = [
    PROJECT_ROOT / "src" / "models_v1_1.py",
    PROJECT_ROOT / "src" / "validate_and_import_data_v1_1.py",
    PROJECT_ROOT / "AGENTS.md",
    PROJECT_ROOT / "schedule_logic.md",
]

CSV_FILES = {
    "workshops": "workshops.csv",
    "machine_types": "machine_types.csv",
    "machines": "machines.csv",
    "materials": "materials.csv",
    "material_purchases": "material_purchases.csv",
    "inventories": "inventories.csv",
    "employees": "employees.csv",
    "machine_calendars": "machine_calendars.csv",
    "employee_weekly_calendars": "employee_weekly_calendars.csv",
    "employee_calendars": "employee_calendars.csv",
    "process_routes": "process_routes.csv",
    "route_steps": "route_steps.csv",
    "route_step_inputs": "route_step_inputs.csv",
    "route_step_machine_types": "route_step_machine_types.csv",
    "route_step_dependencies": "route_step_dependencies.csv",
    "step_quantity_conversions": "step_quantity_conversions.csv",
    "orders": ORDER_FILE_NAME,
    "schedule_tasks": "schedule_tasks.csv",
}

REQUIRED_COLUMNS = {
    "workshops": ["code", "name", "note"],
    "machine_types": ["code", "name", "workshop_code", "note"],
    "machines": ["code", "name", "machine_type_code", "status"],
    "materials": ["code", "name", "material_type", "uom", "material_form_type"],
    "material_purchases": ["material_code", "purchase_lead_time_days"],
    "inventories": ["material_code", "available_qty"],
    "employees": ["code", "name", "workshop_code", "status", "note"],
    "machine_calendars": ["machine_code", "start_time", "end_time", "is_working", "note"],
    "employee_weekly_calendars": ["employee_code", "week_in_cycle", "weekday", "start_time", "end_time", "shift_code", "is_working", "note"],
    "employee_calendars": ["employee_code", "start_time", "end_time", "is_working", "note"],
    "process_routes": ["code", "name", "target_material_code", "note"],
    "route_steps": ["route_code", "code", "name", "process_name", "display_order", "execution_mode", "output_material_code", "output_qty_per_execution", "output_uom_code"],
    "route_step_inputs": ["route_code", "step_code", "material_code", "input_qty_per_execution", "input_uom_code", "consumption_mode"],
    "route_step_machine_types": ["route_code", "step_code", "machine_type_code", "duration_min_override", "capacity_per_execution", "capacity_uom_code"],
    "route_step_dependencies": ["route_code", "predecessor_step_code", "successor_step_code"],
    "step_quantity_conversions": ["route_code", "step_code", "from_uom_code", "from_qty", "to_uom_code", "to_qty", "conversion_type"],
    "orders": ["code", "priority", "due_date", "requested_material_code", "requested_qty", "route_code", "status", "note"],
    "schedule_tasks": ["order_code", "route_id", "route_code", "step_code", "machine_code", "employee_code", "planned_qty", "planned_start", "planned_end", "duration_min", "status"],
}

NON_EMPTY = {
    "workshops": ["code", "name"],
    "machine_types": ["code", "name", "workshop_code"],
    "machines": ["code", "name", "machine_type_code", "status"],
    "materials": ["code", "name", "material_type", "uom", "material_form_type"],
    "material_purchases": ["material_code", "purchase_lead_time_days"],
    "inventories": ["material_code", "available_qty"],
    "employees": ["code", "name", "workshop_code", "status"],
    "machine_calendars": ["machine_code", "start_time", "end_time", "is_working"],
    "employee_weekly_calendars": ["employee_code", "week_in_cycle", "weekday", "start_time", "end_time", "shift_code", "is_working"],
    "employee_calendars": ["employee_code", "start_time", "end_time", "is_working"],
    "process_routes": ["code", "name", "target_material_code"],
    "route_steps": ["route_code", "code", "name", "process_name", "display_order", "execution_mode", "output_material_code", "output_qty_per_execution", "output_uom_code"],
    "route_step_inputs": ["route_code", "step_code", "material_code", "input_qty_per_execution", "input_uom_code", "consumption_mode"],
    "route_step_machine_types": ["route_code", "step_code", "machine_type_code", "duration_min_override", "capacity_per_execution", "capacity_uom_code"],
    "route_step_dependencies": ["route_code", "predecessor_step_code", "successor_step_code"],
    "step_quantity_conversions": ["route_code", "step_code", "from_uom_code", "from_qty", "to_uom_code", "to_qty", "conversion_type"],
    "orders": ["code", "priority", "due_date", "requested_material_code", "requested_qty", "status"],
}

UNIQUE_KEYS = {
    "workshops": ["code"],
    "machine_types": ["code"],
    "machines": ["code"],
    "materials": ["code"],
    "material_purchases": ["material_code"],
    "inventories": ["material_code"],
    "employees": ["code"],
    "process_routes": ["code"],
    "route_steps": ["route_code", "code"],
    "route_step_inputs": ["route_code", "step_code", "material_code"],
    "route_step_machine_types": ["route_code", "step_code", "machine_type_code"],
    "route_step_dependencies": ["route_code", "predecessor_step_code", "successor_step_code"],
    "step_quantity_conversions": ["route_code", "step_code", "from_uom_code", "to_uom_code"],
    "orders": ["code"],
}


class DataValidationError(Exception):
    pass


@dataclass
class ImportContext:
    dataframes: dict[str, pd.DataFrame]
    row_counts: dict[str, int]


def _read_text_utf8(path: Path) -> str:
    return path.read_bytes().decode("utf-8-sig")


def _contains_pua(text: str) -> bool:
    return any(0xE000 <= ord(ch) <= 0xF8FF for ch in text)


def _strip_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    for c in out.columns:
        out[c] = out[c].astype(str).map(lambda v: v.strip())
    return out


def _err(errors: list[str], table: str, row_no: int, msg: str) -> None:
    errors.append(f"[{table}] row {row_no}: {msg}")


def _i(v: str, table: str, row_no: int, field: str, errors: list[str]) -> int:
    try:
        return int(v)
    except Exception:
        _err(errors, table, row_no, f"`{field}` invalid int: {v}")
        return 0


def _d(v: str, table: str, row_no: int, field: str, errors: list[str]) -> Decimal:
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        _err(errors, table, row_no, f"`{field}` invalid decimal: {v}")
        return Decimal("0")

def _b(v: str, table: str, row_no: int, field: str, errors: list[str]) -> bool:
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    _err(errors, table, row_no, f"`{field}` invalid bool: {v}")
    return False


def _dt(v: str, table: str, row_no: int, field: str, errors: list[str]) -> datetime:
    txt = str(v).strip()
    fmts = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"]
    for f in fmts:
        try:
            return datetime.strptime(txt, f)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(txt)
    except ValueError:
        _err(errors, table, row_no, f"`{field}` invalid datetime: {v}")
        return datetime(1970, 1, 1)


def _tm(v: str, table: str, row_no: int, field: str, errors: list[str]) -> time:
    txt = str(v).strip()
    for f in ["%H:%M:%S.%f", "%H:%M:%S", "%H:%M"]:
        try:
            return datetime.strptime(txt, f).time()
        except ValueError:
            pass
    _err(errors, table, row_no, f"`{field}` invalid time: {v}")
    return time(0, 0)


def _none(v: str) -> str | None:
    s = str(v).strip()
    return s if s else None


def _validate_text_integrity() -> None:
    problems: list[str] = []
    for p in list(MANDATORY_TEXT_FILES) + list(DATA_DIR.glob("*.csv")):
        if not p.exists():
            problems.append(f"missing file: {p}")
            continue
        try:
            text = _read_text_utf8(p)
        except UnicodeDecodeError:
            problems.append(f"not UTF-8: {p}")
            continue
        if _contains_pua(text):
            problems.append(f"contains PUA chars: {p}")
        if any(m in text for m in MOJIBAKE_MARKERS):
            problems.append(f"contains mojibake marker: {p}")
    if problems:
        raise DataValidationError("Encoding integrity check failed:\n" + "\n".join(problems[:50]))


def _load_csv(table: str) -> pd.DataFrame:
    path = DATA_DIR / CSV_FILES[table]
    if not path.exists():
        raise FileNotFoundError(f"Missing input CSV: {path}")
    df = _strip_frame(pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig"))
    miss = [c for c in REQUIRED_COLUMNS[table] if c not in df.columns]
    if miss:
        raise DataValidationError(f"[{table}] missing required columns: {miss}")
    return df


def _validate_all(df_map: dict[str, pd.DataFrame]) -> None:
    errors: list[str] = []

    for t, cols in NON_EMPTY.items():
        for c in cols:
            for idx in df_map[t].index[df_map[t][c] == ""].tolist():
                _err(errors, t, idx + 2, f"`{c}` cannot be empty")
    for t, keys in UNIQUE_KEYS.items():
        dup = df_map[t][df_map[t].duplicated(subset=keys, keep=False)]
        if not dup.empty:
            sample = dup[keys].drop_duplicates().head(10).to_dict(orient="records")
            errors.append(f"[{t}] duplicated keys {keys}, sample={sample}")

    workshops = set(df_map["workshops"]["code"].tolist())
    for i, r in df_map["machine_types"].iterrows():
        if r["workshop_code"] not in workshops:
            _err(errors, "machine_types", i + 2, f"unknown workshop_code {r['workshop_code']}")
    machine_type_codes = set(df_map["machine_types"]["code"].tolist())

    for i, r in df_map["machines"].iterrows():
        if r["machine_type_code"] not in machine_type_codes:
            _err(errors, "machines", i + 2, f"unknown machine_type_code {r['machine_type_code']}")
        if r["status"] not in {e.value for e in MachineStatus}:
            _err(errors, "machines", i + 2, f"invalid status {r['status']}")
    machine_codes = set(df_map["machines"]["code"].tolist())

    mat_types: dict[str, str] = {}
    for i, r in df_map["materials"].iterrows():
        if r["material_type"] not in {e.value for e in MaterialType}:
            _err(errors, "materials", i + 2, f"invalid material_type {r['material_type']}")
        if r["material_form_type"] not in {e.value for e in MaterialFormType}:
            _err(errors, "materials", i + 2, f"invalid material_form_type {r['material_form_type']}")
        mat_types[r["code"]] = r["material_type"]
    material_codes = set(mat_types)

    for i, r in df_map["material_purchases"].iterrows():
        row = i + 2
        m = r["material_code"]
        if m not in material_codes:
            _err(errors, "material_purchases", row, f"unknown material_code {m}")
        if mat_types.get(m, "") not in {MaterialType.RAW_MATERIAL.value, MaterialType.AUXILIARY.value}:
            _err(errors, "material_purchases", row, f"material {m} cannot have purchase policy")
        if _i(r["purchase_lead_time_days"], "material_purchases", row, "purchase_lead_time_days", errors) <= 0:
            _err(errors, "material_purchases", row, "purchase_lead_time_days must be > 0")

    for i, r in df_map["inventories"].iterrows():
        row = i + 2
        if r["material_code"] not in material_codes:
            _err(errors, "inventories", row, f"unknown material_code {r['material_code']}")
        if _d(r["available_qty"], "inventories", row, "available_qty", errors) < 0:
            _err(errors, "inventories", row, "available_qty must be >= 0")

    for i, r in df_map["employees"].iterrows():
        row = i + 2
        if r["workshop_code"] not in workshops:
            _err(errors, "employees", row, f"unknown workshop_code {r['workshop_code']}")
        if r["status"] not in {e.value for e in EmployeeStatus}:
            _err(errors, "employees", row, f"invalid status {r['status']}")
    employee_codes = set(df_map["employees"]["code"].tolist())

    for i, r in df_map["machine_calendars"].iterrows():
        row = i + 2
        if r["machine_code"] not in machine_codes:
            _err(errors, "machine_calendars", row, f"unknown machine_code {r['machine_code']}")
        st = _dt(r["start_time"], "machine_calendars", row, "start_time", errors)
        en = _dt(r["end_time"], "machine_calendars", row, "end_time", errors)
        _b(r["is_working"], "machine_calendars", row, "is_working", errors)
        if en <= st:
            _err(errors, "machine_calendars", row, "end_time must be > start_time")

    for i, r in df_map["employee_weekly_calendars"].iterrows():
        row = i + 2
        if r["employee_code"] not in employee_codes:
            _err(errors, "employee_weekly_calendars", row, f"unknown employee_code {r['employee_code']}")
        w = _i(r["week_in_cycle"], "employee_weekly_calendars", row, "week_in_cycle", errors)
        d = _i(r["weekday"], "employee_weekly_calendars", row, "weekday", errors)
        st = _tm(r["start_time"], "employee_weekly_calendars", row, "start_time", errors)
        en = _tm(r["end_time"], "employee_weekly_calendars", row, "end_time", errors)
        _b(r["is_working"], "employee_weekly_calendars", row, "is_working", errors)
        if not 1 <= w <= 2:
            _err(errors, "employee_weekly_calendars", row, "week_in_cycle must be 1..2")
        if not 1 <= d <= 7:
            _err(errors, "employee_weekly_calendars", row, "weekday must be 1..7")
        if en <= st:
            _err(errors, "employee_weekly_calendars", row, "end_time must be > start_time")

    for i, r in df_map["employee_calendars"].iterrows():
        row = i + 2
        if r["employee_code"] not in employee_codes:
            _err(errors, "employee_calendars", row, f"unknown employee_code {r['employee_code']}")
        st = _dt(r["start_time"], "employee_calendars", row, "start_time", errors)
        en = _dt(r["end_time"], "employee_calendars", row, "end_time", errors)
        _b(r["is_working"], "employee_calendars", row, "is_working", errors)
        if en <= st:
            _err(errors, "employee_calendars", row, "end_time must be > start_time")

    route_targets: dict[str, str] = {}
    for i, r in df_map["process_routes"].iterrows():
        if r["target_material_code"] not in material_codes:
            _err(errors, "process_routes", i + 2, f"unknown target_material_code {r['target_material_code']}")
        route_targets[r["code"]] = r["target_material_code"]
    route_codes = set(route_targets)

    step_keys: set[tuple[str, str]] = set()
    for i, r in df_map["route_steps"].iterrows():
        row = i + 2
        if r["route_code"] not in route_codes:
            _err(errors, "route_steps", row, f"unknown route_code {r['route_code']}")
        if r["execution_mode"] not in {e.value for e in StepExecutionMode}:
            _err(errors, "route_steps", row, f"invalid execution_mode {r['execution_mode']}")
        if r["output_material_code"] not in material_codes:
            _err(errors, "route_steps", row, f"unknown output_material_code {r['output_material_code']}")
        if _i(r["display_order"], "route_steps", row, "display_order", errors) < 0:
            _err(errors, "route_steps", row, "display_order must be >= 0")
        if _d(r["output_qty_per_execution"], "route_steps", row, "output_qty_per_execution", errors) <= 0:
            _err(errors, "route_steps", row, "output_qty_per_execution must be > 0")
        step_keys.add((r["route_code"], r["code"]))

    for i, r in df_map["route_step_inputs"].iterrows():
        row = i + 2
        key = (r["route_code"], r["step_code"])
        if key not in step_keys:
            _err(errors, "route_step_inputs", row, f"unknown step key {key}")
        if r["material_code"] not in material_codes:
            _err(errors, "route_step_inputs", row, f"unknown material_code {r['material_code']}")
        if r["consumption_mode"] not in {e.value for e in ConsumptionMode}:
            _err(errors, "route_step_inputs", row, f"invalid consumption_mode {r['consumption_mode']}")
        if _d(r["input_qty_per_execution"], "route_step_inputs", row, "input_qty_per_execution", errors) <= 0:
            _err(errors, "route_step_inputs", row, "input_qty_per_execution must be > 0")

    for i, r in df_map["route_step_machine_types"].iterrows():
        row = i + 2
        key = (r["route_code"], r["step_code"])
        if key not in step_keys:
            _err(errors, "route_step_machine_types", row, f"unknown step key {key}")
        if r["machine_type_code"] not in machine_type_codes:
            _err(errors, "route_step_machine_types", row, f"unknown machine_type_code {r['machine_type_code']}")
        if _i(r["duration_min_override"], "route_step_machine_types", row, "duration_min_override", errors) <= 0:
            _err(errors, "route_step_machine_types", row, "duration_min_override must be > 0")
        if _d(r["capacity_per_execution"], "route_step_machine_types", row, "capacity_per_execution", errors) <= 0:
            _err(errors, "route_step_machine_types", row, "capacity_per_execution must be > 0")

    for i, r in df_map["route_step_dependencies"].iterrows():
        row = i + 2
        a = (r["route_code"], r["predecessor_step_code"])
        b = (r["route_code"], r["successor_step_code"])
        if a not in step_keys:
            _err(errors, "route_step_dependencies", row, f"unknown predecessor {a}")
        if b not in step_keys:
            _err(errors, "route_step_dependencies", row, f"unknown successor {b}")
        if a == b:
            _err(errors, "route_step_dependencies", row, "predecessor and successor cannot be same")

    for i, r in df_map["step_quantity_conversions"].iterrows():
        row = i + 2
        if (r["route_code"], r["step_code"]) not in step_keys:
            _err(errors, "step_quantity_conversions", row, "unknown step key")
        if _d(r["from_qty"], "step_quantity_conversions", row, "from_qty", errors) <= 0:
            _err(errors, "step_quantity_conversions", row, "from_qty must be > 0")
        if _d(r["to_qty"], "step_quantity_conversions", row, "to_qty", errors) <= 0:
            _err(errors, "step_quantity_conversions", row, "to_qty must be > 0")

    for i, r in df_map["orders"].iterrows():
        row = i + 2
        _i(r["priority"], "orders", row, "priority", errors)
        _dt(r["due_date"], "orders", row, "due_date", errors)
        if _d(r["requested_qty"], "orders", row, "requested_qty", errors) <= 0:
            _err(errors, "orders", row, "requested_qty must be > 0")
        m = r["requested_material_code"]
        if m not in material_codes:
            _err(errors, "orders", row, f"unknown requested_material_code {m}")
        elif mat_types.get(m) != MaterialType.PRODUCT.value:
            _err(errors, "orders", row, f"requested_material_code {m} must be product")
        rc = r["route_code"]
        if rc:
            if rc not in route_codes:
                _err(errors, "orders", row, f"unknown route_code {rc}")
            elif route_targets.get(rc) != m:
                _err(errors, "orders", row, f"route_code {rc} target mismatch requested_material_code {m}")
        if r["status"] not in {e.value for e in OrderStatus}:
            _err(errors, "orders", row, f"invalid status {r['status']}")

    if IMPORT_SCHEDULE_TASKS:
        for i, r in df_map["schedule_tasks"].iterrows():
            row = i + 2
            if _d(r["planned_qty"], "schedule_tasks", row, "planned_qty", errors) <= 0:
                _err(errors, "schedule_tasks", row, "planned_qty must be > 0")
            if r["planned_start"]:
                _dt(r["planned_start"], "schedule_tasks", row, "planned_start", errors)
            if r["planned_end"]:
                _dt(r["planned_end"], "schedule_tasks", row, "planned_end", errors)
            if r["status"] not in {e.value for e in TaskStatus}:
                _err(errors, "schedule_tasks", row, f"invalid status {r['status']}")

    if errors:
        head = "\n".join(errors[:120])
        tail = "" if len(errors) <= 120 else f"\n... ({len(errors) - 120} more errors)"
        raise DataValidationError(f"Data validation failed with {len(errors)} issue(s):\n{head}{tail}")


def _load_and_validate_data() -> ImportContext:
    _validate_text_integrity()
    tables = [
        "workshops", "machine_types", "machines", "materials", "material_purchases", "inventories", "employees",
        "machine_calendars", "employee_weekly_calendars", "employee_calendars", "process_routes", "route_steps",
        "route_step_inputs", "route_step_machine_types", "route_step_dependencies", "step_quantity_conversions", "orders",
    ]
    if IMPORT_SCHEDULE_TASKS:
        tables.append("schedule_tasks")
    d = {t: _load_csv(t) for t in tables}
    _validate_all(d)
    return ImportContext(dataframes=d, row_counts={t: len(df) for t, df in d.items()})


def _import_data(context: ImportContext) -> dict[str, int | str]:
    engine, db_file = create_database(db_path=DB_PATH, drop_existing=DROP_EXISTING_DB)
    out: dict[str, int | str] = {}

    with Session(engine) as s:
        with s.begin():
            wid: dict[str, int] = {}
            for _, r in context.dataframes["workshops"].iterrows():
                o = Workshop(code=r["code"], name=r["name"], note=_none(r["note"]))
                s.add(o); s.flush(); wid[r["code"]] = o.id
            out["workshops"] = len(wid)

            mtid: dict[str, int] = {}
            for _, r in context.dataframes["machine_types"].iterrows():
                o = MachineType(code=r["code"], name=r["name"], workshop_id=wid[r["workshop_code"]], note=_none(r["note"]))
                s.add(o); s.flush(); mtid[r["code"]] = o.id
            out["machine_types"] = len(mtid)

            mid: dict[str, int] = {}
            for _, r in context.dataframes["machines"].iterrows():
                o = Machine(code=r["code"], name=r["name"], machine_type_id=mtid[r["machine_type_code"]], status=MachineStatus(r["status"]))
                s.add(o); s.flush(); mid[r["code"]] = o.id
            out["machines"] = len(mid)

            matid: dict[str, int] = {}
            for _, r in context.dataframes["materials"].iterrows():
                o = Material(code=r["code"], name=r["name"], material_type=MaterialType(r["material_type"]), uom=r["uom"], material_form_type=MaterialFormType(r["material_form_type"]))
                s.add(o); s.flush(); matid[r["code"]] = o.id
            out["materials"] = len(matid)

            for _, r in context.dataframes["material_purchases"].iterrows():
                s.add(MaterialPurchase(material_id=matid[r["material_code"]], purchase_lead_time_days=int(r["purchase_lead_time_days"])))
            out["material_purchases"] = len(context.dataframes["material_purchases"])

            for _, r in context.dataframes["inventories"].iterrows():
                s.add(Inventory(material_id=matid[r["material_code"]], available_qty=Decimal(r["available_qty"])))
            out["inventories"] = len(context.dataframes["inventories"])

            eid: dict[str, int] = {}
            for _, r in context.dataframes["employees"].iterrows():
                o = Employee(code=r["code"], name=r["name"], workshop_id=wid[r["workshop_code"]], status=EmployeeStatus(r["status"]), note=_none(r["note"]))
                s.add(o); s.flush(); eid[r["code"]] = o.id
            out["employees"] = len(eid)

            for _, r in context.dataframes["machine_calendars"].iterrows():
                s.add(MachineCalendar(machine_id=mid[r["machine_code"]], start_time=_dt(r["start_time"], "machine_calendars", 0, "start_time", []), end_time=_dt(r["end_time"], "machine_calendars", 0, "end_time", []), is_working=_b(r["is_working"], "machine_calendars", 0, "is_working", []), note=_none(r["note"])))
            out["machine_calendars"] = len(context.dataframes["machine_calendars"])

            for _, r in context.dataframes["employee_weekly_calendars"].iterrows():
                s.add(EmployeeWeeklyCalendar(employee_id=eid[r["employee_code"]], week_in_cycle=int(r["week_in_cycle"]), weekday=int(r["weekday"]), start_time=_tm(r["start_time"], "employee_weekly_calendars", 0, "start_time", []), end_time=_tm(r["end_time"], "employee_weekly_calendars", 0, "end_time", []), shift_code=r["shift_code"], is_working=_b(r["is_working"], "employee_weekly_calendars", 0, "is_working", []), note=_none(r["note"])))
            out["employee_weekly_calendars"] = len(context.dataframes["employee_weekly_calendars"])

            for _, r in context.dataframes["employee_calendars"].iterrows():
                s.add(EmployeeCalendar(employee_id=eid[r["employee_code"]], start_time=_dt(r["start_time"], "employee_calendars", 0, "start_time", []), end_time=_dt(r["end_time"], "employee_calendars", 0, "end_time", []), is_working=_b(r["is_working"], "employee_calendars", 0, "is_working", []), note=_none(r["note"])))
            out["employee_calendars"] = len(context.dataframes["employee_calendars"])

            rid: dict[str, int] = {}
            for _, r in context.dataframes["process_routes"].iterrows():
                o = ProcessRoute(code=r["code"], name=r["name"], target_material_id=matid[r["target_material_code"]], note=_none(r["note"]))
                s.add(o); s.flush(); rid[r["code"]] = o.id
            out["process_routes"] = len(rid)

            sid: dict[tuple[str, str], int] = {}
            for _, r in context.dataframes["route_steps"].iterrows():
                o = RouteStep(route_id=rid[r["route_code"]], code=r["code"], name=r["name"], process_name=r["process_name"], display_order=int(r["display_order"]), execution_mode=StepExecutionMode(r["execution_mode"]), output_material_id=matid[r["output_material_code"]], output_qty_per_execution=Decimal(r["output_qty_per_execution"]), output_uom_code=r["output_uom_code"])
                s.add(o); s.flush(); sid[(r["route_code"], r["code"])] = o.id
            out["route_steps"] = len(context.dataframes["route_steps"])

            for _, r in context.dataframes["route_step_inputs"].iterrows():
                s.add(RouteStepInput(step_id=sid[(r["route_code"], r["step_code"])], material_id=matid[r["material_code"]], input_qty_per_execution=Decimal(r["input_qty_per_execution"]), input_uom_code=r["input_uom_code"], consumption_mode=ConsumptionMode(r["consumption_mode"])))
            out["route_step_inputs"] = len(context.dataframes["route_step_inputs"])

            for _, r in context.dataframes["route_step_machine_types"].iterrows():
                s.add(RouteStepMachineType(step_id=sid[(r["route_code"], r["step_code"])], machine_type_id=mtid[r["machine_type_code"]], duration_min_override=int(r["duration_min_override"]), capacity_per_execution=Decimal(r["capacity_per_execution"]), capacity_uom_code=r["capacity_uom_code"]))
            out["route_step_machine_types"] = len(context.dataframes["route_step_machine_types"])

            for _, r in context.dataframes["route_step_dependencies"].iterrows():
                s.add(RouteStepDependency(predecessor_step_id=sid[(r["route_code"], r["predecessor_step_code"])], successor_step_id=sid[(r["route_code"], r["successor_step_code"])]))
            out["route_step_dependencies"] = len(context.dataframes["route_step_dependencies"])

            for _, r in context.dataframes["step_quantity_conversions"].iterrows():
                s.add(StepQuantityConversion(step_id=sid[(r["route_code"], r["step_code"])], from_uom_code=r["from_uom_code"], from_qty=Decimal(r["from_qty"]), to_uom_code=r["to_uom_code"], to_qty=Decimal(r["to_qty"]), conversion_type=r["conversion_type"]))
            out["step_quantity_conversions"] = len(context.dataframes["step_quantity_conversions"])

            for _, r in context.dataframes["orders"].iterrows():
                rc = r["route_code"]
                s.add(Order(code=r["code"], priority=int(r["priority"]), due_date=_dt(r["due_date"], "orders", 0, "due_date", []), requested_material_id=matid[r["requested_material_code"]], requested_qty=Decimal(r["requested_qty"]), route_id=rid[rc] if rc else None, status=OrderStatus(r["status"]), note=_none(r["note"])))
            out["orders"] = len(context.dataframes["orders"])

            if IMPORT_SCHEDULE_TASKS:
                oid = {x.code: x.id for x in s.query(Order).all()}
                for _, r in context.dataframes["schedule_tasks"].iterrows():
                    s.add(ScheduleTask(order_id=oid[r["order_code"]], route_id=rid[r["route_code"]], step_id=sid[(r["route_code"], r["step_code"])], machine_id=mid.get(r["machine_code"]) if r["machine_code"] else None, employee_id=eid.get(r["employee_code"]) if r["employee_code"] else None, planned_qty=Decimal(r["planned_qty"]), planned_start=_dt(r["planned_start"], "schedule_tasks", 0, "planned_start", []) if r["planned_start"] else None, planned_end=_dt(r["planned_end"], "schedule_tasks", 0, "planned_end", []) if r["planned_end"] else None, status=TaskStatus(r["status"])))
                out["schedule_tasks"] = len(context.dataframes["schedule_tasks"])

    out["db_path"] = str(db_file)
    return out


def main() -> None:
    start = datetime.now()
    logger.info(f"INFO order_file={ORDER_FILE_NAME} import_schedule_tasks={IMPORT_SCHEDULE_TASKS} drop_existing_db={DROP_EXISTING_DB}")

    ctx = _load_and_validate_data()
    cnt = _import_data(ctx)

    # logger.info("INFO validation_row_counts:")
    # for t, c in ctx.row_counts.items():
    #     logger.info(f"  - {t}: {c}")
    # 
    logger.info("INFO imported_row_counts:")
    for t, c in cnt.items():
        if t != "db_path":
            logger.info(f"  - {t}: {c}")

    if not IMPORT_SCHEDULE_TASKS:
        logger.info("INFO schedule_tasks import is disabled by configuration (IMPORT_SCHEDULE_TASKS=False).")

    logger.info(f"INFO db_file={cnt['db_path']}")
    logger.info(f"INFO elapsed_seconds={(datetime.now() - start).total_seconds():.2f}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"IMPORT_FAILED: {exc}")
