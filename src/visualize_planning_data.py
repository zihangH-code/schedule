from __future__ import annotations

import argparse
import csv
import html
import sqlite3
import textwrap
from collections import Counter, defaultdict, deque
from datetime import datetime
from pathlib import Path
from loguru import logger


SRC_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SRC_DIR.parent
DEFAULT_DB = PROJECT_ROOT / "db" / "planning_demo.sqlite"
DEFAULT_OUT_DIR = PROJECT_ROOT / "reports" / "planning_viz"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _pick_main_input(
    input_rows: list[dict[str, str]],
    material_type_by_code: dict[str, str],
) -> dict[str, str] | None:
    non_aux = [r for r in input_rows if material_type_by_code.get(r["material_code"], "") != "auxiliary"]
    if not non_aux:
        return None
    wip = [r for r in non_aux if "__" in str(r["material_code"])]
    pool = wip if wip else non_aux
    return max(pool, key=lambda x: float(x.get("input_qty_per_execution", "0") or "0"))


def build_step_uom_classification_rows(
    data_dir: Path = DEFAULT_DATA_DIR,
) -> list[dict[str, str]]:
    """基于 data/*.csv 构建工序输入/产能/输出的单位分类明细。"""
    steps = _read_csv_rows(data_dir / "route_steps.csv")
    inputs = _read_csv_rows(data_dir / "route_step_inputs.csv")
    machine_caps = _read_csv_rows(data_dir / "route_step_machine_types.csv")
    materials = _read_csv_rows(data_dir / "materials.csv")

    material_type_by_code = {r["code"]: normalize_material_type(r.get("material_type", "")) for r in materials}
    inputs_by_step: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    caps_by_step: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for r in inputs:
        inputs_by_step[(r["route_code"], r["step_code"])].append(r)
    for r in machine_caps:
        caps_by_step[(r["route_code"], r["step_code"])].append(r)

    detail_rows: list[dict[str, str]] = []
    for step in steps:
        key = (step["route_code"], step["code"])
        step_inputs = inputs_by_step.get(key, [])
        if not step_inputs:
            continue
        main_input = _pick_main_input(step_inputs, material_type_by_code)
        if main_input is None:
            continue
        cap_rows = caps_by_step.get(key, [])
        if not cap_rows:
            continue
        cap_row = cap_rows[0]

        main_uom = str(main_input.get("input_uom_code", ""))
        cap_uom = str(cap_row.get("capacity_uom_code", ""))
        out_uom = str(step.get("output_uom_code", ""))
        uom_triple = f"{main_uom}->{cap_uom}->{out_uom}"
        is_all_pcs = main_uom == "pcs" and cap_uom == "pcs" and out_uom == "pcs"

        class_label = "other"
        if is_all_pcs:
            class_label = "batch_pcs_pcs_pcs" if step["execution_mode"] == "batch" else "single_pcs_pcs_pcs"
        elif main_uom == "kg" and out_uom == "pcs":
            class_label = "batch_kg_to_pcs" if step["execution_mode"] == "batch" else "single_kg_to_pcs"

        detail_rows.append(
            {
                "route_code": step["route_code"],
                "step_code": step["code"],
                "step_name": step["name"],
                "process_name": step["process_name"],
                "execution_mode": step["execution_mode"],
                "machine_type_code": cap_row.get("machine_type_code", ""),
                "duration_min": cap_row.get("duration_min_override", ""),
                "main_input_material": main_input.get("material_code", ""),
                "main_input_qty": main_input.get("input_qty_per_execution", ""),
                "main_input_uom": main_uom,
                "capacity_per_execution": cap_row.get("capacity_per_execution", ""),
                "capacity_uom": cap_uom,
                "output_qty_per_execution": step.get("output_qty_per_execution", ""),
                "output_uom": out_uom,
                "uom_triple": uom_triple,
                "is_all_pcs": "True" if is_all_pcs else "False",
                "class_label": class_label,
            }
        )
    return sorted(detail_rows, key=lambda r: (r["class_label"], r["route_code"], r["step_code"]))


def esc(text: object) -> str:
    return html.escape(str(text))


def qfetchall(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def normalize_material_type(raw: object) -> str:
    text = str(raw or "").strip()
    if "." in text:
        text = text.split(".")[-1]
    text = text.lower()
    mapping = {
        "raw_material": "raw_material",
        "auxiliary": "auxiliary",
        "intermediate_product": "intermediate_product",
        "product": "product",
    }
    return mapping.get(text, text)


def fmt_dt_text(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            continue
    return text


def render_step_uom_audit_page(detail_rows: list[dict[str, str]]) -> str:
    summary = Counter(row["class_label"] for row in detail_rows)
    summary_cards = "".join(
        "<div class='stat'>"
        f"<div class='k'>{esc(label)}</div>"
        f"<div class='v'>{count}</div>"
        "</div>"
        for label, count in sorted(summary.items())
    )
    summary_rows = "".join(
        "<tr>"
        f"<td>{esc(label)}</td>"
        f"<td>{count}</td>"
        "</tr>"
        for label, count in sorted(summary.items())
    )
    detail_html = "".join(
        "<tr>"
        f"<td>{esc(row['class_label'])}</td>"
        f"<td>{esc(row['route_code'])}</td>"
        f"<td>{esc(row['step_code'])}</td>"
        f"<td>{esc(row['step_name'])}</td>"
        f"<td>{esc(row['execution_mode'])}</td>"
        f"<td>{esc(row['main_input_qty'])}{esc(row['main_input_uom'])}</td>"
        f"<td>{esc(row['capacity_per_execution'])}{esc(row['capacity_uom'])}</td>"
        f"<td>{esc(row['output_qty_per_execution'])}{esc(row['output_uom'])}</td>"
        f"<td>{esc(row['duration_min'])} min</td>"
        f"<td>{esc(row['machine_type_code'])}</td>"
        "</tr>"
        for row in detail_rows
    )
    body = (
        "<div class='card'><h2>工序单位分类总览</h2>"
        "<div class='hint'>工序审计统一在 HTML 页面展示，不再由可视化脚本生成 reports/data_audit 下的分类 CSV。</div>"
        f"<div class='grid4'>{summary_cards}</div></div>"
        "<div class='card'><h2>分类汇总</h2>"
        "<table><thead><tr><th>分类</th><th>工序数</th></tr></thead>"
        f"<tbody>{summary_rows}</tbody></table></div>"
        "<div class='card'><h2>工序明细</h2>"
        "<table><thead><tr>"
        "<th>分类</th><th>路线</th><th>工序编码</th><th>工序名称</th><th>处理类型</th>"
        "<th>主输入</th><th>容纳能力</th><th>输出</th><th>运行时间</th><th>设备类型</th>"
        "</tr></thead>"
        f"<tbody>{detail_html}</tbody></table></div>"
    )
    return page_shell("工序审计", body)


def page_shell(title: str, body: str, back_href: str = "./index.html") -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <title>{esc(title)}</title>
  <style>
    :root {{
      --bg: #f4f8fb;
      --card: #ffffff;
      --text: #0f172a;
      --muted: #475569;
      --line: #cbd5e1;
      --head: #e6edf4;
      --accent: #0ea5e9;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", Arial, sans-serif; color: var(--text); background: var(--bg); }}
    .wrap {{ max-width: 1280px; margin: 0 auto; padding: 18px; }}
    .top {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }}
    .back {{ color: #0369a1; text-decoration: none; font-weight: 600; }}
    .card {{ border: 1px solid var(--line); border-radius: 12px; background: var(--card); padding: 14px; margin-bottom: 14px; }}
    h1 {{ margin: 0; font-size: 24px; }}
    h2 {{ margin: 0 0 8px 0; font-size: 18px; }}
    h3 {{ margin: 12px 0 6px 0; font-size: 15px; color: #1e293b; }}
    .meta {{ color: var(--muted); font-size: 13px; margin-top: 4px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 8px; }}
    th, td {{ border: 1px solid var(--line); padding: 6px 8px; text-align: left; font-size: 13px; vertical-align: top; }}
    th {{ background: var(--head); }}
    .grid4 {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }}
    .stat {{ border: 1px solid var(--line); border-radius: 10px; background: #f8fcff; padding: 10px; }}
    .stat .k {{ font-size: 12px; color: var(--muted); }}
    .stat .v {{ font-size: 22px; font-weight: 700; margin-top: 4px; }}
    .navcards {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }}
    .navcard {{ border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: #fff; }}
    .navcard a {{ font-size: 16px; font-weight: 700; color: #075985; text-decoration: none; }}
    .hint {{ font-size: 12px; color: var(--muted); }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>{esc(title)}</h1>
      <a class="back" href="{esc(back_href)}">返回首页</a>
    </div>
    {body}
  </div>
</body>
</html>
"""


def topological_levels(step_ids: list[int], edges: list[tuple[int, int]]) -> dict[int, int]:
    succ: dict[int, list[int]] = defaultdict(list)
    indeg = {sid: 0 for sid in step_ids}
    for src, dst in edges:
        succ[src].append(dst)
        indeg[dst] += 1
    queue = deque([sid for sid in step_ids if indeg[sid] == 0])
    level = {sid: 0 for sid in step_ids}
    visited = 0
    while queue:
        curr = queue.popleft()
        visited += 1
        for nxt in succ[curr]:
            level[nxt] = max(level[nxt], level[curr] + 1)
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                queue.append(nxt)
    if visited != len(step_ids):
        raise ValueError("检测到工艺路线 DAG 存在环。")
    return level


def wrap_line(text: str, width: int = 44, continuation_prefix: str = "  ") -> list[str]:
    if len(text) <= width:
        return [text]
    wrapped = textwrap.wrap(
        text,
        width=width,
        break_long_words=True,
        break_on_hyphens=False,
    )
    if not wrapped:
        return [text]
    out = [wrapped[0]]
    out.extend(f"{continuation_prefix}{line}" for line in wrapped[1:])
    return out


def zh_mode(raw: str) -> str:
    mode = (raw or "").strip().lower()
    if mode == "single":
        return "单件"
    if mode == "batch":
        return "批处理"
    return raw or "-"
def block_lines(title: str, items: list[str], width: int = 58, max_items: int = 6) -> list[str]:
    if not items:
        return [f"{title}: -"]
    shown = items[:max_items]
    lines: list[str] = []
    lines.extend(wrap_line(f"{title}: {shown[0]}", width, continuation_prefix="    "))
    for item in shown[1:]:
        lines.extend(wrap_line(f"  - {item}", width, continuation_prefix="    "))
    if len(items) > max_items:
        lines.append(f"  ... 其余 {len(items) - max_items} 项未展开")
    return lines
def zh_consumption_mode(raw: str) -> str:
    t = (raw or "").strip().lower()
    mp = {
        "fixed_per_execution": "固定/每次",
        "proportional_to_output": "按产出比例",
        "carrier_transfer": "载具流转",
        "packaging_per_pack": "按包装单元",
    }
    return mp.get(t, raw or "-")
def zh_conversion_type(raw: str) -> str:
    t = (raw or "").strip().lower()
    if t == "uom_transform":
        return "量纲转换"
    if t == "ratio_transform":
        return "比例转换"
    return raw or "-"
def fmt_qty(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        return "0"
    try:
        val = float(text)
    except ValueError:
        return text
    if abs(val - round(val)) < 1e-9:
        return str(int(round(val)))
    return f"{val:.4f}".rstrip("0").rstrip(".")
def build_vertical_route_svg(route_title: str, step_ids: list[int], node_lines: dict[int, list[str]], edges: list[tuple[int, int]]) -> str:
    level = topological_levels(step_ids, edges)
    by_level: dict[int, list[int]] = defaultdict(list)
    for sid in step_ids:
        by_level[level[sid]].append(sid)
    for lev in by_level:
        by_level[lev].sort()

    max_line_count = max((len(node_lines.get(sid, [])) for sid in step_ids), default=8)
    box_w = 560
    box_h = max(240, 52 + max_line_count * 20)
    col_gap = 36
    row_gap = 110
    margin_x = 30
    margin_y = 56
    title_h = 28

    max_level = max(level.values()) if level else 0
    max_cols = max((len(v) for v in by_level.values()), default=1)
    graph_w = max_cols * box_w + (max_cols - 1) * col_gap
    width = margin_x * 2 + graph_w
    height = margin_y * 2 + title_h + (max_level + 1) * box_h + max_level * row_gap

    pos: dict[int, tuple[int, int]] = {}
    for lev in range(max_level + 1):
        nodes = by_level.get(lev, [])
        row_w = len(nodes) * box_w + max(0, len(nodes) - 1) * col_gap
        row_start_x = margin_x + (graph_w - row_w) // 2
        for idx, sid in enumerate(nodes):
            pos[sid] = (row_start_x + idx * (box_w + col_gap), margin_y + title_h + lev * (box_h + row_gap))

    parts: list[str] = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        'style="background:#f8fafc;border:1px solid #dbe2ea;border-radius:12px">'
    )
    parts.append(
        '<defs><marker id="arrowV" markerWidth="10" markerHeight="7" refX="5" refY="3.5" orient="auto">'
        '<polygon points="0 0, 10 3.5, 0 7" fill="#64748b"/></marker></defs>'
    )
    parts.append(
        f'<text x="{margin_x}" y="{margin_y - 8}" fill="#0f172a" font-size="16" '
        f'font-family="Segoe UI, Arial">{esc(route_title)} 工艺DAG树</text>'
    )

    for src, dst in edges:
        sx, sy = pos[src]
        tx, ty = pos[dst]
        x1 = sx + box_w // 2
        y1 = sy + box_h
        x2 = tx + box_w // 2
        y2 = ty
        c1y = y1 + row_gap // 2
        c2y = y2 - row_gap // 2
        parts.append(
            f'<path d="M {x1} {y1} C {x1} {c1y}, {x2} {c2y}, {x2} {y2}" '
            'stroke="#94a3b8" fill="none" stroke-width="2" marker-end="url(#arrowV)"/>'
        )

    for sid in step_ids:
        x, y = pos[sid]
        parts.append(
            f'<rect x="{x}" y="{y}" width="{box_w}" height="{box_h}" rx="10" ry="10" '
            'fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5"/>'
        )
        lines = node_lines.get(sid, [])
        yline = y + 24
        for idx, line in enumerate(lines):
            color = "#0f172a" if idx == 0 else "#334155"
            size = "14" if idx == 0 else "12"
            weight = "700" if idx == 0 else "400"
            parts.append(
                f'<text x="{x + 10}" y="{yline}" fill="{color}" font-size="{size}" font-weight="{weight}" '
                f'font-family="Segoe UI, Arial">{esc(line)}</text>'
            )
            yline += 18
    parts.append("</svg>")
    return "\n".join(parts)


def build_route_page(
    route: sqlite3.Row,
    steps: list[sqlite3.Row],
    main_inputs_by_step: dict[int, list[str]],
    aux_inputs_by_step: dict[int, list[str]],
    machines_by_step: dict[int, list[tuple[str, str]]],
    conversions_by_step: dict[int, list[str]],
    deps: list[tuple[int, int]],
) -> str:
    step_ids = [s["id"] for s in steps]
    node_lines: dict[int, list[str]] = {}
    for s in steps:
        sid = s["id"]
        lines: list[str] = []
        lines.extend(wrap_line(f"工序: {s['code']} | {s['name']}", 58))
        lines.extend(wrap_line(f"执行方式: {zh_mode(str(s['execution_mode']))}", 58))
        lines.extend(block_lines("主料输入", main_inputs_by_step.get(sid, []), 58, 6))
        lines.extend(block_lines("辅料输入", aux_inputs_by_step.get(sid, []), 58, 6))
        lines.extend(wrap_line(f"输出: {s['output_material_code']} x {fmt_qty(s['output_qty_per_execution'])} {s['output_uom_code']}", 58))
        machine_pairs = machines_by_step.get(sid, [])
        if not machine_pairs:
            lines.append("设备能力: -")
        else:
            for idx, (cap_line, dur_line) in enumerate(machine_pairs[:6]):
                prefix = "设备能力: " if idx == 0 else "  - "
                lines.extend(wrap_line(f"{prefix}{cap_line}", 58, continuation_prefix="    "))
                lines.extend(wrap_line(f"    {dur_line}", 58, continuation_prefix="    "))
            if len(machine_pairs) > 6:
                lines.append(f"  ... 其余 {len(machine_pairs) - 6} 项未展开")
        lines.extend(block_lines("数量转换", conversions_by_step.get(sid, []), 58, 6))
        node_lines[sid] = lines

    svg = build_vertical_route_svg(route["name"], step_ids, node_lines, deps)
    body = (
        f'<div class="card"><div class="meta">路线编码={esc(route["code"])} | 目标物料={esc(route["target_material_name"])} | 步骤数={len(steps)} | 依赖边={len(deps)}</div></div>'
        f'<div class="card" style="overflow:auto">{svg}</div>'
    )
    return page_shell(f"工艺路线: {route['name']}", body)
def main() -> None:
    parser = argparse.ArgumentParser(description="生成更易读的计划模型可视化页面。")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 数据库路径")
    parser.add_argument("--out", default=str(DEFAULT_OUT_DIR), help="输出目录")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    for old_html in out_dir.glob("*.html"):
        old_html.unlink(missing_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        materials = qfetchall(
            conn,
            """
            select m.id, m.code, m.name, m.material_type, m.uom, coalesce(i.available_qty, 0) as available_qty
            from materials m
            left join inventories i on i.material_id = m.id
            order by m.code
            """,
        )
        material_purchases = qfetchall(conn, "select material_id from material_purchases")
        routes = qfetchall(conn, """
            select pr.id, pr.code, pr.name, m.name as target_material_name
            from process_routes pr join materials m on m.id = pr.target_material_id
            order by pr.code
        """)
        workshops = qfetchall(conn, "select id, code, name, coalesce(note,'') as note from workshops order by code")
        machine_types = qfetchall(conn, """
            select mt.id, mt.code, mt.name, mt.workshop_id, ws.code as workshop_code
            from machine_types mt join workshops ws on ws.id=mt.workshop_id order by mt.code
        """)
        machines = qfetchall(conn, """
            select ma.id, ma.code, ma.name, ma.machine_type_id, mt.code as machine_type_code, ws.code as workshop_code
            from machines ma
            join machine_types mt on mt.id = ma.machine_type_id
            join workshops ws on ws.id = mt.workshop_id
            order by ma.code
        """)
        machine_cals = qfetchall(conn, """
            select machine_id, start_time, end_time, is_working, coalesce(note,'') as note
            from machine_calendars order by machine_id, start_time
        """)
        employees = qfetchall(conn, """
            select e.id, e.code, e.name, e.workshop_id, ws.code as workshop_code, e.status, coalesce(e.note,'') as note
            from employees e join workshops ws on ws.id=e.workshop_id order by e.code
        """)
        emp_week = qfetchall(conn, """
            select employee_id, week_in_cycle, weekday, start_time, end_time, shift_code, is_working, coalesce(note,'') as note
            from employee_weekly_calendars order by employee_id, week_in_cycle, weekday, start_time
        """)
        orders = qfetchall(
            conn,
            """
            select o.id, o.code, o.priority, o.due_date, o.requested_qty, o.status,
                   m.code as requested_material_code
            from orders o
            join materials m on m.id = o.requested_material_id
            order by o.priority, o.due_date, o.code
            """,
        )
    finally:
        conn.close()

    purchase_mat_ids = {int(r["material_id"]) for r in material_purchases}
    mat_counter = Counter(normalize_material_type(r["material_type"]) for r in materials)
    purchasable_raw = sum(1 for r in materials if normalize_material_type(r["material_type"]) == "raw_material" and int(r["id"]) in purchase_mat_ids)
    orders_by_status = Counter(str(r["status"]) for r in orders)

    ws_by_id = {int(r["id"]): r for r in workshops}
    mt_by_id = {int(r["id"]): r for r in machine_types}
    machine_by_id = {int(r["id"]): r for r in machines}
    emp_by_id = {int(r["id"]): r for r in employees}

    mt_count_by_ws = Counter(str(r["workshop_code"]) for r in machine_types)
    machine_count_by_ws = Counter(str(r["workshop_code"]) for r in machines)
    emp_count_by_ws = Counter(str(r["workshop_code"]) for r in employees)

    maint_by_machine = Counter()
    for r in machine_cals:
        if int(r["is_working"]) == 0:
            maint_by_machine[int(r["machine_id"])] += 1
    maint_count_by_ws = Counter()
    for mid, cnt in maint_by_machine.items():
        ws = str(machine_by_id[mid]["workshop_code"])
        maint_count_by_ws[ws] += cnt

    day_cover: dict[tuple[str, int, int], int] = Counter()
    night_cover: dict[tuple[str, int, int], int] = Counter()
    week_rows_by_emp: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for r in emp_week:
        eid = int(r["employee_id"])
        week_rows_by_emp[eid].append(r)
        if int(r["is_working"]) == 0:
            continue
        ws = str(emp_by_id[eid]["workshop_code"])
        cycle = int(r["week_in_cycle"])
        weekday = int(r["weekday"])
        if weekday > 5:
            continue
        if str(r["shift_code"]).lower() == "day":
            day_cover[(ws, cycle, weekday)] += 1
        elif str(r["shift_code"]).lower() == "night" and str(r["start_time"]).startswith("20:00:00"):
            night_cover[(ws, cycle, weekday)] += 1

    # Route pages + route list page
    route_index_rows: list[str] = []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        for route in routes:
            steps = qfetchall(conn, """
                select rs.id, rs.code, rs.name, rs.process_name, rs.execution_mode,
                       m.code as output_material_code, rs.output_qty_per_execution, rs.output_uom_code
                from route_steps rs
                join materials m on m.id = rs.output_material_id
                where rs.route_id = ?
                order by rs.display_order, rs.id
            """, (route["id"],))
            step_ids = [int(s["id"]) for s in steps]
            if not step_ids:
                continue

            dep_rows = qfetchall(
                conn,
                "select predecessor_step_id, successor_step_id from route_step_dependencies where predecessor_step_id in ({ids}) and successor_step_id in ({ids})".format(
                    ids=",".join("?" for _ in step_ids)
                ),
                tuple(step_ids + step_ids),
            )
            deps = [(int(r["predecessor_step_id"]), int(r["successor_step_id"])) for r in dep_rows]

            input_rows = qfetchall(
                conn,
                "select rsi.step_id, m.code as material_code, m.material_type, rsi.input_qty_per_execution, rsi.input_uom_code, rsi.consumption_mode from route_step_inputs rsi join materials m on m.id=rsi.material_id where rsi.step_id in ({ids}) order by rsi.step_id, m.code".format(
                    ids=",".join("?" for _ in step_ids)
                ),
                tuple(step_ids),
            )
            main_inputs_by_step: dict[int, list[str]] = defaultdict(list)
            aux_inputs_by_step: dict[int, list[str]] = defaultdict(list)
            for row in input_rows:
                line = (
                    f'{row["material_code"]} x {fmt_qty(row["input_qty_per_execution"])} '
                    f'{row["input_uom_code"]}'
                )
                mtype = normalize_material_type(row["material_type"])
                if mtype == "auxiliary":
                    aux_inputs_by_step[int(row["step_id"])].append(line)
                else:
                    main_inputs_by_step[int(row["step_id"])].append(line)

            mt_rows = qfetchall(
                conn,
                "select rsmt.step_id, mt.code as machine_type_code, ws.code as workshop_code, rsmt.duration_min_override, rsmt.capacity_per_execution, rsmt.capacity_uom_code "
                "from route_step_machine_types rsmt "
                "join machine_types mt on mt.id=rsmt.machine_type_id "
                "join workshops ws on ws.id=mt.workshop_id "
                "where rsmt.step_id in ({ids}) order by rsmt.step_id, mt.code".format(ids=",".join("?" for _ in step_ids)),
                tuple(step_ids),
            )
            machines_by_step: dict[int, list[tuple[str, str]]] = defaultdict(list)
            for row in mt_rows:
                machines_by_step[int(row["step_id"])].append(
                    (
                        f'容纳能力：{fmt_qty(row["capacity_per_execution"])}{row["capacity_uom_code"]}',
                        f'运行时间={fmt_qty(row["duration_min_override"])} 分钟',
                    )
                )
            conv_rows = qfetchall(
                conn,
                "select step_id, from_uom_code, from_qty, to_uom_code, to_qty, conversion_type "
                "from step_quantity_conversions where step_id in ({ids}) order by step_id, id".format(ids=",".join("?" for _ in step_ids)),
                tuple(step_ids),
            )
            conversions_by_step: dict[int, list[str]] = defaultdict(list)
            for row in conv_rows:
                conversions_by_step[int(row["step_id"])].append(
                    f'{fmt_qty(row["from_qty"])} {row["from_uom_code"]} -> {fmt_qty(row["to_qty"])} {row["to_uom_code"]}'
                )

            route_file = out_dir / f'route_{route["id"]}.html'
            route_file.write_text(
                build_route_page(
                    route,
                    steps,
                    main_inputs_by_step,
                    aux_inputs_by_step,
                    machines_by_step,
                    conversions_by_step,
                    deps,
                ),
                encoding="utf-8",
            )
            route_index_rows.append(
                "<tr>"
                f"<td>{esc(route['code'])}</td><td>{esc(route['name'])}</td><td>{esc(route['target_material_name'])}</td>"
                f"<td>{len(steps)}</td><td>{len(deps)}</td><td><a href='./route_{route['id']}.html'>打开</a></td>"
                "</tr>"
            )
    finally:
        conn.close()

    routes_page = page_shell(
        "工艺路线页面",
        "<div class='card'><div class='meta'>每一行可进入对应路线 DAG 页面，查看步骤主体信息。</div>"
        "<table><thead><tr><th>路线编码</th><th>路线名称</th><th>目标物料</th><th>步骤数</th><th>依赖边</th><th>查看</th></tr></thead>"
        f"<tbody>{''.join(route_index_rows)}</tbody></table></div>",
    )
    (out_dir / "routes.html").write_text(routes_page, encoding="utf-8")

    workshop_rows: list[str] = []
    for ws in workshops:
        code = str(ws["code"])
        mt_list = ", ".join(r["code"] for r in machine_types if str(r["workshop_code"]) == code)
        workshop_rows.append(
            "<tr>"
            f"<td>{esc(code)}</td><td>{esc(ws['name'])}</td>"
            f"<td>{mt_count_by_ws.get(code, 0)}</td><td>{machine_count_by_ws.get(code, 0)}</td>"
            f"<td>{emp_count_by_ws.get(code, 0)}</td><td>{maint_count_by_ws.get(code, 0)}</td>"
            f"<td>{esc(mt_list)}</td></tr>"
        )
    workshops_page = page_shell(
        "车间总览",
        "<div class='card'><h2>车间-资源映射</h2>"
        "<table><thead><tr><th>车间编码</th><th>车间名称</th><th>设备类型数</th><th>设备数</th><th>员工数</th><th>维护事件数</th><th>设备类型列表</th></tr></thead>"
        f"<tbody>{''.join(workshop_rows)}</tbody></table></div>",
    )
    (out_dir / "workshops.html").write_text(workshops_page, encoding="utf-8")

    shift_cover_rows: list[str] = []
    for ws in workshops:
        code = str(ws["code"])
        for cyc in (1, 2):
            day_vals = [str(day_cover.get((code, cyc, wd), 0)) for wd in range(1, 6)]
            night_vals = [str(night_cover.get((code, cyc, wd), 0)) for wd in range(1, 6)]
            shift_cover_rows.append(
                "<tr>"
                f"<td>{esc(code)}</td><td>第 {cyc} 周</td><td>{' / '.join(day_vals)}</td><td>{' / '.join(night_vals)}</td>"
                "</tr>"
            )

    emp_rows: list[str] = []
    for e in employees:
        eid = int(e["id"])
        rows = week_rows_by_emp.get(eid, [])
        week1_day = sum(1 for r in rows if int(r["week_in_cycle"]) == 1 and int(r["is_working"]) == 1 and int(r["weekday"]) <= 5 and str(r["shift_code"]).lower() == "day")
        week1_night = sum(1 for r in rows if int(r["week_in_cycle"]) == 1 and int(r["is_working"]) == 1 and str(r["shift_code"]).lower() == "night")
        week2_day = sum(1 for r in rows if int(r["week_in_cycle"]) == 2 and int(r["is_working"]) == 1 and int(r["weekday"]) <= 5 and str(r["shift_code"]).lower() == "day")
        week2_night = sum(1 for r in rows if int(r["week_in_cycle"]) == 2 and int(r["is_working"]) == 1 and str(r["shift_code"]).lower() == "night")
        weekend_off = all(
            any(int(r["week_in_cycle"]) == cyc and int(r["weekday"]) == 6 and int(r["is_working"]) == 0 for r in rows)
            and any(int(r["week_in_cycle"]) == cyc and int(r["weekday"]) == 7 and int(r["is_working"]) == 0 for r in rows)
            for cyc in (1, 2)
        )
        note = str(e["note"] or "").lower()
        if "group_a" in note:
            template = "A组：第1周白班 / 第2周夜班"
        elif "group_b" in note:
            template = "B组：第1周夜班 / 第2周白班"
        else:
            template = "仅白班：第1周白班 / 第2周白班"
        emp_rows.append(
            "<tr>"
            f"<td>{esc(e['code'])}</td><td>{esc(e['name'])}</td><td>{esc(e['workshop_code'])}</td>"
            f"<td>{esc(template)}</td>"
            f"<td>{week1_day} 个白班 / {week1_night} 个夜班窗口</td>"
            f"<td>{week2_day} 个白班 / {week2_night} 个夜班窗口</td>"
            f"<td>{'是' if weekend_off else '否'}</td></tr>"
        )
    employees_page = page_shell(
        "员工与班次模板",
        "<div class='card'><h2>车间班次覆盖情况（周一至周五，第1/2周）</h2>"
        "<div class='hint'>格式：周一 / ... / 周五</div>"
        "<table><thead><tr><th>车间</th><th>周期周次</th><th>白班覆盖</th><th>夜班覆盖</th></tr></thead>"
        f"<tbody>{''.join(shift_cover_rows)}</tbody></table></div>"
        "<div class='card'><h2>员工双周班次模板汇总</h2>"
        "<div class='hint'>夜班拆分为 20:00-23:59:59 与次日 00:00-06:00；周五夜班于周六 06:00 结束，之后进入周末休息。</div>"
        "<table><thead><tr><th>员工编码</th><th>姓名</th><th>车间</th><th>固定双周模板</th><th>第1周窗口</th><th>第2周窗口</th><th>是否定义周末休息</th></tr></thead>"
        f"<tbody>{''.join(emp_rows)}</tbody></table></div>",
    )
    (out_dir / "employees.html").write_text(employees_page, encoding="utf-8")

    machine_rows: list[str] = []
    for m in machines:
        mid = int(m["id"])
        machine_rows.append(
            "<tr>"
            f"<td>{esc(m['code'])}</td><td>{esc(m['name'])}</td><td>{esc(m['machine_type_code'])}</td>"
            f"<td>{esc(m['workshop_code'])}</td><td>{maint_by_machine.get(mid, 0)}</td></tr>"
        )
    machines_page = page_shell(
        "设备与维护",
        "<div class='card'><h2>设备维护覆盖情况</h2>"
        "<div class='hint'>维护事件数 = machine_calendars 中该设备的非工作记录数量。</div>"
        "<table><thead><tr><th>设备编码</th><th>设备名称</th><th>设备类型</th><th>车间</th><th>维护事件数</th></tr></thead>"
        f"<tbody>{''.join(machine_rows)}</tbody></table></div>",
    )
    (out_dir / "machines.html").write_text(machines_page, encoding="utf-8")

    mat_rows = (
        f"<tr><td>原材料</td><td>{mat_counter.get('raw_material', 0)}</td></tr>"
        f"<tr><td>辅料</td><td>{mat_counter.get('auxiliary', 0)}</td></tr>"
        f"<tr><td>中间产品</td><td>{mat_counter.get('intermediate_product', 0)}</td></tr>"
        f"<tr><td>产品</td><td>{mat_counter.get('product', 0)}</td></tr>"
    )
    raw_inventory_rows = []
    aux_inventory_rows = []
    product_inventory_rows = []
    raw_inventory_total = 0.0
    aux_inventory_total = 0.0
    product_inventory_total = 0.0
    for mat in materials:
        mtype = normalize_material_type(mat["material_type"])
        qty = float(mat["available_qty"] or 0)
        if mtype == "raw_material":
            raw_inventory_total += qty
            raw_inventory_rows.append(
                "<tr>"
                f"<td>{esc(mat['code'])}</td>"
                f"<td>{esc(mat['name'])}</td>"
                f"<td>{esc(mat['uom'])}</td>"
                f"<td>{qty:.4f}</td>"
                "</tr>"
            )
        elif mtype == "auxiliary":
            aux_inventory_total += qty
            aux_inventory_rows.append(
                "<tr>"
                f"<td>{esc(mat['code'])}</td>"
                f"<td>{esc(mat['name'])}</td>"
                f"<td>{esc(mat['uom'])}</td>"
                f"<td>{qty:.4f}</td>"
                "</tr>"
            )
        elif mtype == "product":
            product_inventory_total += qty
            product_inventory_rows.append(
                "<tr>"
                f"<td>{esc(mat['code'])}</td>"
                f"<td>{esc(mat['name'])}</td>"
                f"<td>{esc(mat['uom'])}</td>"
                f"<td>{qty:.4f}</td>"
                "</tr>"
            )
    materials_page = page_shell(
        "物料与库存",
        "<div class='card'><h2>物料汇总</h2>"
        "<table><thead><tr><th>类型</th><th>数量</th></tr></thead>"
        f"<tbody>{mat_rows}</tbody></table><div class='meta'>可采购原材料数={purchasable_raw}</div></div>"
        "<div class='card'><h2>原材料库存</h2>"
        "<table><thead><tr><th>编码</th><th>名称</th><th>单位</th><th>可用数量</th></tr></thead>"
        f"<tbody>{''.join(raw_inventory_rows)}</tbody></table>"
        f"<div class='meta'>原材料库存合计={raw_inventory_total:.4f}</div></div>"
        "<div class='card'><h2>辅料库存</h2>"
        "<div class='hint'>辅料只允许作为工序输入，不作为工艺路线输出。</div>"
        "<table><thead><tr><th>编码</th><th>名称</th><th>单位</th><th>可用数量</th></tr></thead>"
        f"<tbody>{''.join(aux_inventory_rows)}</tbody></table>"
        f"<div class='meta'>辅料库存合计={aux_inventory_total:.4f}</div></div>"
        "<div class='card'><h2>成品库存</h2>"
        "<table><thead><tr><th>编码</th><th>名称</th><th>单位</th><th>可用数量</th></tr></thead>"
        f"<tbody>{''.join(product_inventory_rows)}</tbody></table>"
        f"<div class='meta'>产品库存合计={product_inventory_total:.4f}</div></div>",
    )
    (out_dir / "materials.html").write_text(materials_page, encoding="utf-8")

    order_rows = []
    for row in orders:
        order_rows.append(
            "<tr>"
            f"<td>{esc(row['code'])}</td>"
            f"<td>{esc(fmt_dt_text(row['due_date']))}</td>"
            f"<td>{esc(row['priority'])}</td>"
            f"<td>{esc(row['requested_material_code'])}</td>"
            f"<td>{esc(row['requested_qty'])}</td>"
            f"<td>{esc(row['status'])}</td>"
            "</tr>"
        )
    ord_status_rows = "".join(f"<tr><td>{esc(k)}</td><td>{v}</td></tr>" for k, v in sorted(orders_by_status.items()))
    orders_page = page_shell(
        "订单",
        "<div class='card'><h2>订单明细</h2>"
        "<table><thead><tr><th>订单编码</th><th>交期</th><th>优先级</th><th>需求产品</th><th>需求数量</th><th>状态</th></tr></thead>"
        f"<tbody>{''.join(order_rows)}</tbody></table></div>"
        "<div class='card'><h2>订单状态汇总</h2>"
        "<table><thead><tr><th>状态</th><th>数量</th></tr></thead>"
        f"<tbody>{ord_status_rows}</tbody></table></div>",
    )
    (out_dir / "orders.html").write_text(orders_page, encoding="utf-8")

    step_uom_rows = build_step_uom_classification_rows(data_dir=DEFAULT_DATA_DIR)
    audit_page = render_step_uom_audit_page(step_uom_rows)
    (out_dir / "process_audit.html").write_text(audit_page, encoding="utf-8")

    main_body = (
        "<div class='card'><div class='grid4'>"
        f"<div class='stat'><div class='k'>车间数</div><div class='v'>{len(workshops)}</div></div>"
        f"<div class='stat'><div class='k'>设备类型数</div><div class='v'>{len(machine_types)}</div></div>"
        f"<div class='stat'><div class='k'>设备数</div><div class='v'>{len(machines)}</div></div>"
        f"<div class='stat'><div class='k'>员工数</div><div class='v'>{len(employees)}</div></div>"
        f"<div class='stat'><div class='k'>工艺路线数</div><div class='v'>{len(routes)}</div></div>"
        f"<div class='stat'><div class='k'>物料数</div><div class='v'>{len(materials)}</div></div>"
        f"<div class='stat'><div class='k'>订单数</div><div class='v'>{len(orders)}</div></div>"
        f"<div class='stat'><div class='k'>可采购原材料数</div><div class='v'>{purchasable_raw}</div></div>"
        "</div></div>"
        "<div class='card'><h2>导航</h2><div class='navcards'>"
        "<div class='navcard'><a href='./routes.html'>工艺路线页面</a><div class='meta'>工艺路线列表 + route_<id>.html DAG 页面。</div></div>"
        "<div class='navcard'><a href='./workshops.html'>车间总览</a><div class='meta'>车间与设备类型、员工的拓扑关系。</div></div>"
        "<div class='navcard'><a href='./employees.html'>员工与班次</a><div class='meta'>班次模板可读性与覆盖情况。</div></div>"
        "<div class='navcard'><a href='./machines.html'>设备与维护</a><div class='meta'>按设备查看维护覆盖情况。</div></div>"
        "<div class='navcard'><a href='./materials.html'>物料与库存</a><div class='meta'>物料类型统计与原材料可采购情况。</div></div>"
        "<div class='navcard'><a href='./orders.html'>订单</a><div class='meta'>交期、优先级、需求产品与数量。</div></div>"
        "<div class='navcard'><a href='./process_audit.html'>工序审计</a><div class='meta'>输入、产能、输出与执行方式分类。</div></div>"
        "</div></div>"
        f"<div class='card'><div class='meta'>数据库路径={esc(db_path)}</div></div>"
    )
    index_html = page_shell("计划数据可视化", main_body, back_href="./index.html")
    (out_dir / "index.html").write_text(index_html, encoding="utf-8")
    logger.info(f"可视化页面已写入：{out_dir / 'index.html'}")
    logger.info(f"工序审计页面已写入：{out_dir / 'process_audit.html'}")


if __name__ == "__main__":
    main()
