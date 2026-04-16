# A_sch 排产原型说明

## 1. 项目目标
本仓库用于验证可落地的 APS 原型闭环：
- `CSV -> 校验导入 -> SQLite -> 排产求解 -> 多页 HTML 展示`
- 当前重点：工艺 DAG、设备+员工双资源、车间匹配链路、库存与采购、可解释排产过程。

## 2. 目录结构
- `src/`
  - `models_v1_1.py`：ORM 建模与建库实现
  - `validate_and_import_data_v1_1.py`：v1.1 数据校验与导入（重建 `db/planning_demo.sqlite`）
  - `generate_schedule_from_db.py`：从数据库读取后执行排产并输出报告
  - `visualize_planning_data.py`：主数据可视化与审计页生成
  - `choose_orders_from_csv.py`：从 `orders.csv` 截取子集订单文件
- `data/`：输入 CSV（含 `orders.csv`、`orders_25.csv`、`schedule_tasks.csv` 样例）
- `db/`：SQLite 数据库
- `reports/`：排产报告与门户页面
- `logs/`：脚本运行日志（按天滚动）
- `AGENTS.md`：仓库治理规则
- `schedule_logic.md`：排产业务规则

## 3. 环境要求
- Python 3.10
  - `C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe`
- 主要依赖：
  - `SQLAlchemy >= 2.0`
  - `loguru`

## 4. 运行步骤
### 4.1 分钟级基线链路（原链路）
1. 重建资源数据并导入数据库
```powershell
C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe D:\A_sch\src\validate_and_import_data_v1_1.py
```
说明：
- 当前验证入口为 `orders_25.csv`，在 `src/validate_and_import_data_v1_1.py` 顶部参数 `ORDER_FILE_NAME` 配置。
- 当前导入阶段默认不导入 `data/schedule_tasks.csv`（`IMPORT_SCHEDULE_TASKS=False`）。
- `inventories.csv` 在导入阶段会全量覆盖写入数据库库存表；若库存文件为空将直接报错并阻止排产。

2. 运行排产（一次生成多页 HTML）
```powershell
C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe D:\A_sch\src\original\generate_schedule.py
```
说明：
- 默认输出为低噪声实时进度（主进度 + 订单子进度）。
- 如需查看详细里程碑日志，可加 `--verbose`：
```powershell
C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe D:\A_sch\src\original\generate_schedule.py --verbose
```
- 终端输出统一为 `time | level | message` 的单行追加格式（无 `tqdm` 进度条）。
- 运行日志写入 `logs/generate_schedule_from_db_YYYY-MM-DD.log`（按天滚动，保留 14 天）。
- `--verbose` 时文件日志级别提升为 `DEBUG`，终端仍保持主线平滑输出。

3. 生成主数据路线图
```powershell
C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe D:\A_sch\src\original\visualize.py
```

### 4.2 Daily 独立链路（按车间按天）
说明：
- 产物隔离在 `data_daily/`、`db/planning_demo_daily.sqlite`、`reports/schedule_daily/`，不会覆盖原链路输出。

1. 生成扩容数据（设备/员工按随机倍率放大）
```powershell
C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe D:\A_sch\src\generate_data_daily.py
```

2. 校验并导入 daily 数据
```powershell
C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe D:\A_sch\src\daily\validate_and_import.py
```

3. 运行日级排产并生成 daily 报告
```powershell
C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe D:\A_sch\src\daily\generate_schedule.py
```

4. 生成 daily 主数据路线图（可选）
```powershell
C:\Users\11941\AppData\Local\Programs\Python\Python310\python.exe D:\A_sch\src\daily\visualize.py
```

4. 查看 daily 报告首页
- `reports/schedule_daily/index.html`

## 5. data 目录关键表
### 5.1 物料与库存
- `materials.csv`：物料主数据（`code`, `material_type`, `uom`, `material_form_type`）
- `material_purchases.csv`：可采购物料规则（`material_code`, `purchase_lead_time_days`，支持 `raw_material/auxiliary`）
- `inventories.csv`：库存（`material_code`, `available_qty`）

### 5.2 车间、设备、员工
- `workshops.csv`：车间主数据（新增）
- `machine_types.csv`：设备类型（固定 20 类）+ `workshop_code`
- `machines.csv`：设备实例（`machine_type_code`）
- `machine_calendars.csv`：设备维护/停机日历（仅此设备日历）
- `employees.csv`：员工主数据 + `workshop_code`
- `employee_weekly_calendars.csv`：员工两周周期模板（`week_in_cycle`, `weekday`, `shift_code`）
- `employee_calendars.csv`：员工例外日历（请假/加班）

### 5.3 工艺路线
- `process_routes.csv`：路线主表
- `route_steps.csv`：工序定义（`execution_mode`, `output_qty_per_execution`, `output_uom_code`）
- `route_step_inputs.csv`：单次执行输入配方（`input_qty_per_execution`, `input_uom_code`, `consumption_mode`）
- `route_step_machine_types.csv`：设备单次执行能力（`capacity_per_execution`, `capacity_uom_code`）
- `step_quantity_conversions.csv`：步骤内数量转换（如 `1 tree -> 70 pcs`）
- `route_step_dependencies.csv`：工序依赖（DAG）
- `dag_validation.csv`：DAG 校验结果

### 5.4 订单与排产结果
- `orders.csv`：默认订单入口
- `orders_25.csv`：25 条订单样例（可通过 `ORDER_FILE_NAME` 切换导入）
- `schedule_tasks.csv`：历史任务样例文件，当前导入脚本默认不落库（仅排产脚本写回数据库 `schedule_tasks` 表）
- `schedule_execution_blocks.csv`：可选导入兼容文件；用于执行层任务合并结果（通常由排产脚本写入数据库）

## 6. 当前资源建模规则
- 匹配链路：`员工 -> 车间 -> 设备类型 -> 设备`
- 技能体系已移除：不再使用 `skills.csv / machine_type_skills.csv / employee_skills.csv`
- 设备周模板日历已移除：不再使用 `machine_weekly_calendars.csv`
- 每台设备至少 1 条维护停机记录（`machine_calendars.csv`）
- 数量语义统一为“单次执行基准”，计划总量不再写入工艺配方字段
- `single` 工序默认 `output_qty_per_execution=1`
- 量纲切换必须通过 `step_quantity_conversions.csv` 显式表达
- `material_type` 新增 `auxiliary`（辅料）：
  - 仅允许出现在 `route_step_inputs.csv`
  - 不允许作为 `route_steps.output_material_code`
  - 在 `batch` 工序容量单位匹配时忽略 `auxiliary` 输入
- 包装工序统一单件化：
  - 识别为包装的步骤统一 `execution_mode=single`
  - 主输入与输出统一按 `1:1` 表达（`input_qty_per_execution=1`, `output_qty_per_execution=1`）
- `batch` 工序设备容量口径：
  - `route_step_machine_types.capacity_uom_code` 必须与该步骤主输入单位一致（主输入优先取内部 WIP，辅料不参与）
- 当前排产执行语义（`generate_schedule_from_db.py`）：
  - `due_date` 作为截止时间，采用 frePPLe 风格：先 backward（JIT）再 forward（ASAP兜底）
  - 槽位搜索基于真实时间边界（默认 90 天视窗），不再使用固定步数截断
  - `batch` 统一按“固定批次时长 + 按容量分批”执行
  - 余量批仍使用固定批次时长（例如 clean 60/40 都按固定时长）
  - `capacity_uom_code != output_uom_code` 时，按 `step_quantity_conversions` 换算单批最大产出
  - 逾期归因采用 `priority + due` 加权惩罚（`0.6/0.4`），失败页展示惩罚值与依据
  - 任务合并生效：
    - `single/batch` 在分配阶段优先连续化，同资源首尾相接任务会在线折叠写入，减少碎片与间隔
    - `batch` 采用“同一 machine 连续优先（employee 可按批次重选），不跨 machine 拼链”，允许最多 1 个小断点（`gap_limit=max(search_step, min(30min, 0.25*base_dur))`）
    - 连续链搜索步长 `search_step = gcd(base_dur, 30)`（最小 1 分钟），避免 10/15 分钟工序被 30 分钟粒度误杀
    - 后置 `_batch_merge_tasks` 默认关闭（`ENABLE_POST_ORDER_MERGE=False`），仅作为可选兜底校验合并，不再承担主要压缩职责
  - 库存事务与净额冲抵：
    - 每个订单以事务方式扣减库存（成功提交、失败回滚），避免“失败订单先扣库存”
    - 订单先用请求物料库存做净额冲抵，净额为 0 时直接记 `planned`（`msg=fulfilled_by_inventory`，零任务）
  - 上游匹配与分块口径：
    - 工序输入优先按 `route_step_dependencies` 前驱链路匹配提供者；无法唯一匹配时按 `ambiguous_provider` 失败
    - `single` 分块按候选设备真实时长在分配阶段计算，避免按首候选设备时长导致的分块偏差
  - 代码职责拆分（脚本内组件）：
    - `DataLoader`：数据库读取与标准化
    - `RequirementPlanner`：需求展开与上游匹配
    - `ResourceAllocator`：资源分配与任务生成
    - `Reporter`：结果对象与展示输出适配

## 7. 员工班制规则
- 车间定员：
  - 有夜班车间（连续工艺）：8人
  - 仅白班车间：4人
- 基础节奏统一：上5休2（周一到周五工作，周六周日休）
- 连续工艺车间（熔炼铸造、热处理）：两周周期互换
  - Week1：A 组白班、B 组夜班
  - Week2：A 组夜班、B 组白班
- 其他车间：两周均为白班（Mon-Fri）
- 白班：`08:00-18:00`
- 夜班拆段：当日 `20:00-23:59:59` + 次日 `00:00-06:00`
- 周五夜班覆盖到周六 `06:00`，夜班周末休假窗口为周六 `06:00` 至周日 `23:59:59`
- 员工请假/加班记录写入 `employee_calendars.csv`

## 8. 主要输出
### 8.1 `reports/schedule/`
- 说明：本目录由 `generate_schedule_from_db.py` 生成，当前只输出 HTML 页面，不输出 CSV 报告。
- 界面：统一工业蓝图风样式（中文优先，参数编码保留英文），首页采用“统计卡片 + 导航卡片 + 快速导航”结构。
- DAG 交互：
  - `order_<code>.html` 支持工序搜索联动高亮（节点 + 明细行）；
  - 支持“展开节点详情 / 收起节点详情”折叠切换，默认紧凑展示。
- 表格交互：
  - `routes.html`、`failed_orders.html`、`problems.html`、`trace.html` 增加关键词筛选工具条（输入、清空、可见行计数）。
- `gantt.html`（Frepple-Lite：资源泳道 + 双层时间轴 + 关键词高亮 + hover 提示）
- `scheduling_process.html`
- `daily_tasks.html`（按日期与车间查看设备+人员安排与当天计划量，支持单日/车间筛选）
- `orders.html` / `routes.html` / `machines.html` / `purchases.html` / `problems.html` / `trace.html`
- `failed_orders.html`（失败订单定位与原因分析）
- `order_<code>.html`（订单对应路线详情页：竖向 DAG + 采购记录 + 父子任务明细）
- `index.html`
- `routes.html` 仅保留路线索引（路线/任务数/订单数/关联订单）与 `reports/planning_viz/route_<id>.html` 跳转，不再生成 `route_<id>.html` / `route_<route_code>.html` 混合路线详情页
- `orders.html` 新增展示列：需求产品、需求数量、实际产量（终工序口径）、订单开始时间（最早任务开始）
- 订单详情页新增链接：可跳转到 `reports/planning_viz/route_<id>.html` 查看基础工艺路线页面

### 8.1.1 `reports/schedule_daily/`
- 说明：本目录由 `src/daily/generate_schedule.py` 生成，复用原页面框架但使用日级结果。
- 页面清单与 `reports/schedule/` 基本一致，核心入口为：
  - `index.html`
  - `daily_tasks.html`（按日期/车间筛选，展示设备+人员+当天计划量）
- 产量口径为日级计划量（`planned_qty` 聚合），用于和真实“按车间按天”排程结构对齐。

### 8.2 `reports/planning_viz/`
- 界面：与 `reports/schedule/` 统一工业蓝图风样式与导航信息层级（中文优先，参数编码保留英文）。
- `index.html`（主页面）
- `routes.html` + `route_<id>.html`（工艺路线总览与分路线 DAG，节点展示“输入 → 候选设备 → 设备容量 → 运行时间 → 输出”）
  - `routes.html` 增加路线筛选工具条；
  - `route_<id>.html` 支持工序搜索联动高亮（DAG 节点 + 工序详情表）与节点折叠切换（默认紧凑展示）。
- `workshops.html`（车间、设备类型、设备、员工映射）
- `employees.html`（员工班次模板与覆盖）
- `machines.html`（设备维修覆盖）
- `materials.html`（物料摘要 + 原材料/辅料/成品库存明细）
- `orders.html`（订单明细：截止日期、优先级、需求产品、需求数量）
- `process_audit.html`（工序输入/容纳/输出/处理类型审计页，统一在 HTML 中查看，含分类与明细筛选工具条）

### 8.2.1 `reports/planning_viz_daily/`
- 说明：本目录由 `src/daily/visualize.py` 生成，展示 `data_daily` + `planning_demo_daily.sqlite` 的主数据可视化。
- 页面结构与 `reports/planning_viz/` 基本一致，便于基线与 daily 链路并行比对。

### 8.3 `reports/data_audit/`
- `machine_capacity_audit.csv`（设备负载能力核查明细，含单位时间能力与审计标记）
- `heavy_process_restore_audit.csv`（重工序从 `single` 恢复为 `batch` 的改动审计）
- 说明：`visualize_planning_data.py` 的工序分类/审计内容统一输出到 `reports/planning_viz/process_audit.html`，不再默认写入这里。

## 9. 治理与文档约束
文档职责边界：
- `AGENTS.md`：仓库治理规范与执行约束。
- `schedule_logic.md`：仅放置排产逻辑与业务规则，不放运行流程、治理条款、兼容性说明、输出目录清单。
- `README.md`：项目说明、运行方式、输出清单、兼容性说明。
- `reports/schedule/*` 输出清单统一在 `README.md` 维护，不在 `schedule_logic.md` 重复维护。
- 编码规范：仓库文本文件统一使用 UTF-8；发现乱码（mojibake）需先修复再执行脚本。

当 `src/`、`data/`、`reports/` 或治理文档发生结构/合同变更时，必须同次更新：
1. `schedule_logic.md`
2. `README.md`
3. 对应代码与 CSV 合同

## 10. 当前兼容性说明
- 本阶段已完成数量语义重构（模型与数据层）。
- 原链路执行入口迁移至 `src/original/`（根目录旧脚本保留为兼容转发入口）。
- 新增 daily 独立链路：
  - `data_daily -> planning_demo_daily.sqlite -> src/daily/generate_schedule.py -> reports/schedule_daily`
  - 不修改 `route_step_machine_types` 能力参数，增量来自资源扩容与日级分配。
