# 排产逻辑与业务规则

## 1. 求解主线
采用 demand-first 启发式：
1. 按 `priority asc, due_date asc, order_id asc` 处理订单。
2. 递归覆盖物料需求（库存/采购/上游工序）。
3. 工序排程采用 frePPLe 风格：先 backward，再 forward。
   - 终工序 `latest_end = due_date`
   - 前序工序 `latest_end = min(后继工序已排任务最早开始)`
4. 每道工序排程同时受 `latest_end` 与 `material_ready`（库存/采购可用时间）约束。
5. backward 不可行时允许 forward 兜底并记录问题定位信息。
6. 订单工序要求全量完成；不足则判失败，不缩量。
7. 每次排产结束后，将当次任务结果全量覆盖回写数据库 `schedule_tasks`（先清空再写入）。

## 2. 订单门槛与晚交归因
- 自动排产仅处理 `pending` 订单。
- `due_date` 统一解释为截止时间（deadline），不作为开始时间。
- 晚交归因采用加权惩罚：
  - `penalty = 0.6 * priority_rank + 0.4 * due_rank`
  - penalty 越小越优先保护，penalty 越大越可能承担晚交。

## 3. 资源与日历规则
### 3.1 资源链路
`employee -> workshop -> machine_type -> machine`

### 3.2 设备与员工约束
- 技能体系不参与匹配。
- 每个设备类型绑定一个车间，每名员工绑定一个车间。
- 同一设备类型设备默认归属同一车间。

### 3.3 设备日历
- 仅使用 `machine_calendars.csv` 维修/停机日历。
- 每台设备至少一条 `is_working=false` 记录。

### 3.4 员工日历
- 周历：`employee_weekly_calendars.csv`（含 `week_in_cycle`、`shift_code`）。
- 例外：`employee_calendars.csv`（请假/加班）。

### 3.5 槽位搜索边界
- 步长：30 分钟。
- 搜索边界由规划视窗控制（默认 90 天），不使用固定次数截断：
  - backward：`[max(material_ready, due_target - planning_horizon), due_target]`
  - forward：`[max(material_ready, due_target), max(material_ready, due_target) + planning_horizon]`

## 4. 采购与库存职责
- 采购下达基准时间：排产运行日次日 `08:00:00`。
- 采购可用时间：`purchase_ready_time = purchase_base_time + lead_time_days`。
- 库存职责边界：
  - `data/inventories.csv` 由导入脚本读取并全量覆盖写入数据库。
  - 排产脚本只读取数据库库存，不直接读取 CSV。

## 5. 工序数量语义
- `route_steps.output_qty_per_execution`：单次执行产出。
- `route_step_inputs.input_qty_per_execution`：单次执行输入。
- `route_step_machine_types.capacity_per_execution`：设备单次执行能力。
- `single`：`output_qty_per_execution=1`。
- `batch`：固定批时长 + 容量分批。
  - 单批时长固定 `duration_min_override`
  - 单批上限由 `capacity_per_execution` 决定
  - 余量批仍用固定批时长
- 每步 `material_ready` 是排程下界：物料未就绪时不得分配资源。

## 6. 数量转换规则
- 量纲或单次数量比例变化必须配置 `step_quantity_conversions.csv`。
- `batch` 工序若 `capacity_uom_code != output_uom_code`，必须通过转换表换算后再分批。
- 未配置转换记录的量纲跳变视为数据错误。

## 7. 评分规则（100分制）
- `overall_score = Σ(weighted_score)`。
- 当前子项与权重：
  - `feasibility_score`：20%
  - `on_time_rate`：35%
  - `lateness_score`：30%
  - `resource_efficiency`：10%（设备利用+设备均衡+员工利用）
  - `continuity`：5%

## 8. 库存试算与提交规则（2026-04）
- 订单级库存策略：先库存后缺口，库存按订单顺序扣减，不可重复利用。
- 两阶段语义：
  - 阶段A（试算）：需求展开只在本地库存快照上计算，不直接改全局库存与采购池。
  - 阶段B（提交）：仅当订单所有工序排产成功时，才一次性提交库存扣减与采购需求。
- 失败策略：订单失败时只保留失败诊断，库存/采购/任务/资源预占都不得污染后续订单。
- 成品库存净额：订单请求物料先做库存净额冲抵；净额为0时订单直接 planned，且不生成工序任务。
- 工序输入库存净额：所有输入物料先抵库存，剩余缺口才转上游工序或外采。
- `consumption_mode` 口径：
  - `fixed_per_execution`：`need = exec_count * input_qty_per_execution`
  - `proportional_to_output`：`need = planned_output_qty * input_qty_per_execution / output_qty_per_execution`
  - `packaging_per_pack`：`need = ceil(planned_output_qty / output_qty_per_execution) * input_qty_per_execution`
  - `carrier_transfer`：仅记录占用量（`carrier_reserved_qty`），不扣库存、不触发采购；无上游提供者时按失败处理（`carrier_provider_missing`）。
