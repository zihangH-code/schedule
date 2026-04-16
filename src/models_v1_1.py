from __future__ import annotations

"""
智能排产系统 - 初版数据建模（SQLAlchemy）
-------------------------------------------------
用途：
1. 定义初版排产所需的核心 ORM 模型。
2. 直接运行本文件时，自动在当前目录创建 SQLite 数据库和全部表。

一、建模目标
本模型用于支撑“订单 -> 工艺路线 -> 工序步骤 -> 设备能力 -> 排产任务”的基础排产链路，
重点是先把可用于初步排产的主数据、工艺结构和排产结果表达清楚，而不是一步到位覆盖所有
制造业复杂场景。

版本说明：
- 当前迭代版本命名统一采用 v1.x。
- 本文件对应版本为 v1.1。

二、当前建模项（核心实体）
1. Material（物料）
   - 表示原材料、半成品、成品等统一物料对象。
   - 支持订单目标物料、工序输出物料、工序输入物料等多种业务语义。

2. Inventory（库存）
   - 表示物料当前可用库存。
   - 初版采用“每个物料一条库存记录”的简化设计。

3. Machine（设备）
   - 表示可执行工序的设备资源。
   - 记录设备类型、状态等基础属性。

4. Order（订单）
   - 表示客户需求或生产需求。
   - 初版关注：优先级、截止日期、目标物料、需求数量、状态。
   - 订单需求物料当前被限制为“产品”类型。
   - 可显式绑定某条工艺路线，也可留空供后续排产算法自动选择。

5. ProcessRoute（工艺路线）
   - 表示某个目标物料的一条完整生产路线。

6. RouteStep（工艺步骤）
   - 表示工艺路线中的单个工序节点。
   - 记录工序名称、执行模式、产出物料、产出数量等。

7. RouteStepInput（工序输入）
   - 表示某个工序节点需要消耗哪些输入物料，以及消耗数量。
   - 用于表达装配、混料、投料等多输入场景。

8. RouteStepMachineType（工序-设备类型能力映射）
   - 表示某一步可由哪些设备类型执行。
   - 可记录设备上的时长覆盖值。

9. RouteStepDependency（工序依赖）
   - 显式表达工序间前后依赖关系，构成 DAG。
   - 适合表示分支、汇合、多路径等复杂工艺拓扑。

10. MaterialPurchase（物料采购规则）
    - 将“可采购”从物料主数据中拆分为独立行为建模。
    - 仅记录最关键参数：采购提前期（天）。
    - 仅允许 raw_material 配置采购规则。

11. ScheduleTask（排产任务）
    - 表示排产输出结果中的任务记录。
    - 关联订单、路线、步骤、设备、计划数量、计划开始结束时间、任务状态。

三、建模范围说明
本初版模型当前覆盖：
- 订单主数据
- 物料与库存
- 设备资源
- 工艺路线及 DAG 依赖
- 工序输入/输出
- 工序可执行设备集合
- 排产结果落表

本初版模型暂未细化：
- 节假日规则、跨工厂统一日历策略
- 工装/模具/人员等附加资源约束
- 批次、批号、追溯信息
- 换型时间、清洗时间、运输时间
- 仓位、多库存地点、多工厂
- 工序良率、报废率、副产品、联产品
- 更复杂的 ATP / MRP / APS 联动逻辑

四、当前实现约定
- 输出统一采用 logging，不再使用 tkinter 弹窗或 print。
- 当前模型不包含 created_at / updated_at 等审计时间字段。
- 当前已移除 Material / Machine / ProcessRoute 中的 is_active 字段。
- 当前默认数据库为本地 SQLite，方便在 PyCharm 中直接运行验证。
"""

from datetime import datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
import sys
from loguru import logger


# =========================
# 日志配置
# =========================
try:
    from sqlalchemy import (
        Boolean,
        CheckConstraint,
        DateTime,
        Enum as SAEnum,
        ForeignKey,
        Index,
        Integer,
        Numeric,
        String,
        Time,
        Text,
        UniqueConstraint,
        create_engine,
    )
    from sqlalchemy import inspect
    from sqlalchemy.orm import DeclarativeBase, Mapped, declarative_mixin, mapped_column, relationship, validates
except ImportError as exc:
    logger.error(
        "未检测到 SQLAlchemy。请在 PyCharm 的 Python Interpreter 中安装 sqlalchemy 后再运行。{}",
        exc,
    )
    raise SystemExit(1) from exc


# =========================
# 运行配置（可在 PyCharm 里直接修改）
# =========================
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "db" / "planning_demo.sqlite"
DROP_EXISTING_DB_WHEN_RUN = True


# =========================
# Base
# =========================
class Base(DeclarativeBase):
    """
    所有 ORM 模型的统一基类。

    作用：
    - 作为 SQLAlchemy Declarative 模式的根基类。
    - 后续所有表模型均继承自该类，以便统一纳入 Base.metadata 管理。
    - 在 create_database 中通过 Base.metadata.create_all(engine) 一次性建表。

    当前约定：
    - 本版模型不再通过 Mixin 注入 created_at / updated_at 字段。
    - 若后续需要审计字段，可在明确需求后单独补充。
    """

    pass


@declarative_mixin
class ReprMixin:
    """
    ORM 对象字符串展示混入类。

    作用：
    - 参照 test_sql.py 的写法，为模型统一提供更直观的 __repr__ 输出。
    - 打印对象或在日志中输出对象时，可自动展示当前模型的列字段和值。
    - 若字段值为 datetime，则格式化为 YYYY-MM-DD HH:MM:SS，便于阅读。

    说明：
    - 该 Mixin 仅影响对象展示，不影响数据库表结构。
    - 只遍历 column_attrs，不展开 relationship，避免输出过长或递归引用。
    """

    def __repr__(self) -> str:
        params: dict[str, object] = {}
        for column_attr in inspect(self).mapper.column_attrs:
            value = getattr(self, column_attr.key)
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            params[column_attr.key] = value

        items = " ".join(f"{key}={value!r}" for key, value in params.items())
        return f"<{self.__class__.__name__} {items}>"



# =========================
# Enums
# =========================
class MaterialType(str, Enum):
    """物料类型枚举。用于区分原材料、中间产品和最终产品。"""

    RAW_MATERIAL = "raw_material"
    AUXILIARY = "auxiliary"
    INTERMEDIATE_PRODUCT = "intermediate_product"
    PRODUCT = "product"


class OrderStatus(str, Enum):
    """订单状态枚举。用于描述订单从待处理到完成/取消的生命周期。"""

    PENDING = "pending"
    PLANNED = "planned"
    RELEASED = "released"
    DONE = "done"
    CANCELLED = "cancelled"


class MachineStatus(str, Enum):
    """设备状态枚举。用于表达设备当前是否空闲、占用或故障停机。"""

    IDLE = "idle"
    BUSY = "busy"
    DOWN = "down"


class EmployeeStatus(str, Enum):
    """员工状态枚举。"""

    IDLE = "idle"
    BUSY = "busy"
    OFF = "off"


class TaskStatus(str, Enum):
    """排产任务状态枚举。用于表达任务从待排到执行完成的过程状态。"""

    PENDING = "pending"
    PLANNED = "planned"
    RUNNING = "running"
    DONE = "done"
    CANCELLED = "cancelled"


class StepExecutionMode(str, Enum):
    """工序执行模式枚举。single=单件处理，batch=批量处理。"""

    SINGLE = "single"
    BATCH = "batch"


class MaterialFormType(str, Enum):
    """物料形态语义。"""

    STOCK_ITEM = "stock_item"
    WIP_PIECE = "wip_piece"
    PROCESS_CARRIER = "process_carrier"
    PACK_UNIT = "pack_unit"


class ConsumptionMode(str, Enum):
    """工序输入消耗模式。"""

    FIXED_PER_EXECUTION = "fixed_per_execution"
    PROPORTIONAL_TO_OUTPUT = "proportional_to_output"
    CARRIER_TRANSFER = "carrier_transfer"
    PACKAGING_PER_PACK = "packaging_per_pack"


# =========================
# Master Data
# =========================
class Material(Base, ReprMixin):
    """
    物料主数据表。

    建模项说明：
    - 本表是整个排产模型的物料中心。
    - 既可以表示原材料，也可以表示中间半成品、最终成品。
    - 订单需求、工序输出、工序输入均通过该表统一关联。

    关键字段：
    - code: 物料编码，业务唯一。
    - name: 物料名称。
    - material_type: 物料类型，区分原材料 / 中间产品 / 最终产品。
    - uom: 计量单位代码，例如 pcs / kg / box。
    - material_form_type: 物料形态语义（库存件、在制件、工艺载具、包装单元）。
    """

    __tablename__ = "materials"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    material_type: Mapped[MaterialType] = mapped_column(
        SAEnum(MaterialType, name="material_type"),
        nullable=False,
        default=MaterialType.RAW_MATERIAL,
    )
    uom: Mapped[str] = mapped_column(String(32), nullable=False, default="pcs")
    material_form_type: Mapped[MaterialFormType] = mapped_column(
        SAEnum(MaterialFormType, name="material_form_type"),
        nullable=False,
        default=MaterialFormType.STOCK_ITEM,
    )

    # 一个物料在初版中仅关联一条库存记录（简化设计）。
    inventory: Mapped["Inventory"] = relationship(
        back_populates="material",
        uselist=False,
        cascade="all, delete-orphan",
    )

    # 物料采购规则（可选）。仅原材料允许存在采购规则。
    purchase_policy: Mapped["MaterialPurchase | None"] = relationship(
        back_populates="material",
        uselist=False,
        cascade="all, delete-orphan",
    )

    # 被哪些订单作为目标需求物料引用。
    requested_by_orders: Mapped[list["Order"]] = relationship(
        back_populates="requested_material",
        foreign_keys="Order.requested_material_id",
    )

    # 被哪些工艺路线作为“最终产出目标物料”引用。
    route_targets: Mapped[list["ProcessRoute"]] = relationship(
        back_populates="target_material",
        foreign_keys="ProcessRoute.target_material_id",
    )

    # 被哪些工序步骤作为输出物料引用。
    step_outputs: Mapped[list["RouteStep"]] = relationship(
        back_populates="output_material",
        foreign_keys="RouteStep.output_material_id",
    )

    # 被哪些工序输入项作为消耗物料引用。
    step_inputs: Mapped[list["RouteStepInput"]] = relationship(
        back_populates="material",
        cascade="all, delete-orphan",
    )

class MaterialPurchase(Base, ReprMixin):
    """
    物料采购行为表（独立建模）。

    建模意图：
    - 将采购能力从 Material 主数据中拆分，单独表达“可采购行为”。
    - 当前仅保留最关键参数：采购提前期（天）。

    业务规则：
    - 仅 raw_material / auxiliary 可配置采购规则。
    - 每个物料最多一条采购规则。
    """

    __tablename__ = "material_purchases"
    __table_args__ = (
        UniqueConstraint("material_id", name="uq_material_purchase_material"),
        CheckConstraint("purchase_lead_time_days > 0", name="ck_material_purchase_lead_days_positive"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    material_id: Mapped[int] = mapped_column(
        ForeignKey("materials.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    purchase_lead_time_days: Mapped[int] = mapped_column(Integer, nullable=False)

    material: Mapped["Material"] = relationship(back_populates="purchase_policy")

    @validates("material")
    def validate_material(self, key: str, value: Material) -> Material:
        if value.material_type not in {MaterialType.RAW_MATERIAL, MaterialType.AUXILIARY}:
            raise ValueError("仅 raw_material/auxiliary 可配置采购规则")
        return value


class Inventory(Base, ReprMixin):
    """
    库存表。

    建模项说明：
    - 用于记录物料当前可用库存数量。
    - 初版建模按“物料维度”聚合库存，不区分仓库、库位、批次。
    - 适合用于初步排产前的可用量判断。

    约束说明：
    - material_id 唯一：每个物料仅有一条库存记录。
    - available_qty >= 0：库存可用量不能为负数。
    """

    __tablename__ = "inventories"
    __table_args__ = (
        UniqueConstraint("material_id", name="uq_inventory_material"),
        CheckConstraint("available_qty >= 0", name="ck_inventory_available_qty_non_negative"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    material_id: Mapped[int] = mapped_column(
        ForeignKey("materials.id", ondelete="CASCADE"),
        nullable=False,
    )
    available_qty: Mapped[Decimal] = mapped_column(
        Numeric(18, 4),
        nullable=False,
        default=0,
    )

    material: Mapped["Material"] = relationship(back_populates="inventory")


class MachineType(Base, ReprMixin):
    """
    设备类型主数据表。

    说明：
    - 将“设备类型”从机器表字符串字段中拆分为独立实体。
    - 工序能力映射改为“工序 -> 设备类型”，具体排产时再落到该类型下的某台设备。
    """

    __tablename__ = "machine_types"
    __table_args__ = (
        UniqueConstraint("code", name="uq_machine_type_code"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    workshop_id: Mapped[int] = mapped_column(
        ForeignKey("workshops.id"),
        nullable=False,
        index=True,
    )
    note: Mapped[str | None] = mapped_column(Text)

    workshop: Mapped["Workshop"] = relationship(back_populates="machine_types")
    machines: Mapped[list["Machine"]] = relationship(back_populates="machine_type")
    step_capabilities: Mapped[list["RouteStepMachineType"]] = relationship(
        back_populates="machine_type",
        cascade="all, delete-orphan",
    )


class Machine(Base, ReprMixin):
    """
    设备主数据表。

    建模项说明：
    - 表示生产资源中的设备实体。
    - 设备可通过 RouteStepMachineType 与工序步骤建立“可执行能力”映射。
    - 排产任务最终会落到具体设备上。

    关键字段：
    - code: 设备编码，业务唯一。
    - machine_type_id: 设备类型外键。
    - status: 当前设备状态。
    """

    __tablename__ = "machines"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    machine_type_id: Mapped[int] = mapped_column(
        ForeignKey("machine_types.id"),
        nullable=False,
        index=True,
    )
    status: Mapped[MachineStatus] = mapped_column(
        SAEnum(MachineStatus, name="machine_status"),
        default=MachineStatus.IDLE,
        nullable=False,
    )

    machine_type: Mapped["MachineType"] = relationship(back_populates="machines")

    # 已经落到该设备上的排产任务集合。
    tasks: Mapped[list["ScheduleTask"]] = relationship(back_populates="machine")
    calendars: Mapped[list["MachineCalendar"]] = relationship(
        back_populates="machine",
        cascade="all, delete-orphan",
    )


# =========================
# Machine Calendar
# =========================
class MachineCalendar(Base, ReprMixin):
    """
    设备日历表。
    建模说明：
    - 表示设备在某一时间窗口的可用或不可用状态。
    - 可用于表达班次、停机、维护等时间约束。
    """

    __tablename__ = "machine_calendars"
    __table_args__ = (
        Index(
            "ix_machine_calendar_machine_time",
            "machine_id",
            "start_time",
            "end_time",
        ),
        CheckConstraint("end_time > start_time", name="ck_machine_calendar_end_after_start"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    machine_id: Mapped[int] = mapped_column(
        ForeignKey("machines.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    start_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    is_working: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    note: Mapped[str | None] = mapped_column(Text)

    machine: Mapped["Machine"] = relationship(back_populates="calendars")


# =========================
# Workshop / Employee
# =========================
class Workshop(Base, ReprMixin):
    """车间主数据表。"""

    __tablename__ = "workshops"
    __table_args__ = (
        UniqueConstraint("code", name="uq_workshop_code"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    note: Mapped[str | None] = mapped_column(Text)

    machine_types: Mapped[list["MachineType"]] = relationship(
        back_populates="workshop",
        cascade="all, delete-orphan",
    )
    employees: Mapped[list["Employee"]] = relationship(
        back_populates="workshop",
        cascade="all, delete-orphan",
    )


class Employee(Base, ReprMixin):
    """员工主数据表。"""

    __tablename__ = "employees"
    __table_args__ = (
        UniqueConstraint("code", name="uq_employee_code"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    workshop_id: Mapped[int] = mapped_column(
        ForeignKey("workshops.id"),
        nullable=False,
        index=True,
    )
    status: Mapped[EmployeeStatus] = mapped_column(
        SAEnum(EmployeeStatus, name="employee_status"),
        default=EmployeeStatus.IDLE,
        nullable=False,
    )
    note: Mapped[str | None] = mapped_column(Text)

    workshop: Mapped["Workshop"] = relationship(back_populates="employees")
    calendars: Mapped[list["EmployeeCalendar"]] = relationship(
        back_populates="employee",
        cascade="all, delete-orphan",
    )
    weekly_calendars: Mapped[list["EmployeeWeeklyCalendar"]] = relationship(
        back_populates="employee",
        cascade="all, delete-orphan",
    )
    tasks: Mapped[list["ScheduleTask"]] = relationship(back_populates="employee")


class EmployeeCalendar(Base, ReprMixin):
    """员工例外日历表。"""

    __tablename__ = "employee_calendars"
    __table_args__ = (
        Index(
            "ix_employee_calendar_employee_time",
            "employee_id",
            "start_time",
            "end_time",
        ),
        CheckConstraint("end_time > start_time", name="ck_employee_calendar_end_after_start"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    start_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    is_working: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    note: Mapped[str | None] = mapped_column(Text)

    employee: Mapped["Employee"] = relationship(back_populates="calendars")


class EmployeeWeeklyCalendar(Base, ReprMixin):
    """员工周模板日历（两周周期 + 按周几）。"""

    __tablename__ = "employee_weekly_calendars"
    __table_args__ = (
        Index(
            "ix_employee_weekly_calendar_employee_weekday",
            "employee_id",
            "week_in_cycle",
            "weekday",
            "start_time",
            "end_time",
        ),
        CheckConstraint("week_in_cycle >= 1 AND week_in_cycle <= 2", name="ck_employee_weekly_calendar_week_in_cycle"),
        CheckConstraint("weekday >= 1 AND weekday <= 7", name="ck_employee_weekly_calendar_weekday"),
        CheckConstraint("end_time > start_time", name="ck_employee_weekly_calendar_end_after_start"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    employee_id: Mapped[int] = mapped_column(
        ForeignKey("employees.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    week_in_cycle: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    weekday: Mapped[int] = mapped_column(Integer, nullable=False)
    start_time: Mapped[time] = mapped_column(Time, nullable=False)
    end_time: Mapped[time] = mapped_column(Time, nullable=False)
    shift_code: Mapped[str] = mapped_column(String(16), nullable=False, default="day")
    is_working: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    note: Mapped[str | None] = mapped_column(Text)

    employee: Mapped["Employee"] = relationship(back_populates="weekly_calendars")


# =========================
# Order
# =========================
class Order(Base, ReprMixin):
    """
    订单表。

    建模项说明：
    - 表示排产的外部需求输入。
    - 初版订单只关心：优先级、交期、目标物料、需求数量。
    - 不直接展开为工序任务，而是通过 route_id 或自动选路逻辑映射到工艺路线。

    关键约束：
    - requested_qty > 0：需求数量必须为正。
    - 订单需求物料必须为 MaterialType.PRODUCT。
    - (priority, due_date) 建索引：便于后续按优先级和交期排序。
    """

    __tablename__ = "orders"
    __table_args__ = (
        Index("ix_orders_priority_due_date", "priority", "due_date"),
        CheckConstraint("requested_qty > 0", name="ck_order_requested_qty_positive"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)

    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    due_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # 订单只描述最终想要什么物料、要多少。
    requested_material_id: Mapped[int] = mapped_column(
        ForeignKey("materials.id"),
        nullable=False,
    )
    requested_qty: Mapped[Decimal] = mapped_column(
        Numeric(18, 4),
        nullable=False,
    )

    # 初版允许订单显式绑定路线；若为空，可由后续排产逻辑自动选路线。
    route_id: Mapped[int | None] = mapped_column(
        ForeignKey("process_routes.id"),
        nullable=True,
    )

    status: Mapped[OrderStatus] = mapped_column(
        SAEnum(OrderStatus, name="order_status"),
        default=OrderStatus.PENDING,
        nullable=False,
    )
    note: Mapped[str | None] = mapped_column(Text)

    requested_material: Mapped["Material"] = relationship(
        back_populates="requested_by_orders",
        foreign_keys=[requested_material_id],
    )
    route: Mapped["ProcessRoute | None"] = relationship(
        back_populates="orders",
        foreign_keys=[route_id],
    )

    @validates("requested_material")
    def validate_requested_material(self, key: str, material: "Material") -> "Material":
        """
        校验订单需求物料。

        说明：
        - 当前版本要求订单只能需求“最终产品”。
        - 由于该约束涉及跨表字段，SQLite 层面不适合直接用 CheckConstraint 表达，
          因此先在 ORM 层做轻量校验。
        """
        if material.material_type != MaterialType.PRODUCT:
            raise ValueError(
                f"订单需求物料必须是产品类型，当前物料 {material.code} 的类型为 {material.material_type.value}"
            )
        return material

    # 订单被排开后，对应生成的任务集合。
    tasks: Mapped[list["ScheduleTask"]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
    )


# =========================
# Process Route (DAG)
# =========================
class ProcessRoute(Base, ReprMixin):
    """
    工艺路线表。

    建模项说明：
    - 一条工艺路线描述“如何生产出某个目标物料”。
    - 路线下包含多个 RouteStep 节点，并可通过 RouteStepDependency 形成 DAG。

    设计原因：
    - 初版排产必须先有“路线”概念，订单才能映射到可执行工艺。
    - 当前路线状态不再通过 is_active 字段表达。
    """

    __tablename__ = "process_routes"
    __table_args__ = (
        UniqueConstraint("code", name="uq_process_route_code"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)

    # 一条路线最终产出什么物料。
    target_material_id: Mapped[int] = mapped_column(
        ForeignKey("materials.id"),
        nullable=False,
    )

    note: Mapped[str | None] = mapped_column(Text)

    target_material: Mapped["Material"] = relationship(
        back_populates="route_targets",
        foreign_keys=[target_material_id],
    )

    # 路线包含的所有工序步骤。
    steps: Mapped[list["RouteStep"]] = relationship(
        back_populates="route",
        cascade="all, delete-orphan",
        order_by="RouteStep.id",
    )

    # 显式绑定该路线的订单集合。
    orders: Mapped[list["Order"]] = relationship(back_populates="route")


class RouteStep(Base, ReprMixin):
    """
    工艺步骤表。

    建模项说明：
    - 表示工艺路线中的单个工序节点。
    - 一个节点有自己的输入物料、输出物料、执行模式、候选设备。
    - 多个步骤再通过依赖关系构成完整工艺 DAG。

    字段含义：
    - code: 工序节点编码，在同一路线内唯一。
    - process_name: 工艺名称，例如“切割”“烧结”“装配”。
    - display_order: 用于展示顺序，不代表真实拓扑顺序。
    - execution_mode: 工序执行模式（单件/批量）。
    - output_material_id / output_qty_per_execution: 本步骤单次执行的产出定义。
    - output_uom_code: 本步骤产出量纲代码。
    """

    __tablename__ = "route_steps"
    __table_args__ = (
        UniqueConstraint("route_id", "code", name="uq_route_step_code"),
        CheckConstraint("output_qty_per_execution > 0", name="ck_route_step_output_qty_positive"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)

    route_id: Mapped[int] = mapped_column(
        ForeignKey("process_routes.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    process_name: Mapped[str] = mapped_column(String(128), nullable=False)

    # 仅用于展示，不代表真实拓扑顺序。
    display_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    execution_mode: Mapped[StepExecutionMode] = mapped_column(
        SAEnum(StepExecutionMode, name="step_execution_mode"),
        nullable=False,
        default=StepExecutionMode.SINGLE,
    )

    output_material_id: Mapped[int] = mapped_column(
        ForeignKey("materials.id"),
        nullable=False,
    )
    output_qty_per_execution: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=1)
    output_uom_code: Mapped[str] = mapped_column(String(32), nullable=False, default="pcs")

    route: Mapped["ProcessRoute"] = relationship(back_populates="steps")
    output_material: Mapped["Material"] = relationship(
        back_populates="step_outputs",
        foreign_keys=[output_material_id],
    )

    # 当前工序消耗的输入物料集合。
    inputs: Mapped[list["RouteStepInput"]] = relationship(
        back_populates="step",
        cascade="all, delete-orphan",
    )

    # 当前工序可由哪些设备类型执行。
    candidate_machine_types: Mapped[list["RouteStepMachineType"]] = relationship(
        back_populates="step",
        cascade="all, delete-orphan",
    )

    # 前驱依赖：哪些步骤必须先于本步骤完成。
    predecessor_links: Mapped[list["RouteStepDependency"]] = relationship(
        "RouteStepDependency",
        foreign_keys="RouteStepDependency.successor_step_id",
        back_populates="successor",
        cascade="all, delete-orphan",
    )

    # 后继依赖：本步骤完成后可触发哪些步骤。
    successor_links: Mapped[list["RouteStepDependency"]] = relationship(
        "RouteStepDependency",
        foreign_keys="RouteStepDependency.predecessor_step_id",
        back_populates="predecessor",
        cascade="all, delete-orphan",
    )

    # 由该工序步骤展开而来的排产任务。
    tasks: Mapped[list["ScheduleTask"]] = relationship(back_populates="step")
    quantity_conversions: Mapped[list["StepQuantityConversion"]] = relationship(
        "StepQuantityConversion",
        back_populates="step",
        cascade="all, delete-orphan",
    )


class RouteStepInput(Base, ReprMixin):
    """
    工序输入表。

    建模项说明：
    - 表达某一步需要消耗哪些输入物料。
    - 用于支持多输入工艺，例如：B + C --(装配)--> D。
    - 同一步骤中同一种物料只允许出现一次。

    示例：
    若某装配步骤需要：
    - 1 个外壳
    - 2 个螺丝
    则该步骤会对应两条 RouteStepInput 记录。
    """

    __tablename__ = "route_step_inputs"
    __table_args__ = (
        UniqueConstraint("step_id", "material_id", name="uq_route_step_input"),
        CheckConstraint("input_qty_per_execution > 0", name="ck_route_step_input_qty_positive"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    step_id: Mapped[int] = mapped_column(
        ForeignKey("route_steps.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    material_id: Mapped[int] = mapped_column(
        ForeignKey("materials.id"),
        nullable=False,
    )
    input_qty_per_execution: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    input_uom_code: Mapped[str] = mapped_column(String(32), nullable=False, default="pcs")
    consumption_mode: Mapped[ConsumptionMode] = mapped_column(
        SAEnum(ConsumptionMode, name="consumption_mode"),
        nullable=False,
        default=ConsumptionMode.PROPORTIONAL_TO_OUTPUT,
    )

    step: Mapped["RouteStep"] = relationship(back_populates="inputs")
    material: Mapped["Material"] = relationship(back_populates="step_inputs")


class RouteStepMachineType(Base, ReprMixin):
    """
    工序-设备类型能力映射表。

    建模项说明：
    - 表达某一步可由哪些设备类型执行。
    - 这是“工艺”和“资源”之间的桥接表。
    - 支持对某台设备设置时长覆盖值，以表达设备效率差异。

    设计意图：
    - 初版排产先绑定设备类型，再在该类型下选择具体设备与员工。
    - capacity_per_execution 表示该设备类型下单台设备单次执行可处理的能力上限。
    """

    __tablename__ = "route_step_machine_types"
    __table_args__ = (
        UniqueConstraint("step_id", "machine_type_id", name="uq_route_step_machine_type"),
        CheckConstraint(
            "duration_min_override > 0",
            name="ck_route_step_machine_type_duration_positive",
        ),
        CheckConstraint(
            "capacity_per_execution > 0",
            name="ck_route_step_machine_type_capacity_positive",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    step_id: Mapped[int] = mapped_column(
        ForeignKey("route_steps.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    machine_type_id: Mapped[int] = mapped_column(
        ForeignKey("machine_types.id"),
        nullable=False,
        index=True,
    )

    # 设备类型级工时（single 为单件时长，batch 为单批时长）。
    duration_min_override: Mapped[int] = mapped_column(Integer, nullable=False)
    capacity_per_execution: Mapped[Decimal] = mapped_column(
        Numeric(18, 4),
        nullable=False,
        default=1,
    )
    capacity_uom_code: Mapped[str] = mapped_column(String(32), nullable=False, default="pcs")

    step: Mapped["RouteStep"] = relationship(back_populates="candidate_machine_types")
    machine_type: Mapped["MachineType"] = relationship(back_populates="step_capabilities")


class RouteStepDependency(Base, ReprMixin):
    """
    工序依赖关系表。

    建模项说明：
    - 显式表达工序 DAG 中的前驱/后继关系。
    - 不只依赖物料名称来推导顺序，而是直接存储拓扑边。

    为什么需要这张表：
    - 能表示分支工艺。
    - 能表示多路汇合。
    - 能表示“同一种物料，但来源路径不同”的复杂拓扑。
    - 对后续拓扑排序、路径分析、关键路径分析都更友好。
    """

    __tablename__ = "route_step_dependencies"
    __table_args__ = (
        UniqueConstraint(
            "predecessor_step_id",
            "successor_step_id",
            name="uq_route_step_dependency",
        ),
        CheckConstraint(
            "predecessor_step_id <> successor_step_id",
            name="ck_route_step_dependency_not_self",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)

    predecessor_step_id: Mapped[int] = mapped_column(
        ForeignKey("route_steps.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    successor_step_id: Mapped[int] = mapped_column(
        ForeignKey("route_steps.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    predecessor: Mapped["RouteStep"] = relationship(
        "RouteStep",
        foreign_keys=[predecessor_step_id],
        back_populates="successor_links",
    )
    successor: Mapped["RouteStep"] = relationship(
        "RouteStep",
        foreign_keys=[successor_step_id],
        back_populates="predecessor_links",
    )


class StepQuantityConversion(Base, ReprMixin):
    """步骤内数量换算规则。"""

    __tablename__ = "step_quantity_conversions"
    __table_args__ = (
        UniqueConstraint("step_id", "from_uom_code", "to_uom_code", name="uq_step_qty_conversion"),
        CheckConstraint("from_qty > 0", name="ck_step_qty_conversion_from_positive"),
        CheckConstraint("to_qty > 0", name="ck_step_qty_conversion_to_positive"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    step_id: Mapped[int] = mapped_column(
        ForeignKey("route_steps.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    from_uom_code: Mapped[str] = mapped_column(String(32), nullable=False)
    from_qty: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    to_uom_code: Mapped[str] = mapped_column(String(32), nullable=False)
    to_qty: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    conversion_type: Mapped[str] = mapped_column(String(32), nullable=False, default="ratio_transform")

    step: Mapped["RouteStep"] = relationship("RouteStep", back_populates="quantity_conversions")


# =========================
# Scheduling Result
# =========================
class ScheduleTask(Base, ReprMixin):
    """
    排产任务结果表。

    建模项说明：
    - 表示排产算法最终输出的任务明细。
    - 一个订单在选定路线后，通常会展开成多个工序任务。
    - 每条任务记录对应“某订单的某一步，在某设备上，于某时间段，生产多少数量”。

    关键字段：
    - order_id: 来源订单。
    - route_id: 采用的工艺路线。
    - step_id: 对应的工序步骤。
    - machine_id: 分配到的设备（允许为空，表示尚未分配）。
    - employee_id: 分配到的员工（允许为空，表示尚未分配）。
    - planned_qty: 计划数量。
    - planned_start / planned_end: 计划时间窗口。
    - status: 任务状态。
    """

    __tablename__ = "schedule_tasks"
    __table_args__ = (
        Index("ix_schedule_tasks_order_id", "order_id"),
        Index("ix_schedule_tasks_machine_time", "machine_id", "planned_start", "planned_end"),
        Index("ix_schedule_tasks_execution_block_id", "execution_block_id"),
        CheckConstraint("planned_qty > 0", name="ck_schedule_task_planned_qty_positive"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)

    order_id: Mapped[int] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"),
        nullable=False,
    )
    route_id: Mapped[int] = mapped_column(
        ForeignKey("process_routes.id"),
        nullable=False,
    )
    step_id: Mapped[int] = mapped_column(
        ForeignKey("route_steps.id"),
        nullable=False,
    )

    machine_id: Mapped[int | None] = mapped_column(
        ForeignKey("machines.id"),
        nullable=True,
    )
    employee_id: Mapped[int | None] = mapped_column(
        ForeignKey("employees.id"),
        nullable=True,
    )
    execution_block_id: Mapped[int | None] = mapped_column(
        ForeignKey("schedule_execution_blocks.id"),
        nullable=True,
    )

    planned_qty: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    planned_start: Mapped[datetime | None] = mapped_column(DateTime)
    planned_end: Mapped[datetime | None] = mapped_column(DateTime)

    status: Mapped[TaskStatus] = mapped_column(
        SAEnum(TaskStatus, name="task_status"),
        default=TaskStatus.PENDING,
        nullable=False,
    )

    order: Mapped["Order"] = relationship(back_populates="tasks")
    step: Mapped["RouteStep"] = relationship(back_populates="tasks")
    machine: Mapped["Machine | None"] = relationship(back_populates="tasks")
    employee: Mapped["Employee | None"] = relationship(back_populates="tasks")
    route: Mapped["ProcessRoute"] = relationship()
    execution_block: Mapped["ScheduleExecutionBlock | None"] = relationship(back_populates="tasks")


class ScheduleExecutionBlock(Base, ReprMixin):
    """
    排产执行块。

    说明：
    - 表示多个任务在执行层面的合并结果。
    - `batch` 同订单合并、`single` 跨订单合并均落到该表。
    """

    __tablename__ = "schedule_execution_blocks"
    __table_args__ = (
        UniqueConstraint("block_code", name="uq_schedule_execution_block_code"),
        Index("ix_schedule_execution_blocks_step_time", "step_id", "planned_start", "planned_end"),
        CheckConstraint("total_qty > 0", name="ck_schedule_execution_block_total_qty_positive"),
        CheckConstraint("source_task_count >= 1", name="ck_schedule_execution_block_source_task_count"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    block_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    merge_type: Mapped[str] = mapped_column(String(64), nullable=False, default="none")

    route_id: Mapped[int] = mapped_column(ForeignKey("process_routes.id"), nullable=False)
    step_id: Mapped[int] = mapped_column(ForeignKey("route_steps.id"), nullable=False)
    machine_id: Mapped[int | None] = mapped_column(ForeignKey("machines.id"), nullable=True)
    employee_id: Mapped[int | None] = mapped_column(ForeignKey("employees.id"), nullable=True)

    total_qty: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    planned_start: Mapped[datetime | None] = mapped_column(DateTime)
    planned_end: Mapped[datetime | None] = mapped_column(DateTime)
    duration_min: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source_task_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    note: Mapped[str | None] = mapped_column(Text)

    route: Mapped["ProcessRoute"] = relationship()
    step: Mapped["RouteStep"] = relationship()
    machine: Mapped["Machine | None"] = relationship()
    employee: Mapped["Employee | None"] = relationship()
    tasks: Mapped[list["ScheduleTask"]] = relationship(back_populates="execution_block")


# =========================
# Helper Functions
# =========================
def get_engine(db_path: str | Path | None = None):
    """
    创建并返回 SQLAlchemy Engine。

    参数：
    - db_path: SQLite 数据库文件路径；若为空，则使用默认路径 DEFAULT_DB_PATH。

    返回：
    - SQLAlchemy Engine 对象。

    说明：
    - 会自动创建数据库文件所在目录。
    - 当前使用 SQLite 作为初版演示数据库，便于在 PyCharm 中直接运行。
    """
    target = Path(db_path) if db_path else DEFAULT_DB_PATH
    target = target.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{target}", echo=False, future=True)


def create_database(db_path: str | Path | None = None, drop_existing: bool = False):
    """
    创建数据库文件并初始化全部表结构。

    参数：
    - db_path: 数据库文件路径；为空则使用默认路径。
    - drop_existing: 是否在建库前删除已存在的同名数据库文件。

    返回：
    - (engine, target)
      - engine: SQLAlchemy Engine 对象。
      - target: 实际数据库文件路径。

    使用场景：
    - 初始化本地开发环境。
    - 每次重新生成干净的测试数据库。

    注意：
    - 若 drop_existing=True 且目标文件已存在，会直接删除原数据库文件。
    - 该函数只负责建表，不负责灌入测试数据。
    - 过程信息统一通过 logging 输出。
    """
    target = Path(db_path) if db_path else DEFAULT_DB_PATH
    target = target.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    if drop_existing and target.exists():
        logger.info("检测到旧数据库文件，准备删除：{}", target)
        target.unlink()

    engine = get_engine(target)
    Base.metadata.create_all(engine)
    logger.info("数据库表结构创建完成（v1.1）：{}", target)
    return engine, target


# =========================
# Main
# =========================
def main() -> None:
    """
    程序主入口。

    执行流程：
    1. 根据运行配置创建数据库。
    2. 通过日志输出建模完成信息。
    3. 若过程中出错，则统一通过日志记录异常。

    说明：
    - 直接在 PyCharm 中运行本文件时，会执行该函数。
    - 适合做“先把表建出来”的初始动作验证。
    """
    try:
        _, db_path = create_database(
            db_path=DEFAULT_DB_PATH,
            drop_existing=DROP_EXISTING_DB_WHEN_RUN,
        )
        logger.info(
            "数据库建模完成（v1.1）。数据库文件：{}；已创建表：materials / material_purchases / inventories / "
            "workshops / machine_types / machines / machine_calendars / "
            "employees / employee_calendars / employee_weekly_calendars / "
            "process_routes / route_steps / route_step_inputs / route_step_machine_types / "
            "route_step_dependencies / step_quantity_conversions / orders / schedule_tasks",
            db_path,
        )
    except Exception:
        logger.exception("建模失败。")


if __name__ == "__main__":
    main()
