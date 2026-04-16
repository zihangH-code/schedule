from __future__ import annotations
import argparse
import copy
import math, re, sqlite3
import sys
from collections import defaultdict, Counter, deque
from datetime import datetime, date, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from html import escape
from loguru import logger
ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / 'db' / 'planning_demo.sqlite'
OUT = ROOT / 'reports' / 'schedule'
LOG_DIR = ROOT / 'logs'
PENDING = {'pending'}
D4 = Decimal('0.0001')
PRIORITY_WEIGHT = 0.6
DUE_WEIGHT = 0.4
PLANNING_HORIZON_DAYS = 90
VERBOSE = False

def q(v: Decimal) -> Decimal: return v.quantize(D4, rounding=ROUND_HALF_UP)
def dec(v: object) -> Decimal: return q(Decimal(str(v)))
def pdt(v: object) -> datetime:
    if isinstance(v, datetime): return v
    s = str(v or '').strip()
    for f in ('%Y-%m-%d %H:%M:%S.%f','%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M','%Y-%m-%d'):
        try: return datetime.strptime(s,f)
        except ValueError: pass
    return datetime.fromisoformat(s)
def ptm(v: object) -> time:
    s=str(v or '').strip()
    for f in ('%H:%M:%S.%f','%H:%M:%S','%H:%M'):
        try: return datetime.strptime(s,f).time()
        except ValueError: pass
    raise ValueError(s)
def fdt(v: datetime|None) -> str: return '' if v is None else v.strftime('%Y-%m-%d %H:%M:%S')
def ov(a0,a1,b0,b1): return a0 < b1 and b0 < a1
def ceildiv(a:Decimal,b:Decimal)->int: return max(1,int(math.ceil(float(a/max(b,Decimal('0.0001'))))))
def clamp(x:float)->float: return min(1.0,max(0.0,x))

def _setup_logging(verbose: bool = False) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.remove()
    console_fmt = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level:<7}</level> | {message}"
    file_fmt = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<7} | {message}"
    logger.add(
        sys.stdout,
        level='INFO',
        format=console_fmt,
        colorize=True,
        enqueue=True,
    )
    logger.add(
        str(LOG_DIR / '{time:YYYY-MM-DD}.log'),
        level='DEBUG' if verbose else 'INFO',
        format=file_fmt,
        encoding='utf-8',
        rotation='00:00',
        retention='14 days',
        enqueue=True,
    )
    return LOG_DIR

def _progress_write(msg: str, level: str = 'info'):
    if level == 'debug' and not VERBOSE:
        return
    fn = getattr(logger, str(level).lower(), logger.info)
    fn(msg)

def _fmt_dt_short(v: datetime | None) -> str:
    return '' if v is None else v.strftime('%m-%d %H:%M')



def _attr(v: object) -> str:
    return escape(str(v)).replace('"', "&quot;")

def read_db():
    con=sqlite3.connect(str(DB)); con.row_factory=sqlite3.Row
    qy=lambda s: con.execute(s).fetchall()
    ws_info={int(r['id']):{'code':r['code'],'name':r['name']} for r in qy('select id,code,name from workshops')}
    mats={int(r['id']):{'code':r['code'],'type':str(r['material_type']).split('.')[-1].lower(),'uom':str(r['uom']).lower()} for r in qy('select id,code,material_type,uom from materials')}
    purch={i:False for i in mats}; lead={i:0 for i in mats}
    for r in qy('select material_id,purchase_lead_time_days from material_purchases'): purch[int(r['material_id'])]=True; lead[int(r['material_id'])]=int(r['purchase_lead_time_days'])
    inv={int(r['material_id']):dec(r['available_qty']) for r in qy('select material_id,available_qty from inventories')}
    for i in mats: inv.setdefault(i,Decimal('0'))
    routes={int(r['id']):{'id':int(r['id']),'code':r['code'],'name':r['name'],'target':int(r['target_material_id'])} for r in qy('select id,code,name,target_material_id from process_routes')}
    route_id_by_code={r['code']:rid for rid,r in routes.items()}
    r_by_target=defaultdict(list)
    for rid,r in routes.items(): r_by_target[r['target']].append(rid)
    orders=[{'id':int(r['id']),'code':r['code'],'pri':int(r['priority']),'due':pdt(r['due_date']),'mat':int(r['requested_material_id']),'qty':dec(r['requested_qty']),'route':(int(r['route_id']) if r['route_id'] is not None else None),'status':str(r['status']).split('.')[-1].lower()} for r in qy('select id,code,priority,due_date,requested_material_id,requested_qty,route_id,status from orders order by priority,due_date,id')]
    orders=[o for o in orders if o['status'] in PENDING]
    steps={int(r['id']):{'route':int(r['route_id']),'code':r['code'],'name':r['name'],'mode':str(r['execution_mode']).split('.')[-1].lower(),'out_mat':int(r['output_material_id']),'out_qty':dec(r['output_qty_per_execution']),'out_uom':str(r['output_uom_code']).lower()} for r in qy('select id,route_id,code,name,execution_mode,output_material_id,output_qty_per_execution,output_uom_code from route_steps order by route_id,display_order,id')}
    s_by_route=defaultdict(list)
    for sid,s in steps.items(): s_by_route[s['route']].append(sid)
    pred=defaultdict(list); edges=defaultdict(list)
    for r in qy('select predecessor_step_id,successor_step_id from route_step_dependencies'):
        a,b=int(r['predecessor_step_id']),int(r['successor_step_id']); pred[b].append(a); edges[steps[a]['route']].append((a,b))
    topo={}
    for rid,sids in s_by_route.items():
        indeg={s:0 for s in sids}; suc=defaultdict(list)
        for a,b in edges[rid]: suc[a].append(b); indeg[b]+=1
        dq=deque(sorted([s for s in sids if indeg[s]==0])); out=[]
        while dq:
            x=dq.popleft(); out.append(x)
            for y in suc.get(x,[]): indeg[y]-=1; (dq.append(y) if indeg[y]==0 else None)
        if len(out)!=len(sids): raise ValueError(f'cycle route {rid}')
        topo[rid]=out
    inputs=defaultdict(list)
    for r in qy('select step_id,material_id,input_qty_per_execution,input_uom_code,consumption_mode from route_step_inputs'):
        inputs[int(r['step_id'])].append({'mat':int(r['material_id']),'qty':dec(r['input_qty_per_execution']),'uom':str(r['input_uom_code']).lower(),'mode':str(r['consumption_mode']).split('.')[-1]})
    cands=defaultdict(list)
    for r in qy("select rsmt.step_id,rsmt.machine_type_id,mt.code mcode,rsmt.duration_min_override d,rsmt.capacity_per_execution c,rsmt.capacity_uom_code cu from route_step_machine_types rsmt join machine_types mt on mt.id=rsmt.machine_type_id"):
        cands[int(r['step_id'])].append({'mt':int(r['machine_type_id']),'mt_code':r['mcode'],'dur':int(r['d']),'cap':dec(r['c']),'cap_uom':str(r['cu']).lower()})
    mt_ws={}; mt_info={}
    for r in qy('select id,code,name,workshop_id from machine_types'):
        mt_id=int(r['id']); ws_id=int(r['workshop_id'])
        mt_ws[mt_id]=ws_id
        mt_info[mt_id]={
            'code':r['code'],
            'name':r['name'],
            'ws_id':ws_id,
            'ws_code':ws_info.get(ws_id,{}).get('code',''),
            'ws_name':ws_info.get(ws_id,{}).get('name',''),
        }
    mac_by_type=defaultdict(list)
    mac_info={}
    for r in qy('select id,code,machine_type_id,status from machines'):
        # Current master data uses "idle" as the normal available state.
        # Only exclude explicit decommission/inactive markers.
        st = str(r['status']).split('.')[-1].lower()
        if st in {'inactive','disabled','retired','decommissioned'}:
            continue
        mt_id=int(r['machine_type_id']); mid=int(r['id']); ws_id=mt_ws.get(mt_id)
        mac_by_type[mt_id].append({'id':mid,'code':r['code']})
        mac_info[mid]={
            'code':r['code'],
            'mt_id':mt_id,
            'mt_code':mt_info.get(mt_id,{}).get('code',''),
            'ws_id':ws_id,
            'ws_code':ws_info.get(ws_id,{}).get('code',''),
        }
    mac_id_by_code={v['code']:mid for mid,v in mac_info.items()}
    emp_by_ws=defaultdict(list)
    emp_info={}
    for r in qy('select id,code,workshop_id,status from employees'):
        st = str(r['status']).split('.')[-1].lower()
        if st in {'inactive','disabled','retired','left'}:
            continue
        ws_id=int(r['workshop_id']); eid=int(r['id'])
        emp_by_ws[ws_id].append({'id':eid,'code':r['code']})
        emp_info[eid]={
            'code':r['code'],
            'ws_id':ws_id,
            'ws_code':ws_info.get(ws_id,{}).get('code',''),
            'ws_name':ws_info.get(ws_id,{}).get('name',''),
        }
    emp_id_by_code={v['code']:eid for eid,v in emp_info.items()}
    mdown=defaultdict(list)
    for r in qy('select machine_id,start_time,end_time,is_working from machine_calendars'):
        if bool(r['is_working']): continue
        mdown[int(r['machine_id'])].append((pdt(r['start_time']),pdt(r['end_time'])))
    ew=defaultdict(list)
    for r in qy('select employee_id,week_in_cycle,weekday,start_time,end_time,shift_code,is_working from employee_weekly_calendars'):
        ew[int(r['employee_id'])].append({'w':int(r['week_in_cycle']),'d':int(r['weekday']),'s':ptm(r['start_time']),'e':ptm(r['end_time']),'shift':str(r['shift_code']),'on':bool(r['is_working'])})
    eevt=defaultdict(list)
    for r in qy('select employee_id,start_time,end_time,is_working from employee_calendars'):
        eevt[int(r['employee_id'])].append((pdt(r['start_time']),pdt(r['end_time']),bool(r['is_working'])))
    conv=defaultdict(list)
    for r in qy('select step_id,from_uom_code,from_qty,to_uom_code,to_qty,conversion_type from step_quantity_conversions'):
        conv[int(r['step_id'])].append((str(r['from_uom_code']).lower(),dec(r['from_qty']),str(r['to_uom_code']).lower(),dec(r['to_qty']),str(r['conversion_type'])))
    con.close()
    return {
        'ws_info':ws_info,'mt_info':mt_info,'mac_info':mac_info,'emp_info':emp_info,
        'mats':mats,'purch':purch,'lead':lead,'inv':inv,'routes':routes,'r_by_target':r_by_target,
        'orders':orders,'steps':steps,'topo':topo,'pred':pred,'inputs':inputs,'cands':cands,
        'route_edges':edges,'route_id_by_code':route_id_by_code,
        'mt_ws':mt_ws,'mac_by_type':mac_by_type,'emp_by_ws':emp_by_ws,'mdown':mdown,'ew':ew,'eevt':eevt,'conv':conv,
        'mac_id_by_code':mac_id_by_code,'emp_id_by_code':emp_id_by_code,
    }
class Core:
    def __init__(self,d):
        self.d=d; self.start=datetime.now().replace(second=0,microsecond=0); self.anchor=(self.start-timedelta(days=self.start.weekday())).date()
        self.purchase_base=datetime.combine(self.start.date(),time(0,0))+timedelta(days=1,hours=8)
        self.planning_horizon_days=PLANNING_HORIZON_DAYS
        self.priority_weight=PRIORITY_WEIGHT
        self.due_weight=DUE_WEIGHT
        self.inv=dict(d['inv']); self.mbook=defaultdict(list); self.ebook=defaultdict(list); self.mbusy=Counter()
        self.tasks=[]; self.trace=[]; self.problems=[]; self.outcomes=[]; self.pool={}; self.failed=[]
        self.execution_blocks=[]
        self.merge_stats={'passes':0,'merged_rows':0}
        self.order_penalty={}
        self.order_rank={}
        self._current_step_code = '-'
        self._current_candidate_count = 0
        self._current_order_code = '-'
        self._current_step_idx = 0
        self._current_step_total = 0
        self._order_stats = Counter()
        self._failed_order_first_logged = set()
    def add_trace(self,order,route,step,et,mat,m,e,qty,msg):
        self.trace.append({'event_time':fdt(datetime.now()),'order_code':order,'route_code':route,'step_code':step,'event_type':et,'material_code':mat,'machine_code':m,'employee_code':e,'qty':f"{q(qty):.4f}",'message':msg})
    def add_prob(self,et,ec,pt,sv,desc,st=None,en=None,order_code='',route_code='',step_code='',analysis='',suggestion=''):
        self.problems.append({
            'entity_type':et,'entity_code':ec,'problem_type':pt,'severity':sv,'start':fdt(st),'end':fdt(en),'description':desc,
            'order_code':order_code,'route_code':route_code,'step_code':step_code,'analysis':analysis,'suggestion':suggestion
        })
    def add_failed(
        self,
        o,
        route_code,
        step_code,
        problem_type,
        severity,
        start,
        end,
        reason,
        analysis,
        suggestion,
        resource_snapshot='',
        deadline=None,
        latest_end_target=None,
        material_ready=None,
        attempt_window='',
        lateness_penalty='',
        penalty_basis='',
    ):
        self.failed.append({
            'order_code':o.get('code',''),'priority':str(o.get('pri','')),'due_time':fdt(o.get('due')),
            'route_code':route_code,'step_code':step_code,'problem_type':problem_type,'severity':severity,
            'window_start':fdt(start),'window_end':fdt(end),'reason':reason,'analysis':analysis,'suggestion':suggestion,
            'resource_snapshot':resource_snapshot,
            'deadline':fdt(deadline),
            'latest_end_target':fdt(latest_end_target),
            'material_ready':fdt(material_ready),
            'attempt_window':attempt_window,
            'lateness_penalty':str(lateness_penalty or ''),
            'penalty_basis':str(penalty_basis or ''),
        })
        order_code = str(o.get('code',''))
        if order_code and order_code not in self._failed_order_first_logged:
            self._failed_order_first_logged.add(order_code)
            loc = f"{route_code}.{step_code}" if step_code and step_code != '-' else route_code
            rt = str(resource_snapshot or '-')
            msg = (
                f"失败定位 | 订单={order_code} | 工序={loc or '-'} | 类型={problem_type} "
                f"| 窗口={_fmt_dt_short(start)}~{_fmt_dt_short(end)} | 到料={_fmt_dt_short(material_ready)} "
                f"| 截止={_fmt_dt_short(deadline)} | 资源={rt}"
            )
            _progress_write(msg, level='warning')

    def _log_step_status(self, step_code: str, ready: datetime | None, latest_end: datetime | None, candidate_count: int, status: str):
        _progress_write(
            f"步骤状态 | 工序={step_code or '-'} | 候选={candidate_count} | ready={_fmt_dt_short(ready)} | latest={_fmt_dt_short(latest_end)} | 状态={status}",
            level='debug'
        )

    def _update_order_bar(self, idx: int, total: int, late: int, failed: int, solve_started: datetime, current_step: str | None = None):
        elapsed = (datetime.now() - solve_started).total_seconds() / 60.0
        avg = elapsed / max(1, idx)
        eta_min = max(0.0, avg * max(0, total - idx))
        planned = int(self._order_stats.get('planned', 0))
        fail_cnt = int(self._order_stats.get('failed', 0))
        _progress_write(
            f"订单进度 | 完成={idx}/{total} | 当前订单={self._current_order_code} | 工序={current_step or self._current_step_code} | 成功/失败={planned}/{fail_cnt} | 逾期={late} | 失败={failed} | 任务={len(self.tasks)} | ETA={eta_min:.1f}m",
            level='debug'
        )

    def _open_step_bar(self, order_code: str, total_steps: int):
        self._current_order_code = order_code
        self._current_step_idx = 0
        self._current_step_total = max(1, int(total_steps or 0))
        _progress_write(
            f"工序开始 | 订单={order_code} | 步骤数={self._current_step_total}",
            level='debug'
        )

    def _step_status(self, step_code: str, ready: datetime | None = None, latest_end: datetime | None = None, extra: str = ''):
        self._current_step_code = step_code or '-'
        self._log_step_status(step_code, ready, latest_end, self._current_candidate_count, extra or '-')

    def _advance_step_bar(self, step_code: str, outcome: str, qty: object = ''):
        self._current_step_idx += 1
        qty_text = _fmt_qty(qty) if qty not in ('', None) else '-'
        _progress_write(
            f"工序完成 | 订单={self._current_order_code} | 步骤={self._current_step_idx}/{self._current_step_total} | 工序={step_code or '-'} | 结果={outcome} | 数量={qty_text}",
            level='debug'
        )

    def _close_step_bar(self):
        _progress_write(f"工序结束 | 订单={self._current_order_code}", level='debug')

    def _log_order_start(self, idx: int, total: int, o):
        _progress_write(
            f"订单开始 | 序号={idx}/{total} | 订单={o.get('code', '')} | 优先级={o.get('pri', '')} | 数量={_fmt_qty(o.get('qty', ''))} | 交期={_fmt_dt_short(o.get('due'))}",
            level='info'
        )

    def _log_order_done(self, o, last, merged_now: int, order_start: datetime):
        order_elapsed = (datetime.now() - order_start).total_seconds() / 60.0
        _progress_write(
            f"订单完成 | 订单={o.get('code', '')} | 结果={last.get('result', '-')} | 延迟={float(last.get('delay', 0.0) or 0.0):.1f}m | 任务={len(self.tasks)} | 合并={merged_now} | 耗时={order_elapsed:.1f}m",
            level='info'
        )

    def solve(self):
        orders = list(self.d['orders'])
        if orders:
            pri_levels=sorted({int(o['pri']) for o in orders})
            due_levels=sorted({o['due'] for o in orders})
            pri_rank={p:i+1 for i,p in enumerate(pri_levels)}
            due_rank={d:i+1 for i,d in enumerate(due_levels)}
            for o in orders:
                p_rank=pri_rank[int(o['pri'])]
                d_rank=due_rank[o['due']]
                penalty=self.priority_weight*float(p_rank) + self.due_weight*float(d_rank)
                self.order_penalty[o['code']]=penalty
                self.order_rank[o['code']]={'priority_rank':p_rank,'due_rank':d_rank}
            orders.sort(key=lambda o:(self.order_penalty.get(o['code'],0.0), int(o['pri']), o['due'], o['id']))
        total = len(orders)
        for idx,o in enumerate(orders,1):
            order_start = datetime.now()
            self._log_order_start(idx, total, o)
            self.solve_order(idx,o)
            before = len(self.tasks)
            self.tasks, info = _batch_merge_tasks(self.tasks, self.d, show_progress=False)
            merged_now = int(info.get('merged_rows', 0))
            self.merge_stats['passes'] += 1
            self.merge_stats['merged_rows'] += merged_now
            if merged_now > 0 or len(self.tasks) != before:
                self._rebuild_books_from_tasks()
            last = self.outcomes[-1] if self.outcomes else {}
            self._order_stats[last.get('result', 'failed')] += 1
            self._log_order_done(o, last, merged_now, order_start)
    def _rebuild_books_from_tasks(self):
        self.mbook=defaultdict(list)
        self.ebook=defaultdict(list)
        self.mbusy=Counter()
        mac_id_by_code=self.d.get('mac_id_by_code',{})
        emp_id_by_code=self.d.get('emp_id_by_code',{})
        for t in self.tasks:
            try:
                st=pdt(t.get('planned_start',''))
                en=pdt(t.get('planned_end',''))
            except Exception:
                continue
            mcode=str(t.get('machine_code','')).strip()
            ecode=str(t.get('employee_code','')).strip()
            mid=mac_id_by_code.get(mcode)
            eid=emp_id_by_code.get(ecode)
            if mid is not None:
                self.mbook[mid].append((st,en))
                try:
                    self.mbusy[mid]+=max(1,int(float(t.get('duration_min','0') or 0)))
                except Exception:
                    self.mbusy[mid]+=max(1,int((en-st).total_seconds()/60.0))
            if eid is not None:
                self.ebook[eid].append((st,en))

    def route_for(self,o):
        if o['route'] in self.d['routes']: return o['route']
        lst=self.d['r_by_target'].get(o['mat'],[]); return lst[0] if lst else None
    def _snapshot_order_state(self):
        return {
            'tasks_len': len(self.tasks),
            'trace_len': len(self.trace),
            'mbook': copy.deepcopy(self.mbook),
            'ebook': copy.deepcopy(self.ebook),
            'mbusy': self.mbusy.copy(),
        }
    def _rollback_order_state(self,snap):
        self.tasks=self.tasks[:int(snap.get('tasks_len',0))]
        self.trace=self.trace[:int(snap.get('trace_len',0))]
        self.mbook=copy.deepcopy(snap.get('mbook',defaultdict(list)))
        self.ebook=copy.deepcopy(snap.get('ebook',defaultdict(list)))
        self.mbusy=Counter(snap.get('mbusy',Counter()))
    @staticmethod
    def _consume_inventory(inv_map,mid,need):
        need_q=q(Decimal(str(need)))
        if need_q<=0:
            return Decimal('0'),Decimal('0')
        av=inv_map.get(mid,Decimal('0'))
        use=min(av,need_q)
        rem=q(need_q-use)
        inv_map[mid]=q(av-use)
        return q(use),rem
    @staticmethod
    def _calc_input_need(mode,planned_output_qty,output_qty_per_execution,exec_count,input_qty):
        m=str(mode or '').strip().lower()
        out_q=q(Decimal(str(planned_output_qty)))
        step_out=q(Decimal(str(output_qty_per_execution)))
        in_q=q(Decimal(str(input_qty)))
        ex=max(1,int(exec_count or 1))
        if m=='proportional_to_output':
            if step_out<=0:
                return Decimal('0')
            return q(out_q*in_q/step_out)
        if m=='packaging_per_pack':
            return q(Decimal(ex)*in_q)
        if m in {'fixed_per_execution','carrier_transfer'}:
            return q(Decimal(ex)*in_q)
        return q(Decimal(ex)*in_q)
    def reqs(self,rid,target_mat,target_qty,available_inv_snapshot):
        topo=self.d['topo'][rid]; tix={s:i for i,s in enumerate(topo)}; sinks=[s for s in topo if self.d['steps'][s]['out_mat']==target_mat]
        if not sinks: return None
        inv_trial={int(k):q(Decimal(str(v))) for k,v in available_inv_snapshot.items()}
        sink=max(sinks,key=lambda s:tix[s]); providers=defaultdict(list)
        for s in topo: providers[self.d['steps'][s]['out_mat']].append(s)
        req_out=defaultdict(lambda:Decimal('0')); req_out[sink]=q(target_qty); reservation_plan={}
        for s in reversed(topo):
            out=req_out.get(s,Decimal('0'))
            if out<=0: continue
            st=self.d['steps'][s]; ex=ceildiv(out,st['out_qty'])
            for inp in self.d['inputs'].get(s,[]):
                mode=str(inp.get('mode','')).strip().lower()
                need=self._calc_input_need(mode,out,st['out_qty'],ex,inp['qty'])
                c=[p for p in providers.get(inp['mat'],[]) if tix[p]<tix[s]]
                pv=max(c,key=lambda x:tix[x]) if c else None
                stock_use=Decimal('0'); upstream=Decimal('0'); external=Decimal('0'); carrier_reserved=Decimal('0')
                if mode=='carrier_transfer':
                    carrier_reserved=need
                    if need>0 and pv is not None:
                        upstream=need
                        req_out[pv]=q(req_out.get(pv,Decimal('0'))+upstream)
                else:
                    stock_use,rem=self._consume_inventory(inv_trial,inp['mat'],need)
                    upstream=Decimal('0'); external=rem
                    if rem>0 and pv is not None:
                        upstream=rem; external=Decimal('0')
                        req_out[pv]=q(req_out.get(pv,Decimal('0'))+upstream)
                key=(s,inp['mat'])
                row=reservation_plan.get(key)
                if row is None:
                    reservation_plan[key]={
                        'need_qty':need,
                        'mode':mode,
                        'stock_reserved_qty':stock_use,
                        'upstream_required_qty':q(upstream),
                        'external_required_qty':q(external),
                        'carrier_reserved_qty':q(carrier_reserved),
                        'provider_step_id':pv,
                    }
                else:
                    row['need_qty']=q(Decimal(str(row.get('need_qty',Decimal('0'))))+need)
                    row['stock_reserved_qty']=q(Decimal(str(row.get('stock_reserved_qty',Decimal('0'))))+stock_use)
                    row['upstream_required_qty']=q(Decimal(str(row.get('upstream_required_qty',Decimal('0'))))+upstream)
                    row['external_required_qty']=q(Decimal(str(row.get('external_required_qty',Decimal('0'))))+external)
                    row['carrier_reserved_qty']=q(Decimal(str(row.get('carrier_reserved_qty',Decimal('0'))))+carrier_reserved)
                    if row.get('provider_step_id') is None:
                        row['provider_step_id']=pv
        req={}
        for s in topo:
            out=req_out.get(s,Decimal('0'))
            if out<=0: continue
            req[s]={'out':out,'ex':ceildiv(out,self.d['steps'][s]['out_qty'])}
        return req,reservation_plan,inv_trial
    def ext_mat(self,o,route_code,step_code,mid,need,order_purchase_plan):
        mat=self.d['mats'][mid]; rem=q(Decimal(str(need)))
        if rem<=0:
            return self.start
        if not self.d['purch'].get(mid,False):
            self.add_prob('material',mat['code'],'material_unavailable','critical',f'need={rem:.4f} not purchasable')
            return None
        ld=int(self.d['lead'].get(mid,0)); ready=self.purchase_base+timedelta(days=ld)
        b=order_purchase_plan.get(mid)
        if b is None:
            b={'mat':mat['code'],'qty':Decimal('0'),'lead':ld,'purchase_start':self.purchase_base,'ready':ready,'orders':set(),'routes':set(),'steps':set(),'cnt':0}; order_purchase_plan[mid]=b
        b['qty']=q(Decimal(str(b['qty']))+rem); b['orders'].add(o['code']); b['routes'].add(route_code); b['steps'].add(step_code); b['cnt']=int(b['cnt'])+1
        self.add_trace(o['code'],route_code,step_code,'PURCHASE',mat['code'],'','',rem,f'purchase_start={self.purchase_base:%Y-%m-%d %H:%M:%S}; lead_days={ld}; ready={ready:%Y-%m-%d %H:%M:%S}')
        return ready
    def merge_purchase_plan(self,order_purchase_plan):
        for mid,b in order_purchase_plan.items():
            cur=self.pool.get(mid)
            if cur is None:
                self.pool[mid]={
                    'mat':str(b.get('mat','')),
                    'qty':q(Decimal(str(b.get('qty',Decimal('0'))))),
                    'lead':int(b.get('lead',0)),
                    'purchase_start':b.get('purchase_start'),
                    'ready':b.get('ready'),
                    'orders':set(b.get('orders',set())),
                    'routes':set(b.get('routes',set())),
                    'steps':set(b.get('steps',set())),
                    'cnt':int(b.get('cnt',0)),
                }
                continue
            cur['qty']=q(Decimal(str(cur.get('qty',Decimal('0'))))+Decimal(str(b.get('qty',Decimal('0')))))
            cur['orders'].update(b.get('orders',set()))
            cur['routes'].update(b.get('routes',set()))
            cur['steps'].update(b.get('steps',set()))
            cur['cnt']=int(cur.get('cnt',0))+int(b.get('cnt',0))
    def missing_conversions(self,sid):
        st=self.d['steps'][sid]; miss=[]
        ins_by_uom=defaultdict(list)
        for i in self.d['inputs'].get(sid,[]):
            if self.d['mats'].get(i['mat'],{}).get('type')=='auxiliary':
                continue
            ins_by_uom[i['uom']].append(i)
        for iu,rows in ins_by_uom.items():
            if iu==st['out_uom']:
                continue
            has=any(c[0]==iu and c[2]==st['out_uom'] for c in self.d['conv'].get(sid,[]))
            if has:
                continue
            from_qty=q(sum((r['qty'] for r in rows), Decimal('0')))
            miss.append({'from_uom':iu,'to_uom':st['out_uom'],'from_qty':from_qty,'to_qty':st['out_qty']})
        return miss
    def step_resource_snapshot(self,sid):
        rows=[]
        for c in self.d['cands'].get(sid,[]):
            mt=self.d['mt_info'].get(c['mt'],{})
            ws_id=mt.get('ws_id')
            m_cnt=len(self.d['mac_by_type'].get(c['mt'],[]))
            e_cnt=len(self.d['emp_by_ws'].get(ws_id,[]))
            rows.append({
                'mt_code':mt.get('code',c.get('mt_code','')),
                'workshop_code':mt.get('ws_code',''),
                'machine_count':m_cnt,
                'employee_count':e_cnt,
                'duration_min':c.get('dur',0),
                'capacity':c.get('cap',Decimal('0')),
                'capacity_uom':c.get('cap_uom',''),
            })
        return rows
    def snapshot_text(self,rows):
        if not rows:
            return 'no_machine_type_candidate'
        parts=[]
        for r in rows:
            parts.append(
                f"{r['mt_code']}@{r['workshop_code']}(m={r['machine_count']},e={r['employee_count']},dur={r['duration_min']},cap={r['capacity']}{r['capacity_uom']})"
            )
        return ' | '.join(parts)
    def analyze_allocation_failure(self,sid,ready,due_target):
        rows=self.step_resource_snapshot(sid)
        snap=self.snapshot_text(rows)
        if not rows:
            return 'no candidate machine_type for step', 'No machine type mapping configured for this step', 'Add route_step_machine_types mapping for this step', snap
        no_machine=[r for r in rows if r['machine_count']==0]
        no_emp=[r for r in rows if r['employee_count']==0]
        if no_machine:
            return 'candidate machine_type has zero machines', 'Machine type exists but has no available machine instances', 'Add machine instances in machines.csv for this machine type', snap
        if no_emp:
            return 'candidate workshop has zero employees', 'Workshop has no available employees for this machine type', 'Add employees to the workshop or adjust workshop bindings', snap
        if ready>due_target:
            return 'material ready is later than latest_end target', 'Material becomes available after the backward latest_end target', 'Reduce lead time, increase stock, or relax due date', snap
        return 'no slot in search window', 'Candidate resources exist, but all slots conflict with calendars or existing bookings', 'Increase clean resources or widen the slot search window', snap
    def cap_to_output(self,sid,cap_qty: Decimal,from_uom: str,to_uom: str)->Decimal:
        if from_uom==to_uom:
            return q(cap_qty)
        for fu,fq,tu,tq,_ in self.d['conv'].get(sid,[]):
            if fu==from_uom and tu==to_uom and fq>0 and tq>0:
                return q(cap_qty*tq/fq)
        return Decimal('0')
    def candidate_lot_max(self,sid,cand,out_uom: str)->Decimal:
        return self.cap_to_output(sid,cand.get('cap',Decimal('0')),cand.get('cap_uom',''),out_uom)
    def e_windows(self,eid,d):
        wd=d.isoweekday(); wc=((d-self.anchor).days//7)%2+1; arr=[]
        for r in self.d['ew'].get(eid,[]):
            if not r['on'] or r['w']!=wc or r['d']!=wd: continue
            st=datetime.combine(d,r['s']); en=datetime.combine(d,r['e']);
            if en<=st: en+=timedelta(days=1)
            arr.append((st,en))
        arr=self.merge(arr); ds=datetime.combine(d,time(0,0)); de=ds+timedelta(days=1)
        adds=[]; subs=[]
        for s,e,on in self.d['eevt'].get(eid,[]):
            if not ov(ds,de,s,e): continue
            seg=(max(ds,s),min(de,e)); (adds.append(seg) if on else subs.append(seg))
        if adds: arr=self.merge(arr+adds)
        if subs: arr=self.sub(arr,subs)
        return arr
    @staticmethod
    def merge(it):
        if not it: return []
        it=sorted(it,key=lambda x:x[0]); out=[it[0]]
        for s,e in it[1:]:
            ls,le=out[-1]
            if s<=le: out[-1]=(ls,max(le,e))
            else: out.append((s,e))
        return out
    @staticmethod
    def sub(base,subs):
        out=base[:]
        for ss,se in subs:
            nxt=[]
            for bs,be in out:
                if se<=bs or be<=ss: nxt.append((bs,be)); continue
                if bs<ss: nxt.append((bs,ss))
                if se<be: nxt.append((se,be))
            out=nxt
        return out
    def e_ok(self,eid,st,en):
        cur=st
        while cur<en:
            dd=datetime.combine((cur+timedelta(days=1)).date(),time(0,0)); seg_end=min(en,dd); ok=False
            for a,b in self.e_windows(eid,cur.date()):
                if a<=cur and seg_end<=b: ok=True; break
            if not ok: return False
            cur=seg_end
        return True
    def can(self,mid,eid,st,en):
        for a,b in self.d['mdown'].get(mid,[]):
            if ov(st,en,a,b): return False
        for a,b in self.mbook.get(mid,[]):
            if ov(st,en,a,b): return False
        for a,b in self.ebook.get(eid,[]):
            if ov(st,en,a,b): return False
        for a,b,on in self.d['eevt'].get(eid,[]):
            if (not on) and ov(st,en,a,b): return False
        return self.e_ok(eid,st,en)
    def slot(self,mid,eid,dur,ready,due):
        step=timedelta(minutes=30); d=timedelta(minutes=dur)
        horizon=timedelta(days=self.planning_horizon_days)
        cur=due; lim=max(ready,due-horizon)
        while cur-d>=lim:
            st, en = cur-d, cur
            if st>=ready and self.can(mid,eid,st,en): return st,en
            cur-=step
        cur=max(ready,due); lim2=cur+horizon
        while cur+d<=lim2:
            st,en=cur,cur+d
            if self.can(mid,eid,st,en): return st,en
            cur+=step
        return None
    def alloc(self,o,route,step,sid,work_units,pqty,ready,due_target,mode):
        best=None; best_l=10**18
        for c in self.d['cands'].get(sid,[]):
            ws=self.d['mt_ws'].get(c['mt']); macs=self.d['mac_by_type'].get(c['mt'],[]); emps=self.d['emp_by_ws'].get(ws,[])
            if not macs or not emps: continue
            base_dur=max(1,int(c['dur']))
            if mode=='batch':
                lot_max=self.candidate_lot_max(sid,c,step['out_uom'])
                if lot_max<=0:
                    continue
                lot_qty=min(q(pqty),lot_max)
                if lot_qty<=0:
                    continue
                dur=base_dur
            else:
                lot_qty=q(pqty)
                dur=base_dur*max(1,work_units)
            for m in macs:
                for e in emps:
                    sl=self.slot(m['id'],e['id'],dur,ready,due_target)
                    if sl is None: continue
                    st,en=sl; late=max(0.0,(en-due_target).total_seconds()/60.0)
                    if late<best_l or (abs(late-best_l)<1e-6 and (best is None or en<best['en'])):
                        best_l=late; best={'mid':m['id'],'mcode':m['code'],'eid':e['id'],'ecode':e['code'],'st':st,'en':en,'dur':dur,'qty':lot_qty}
        return best
    def solve_order(self,seq,o):
        order_snapshot=self._snapshot_order_state()
        order_purchase_plan={}
        inv_trial={int(k):q(Decimal(str(v))) for k,v in self.inv.items()}
        req_mat_code=self.d['mats'].get(o['mat'],{}).get('code','')
        target_stock_used,net_target_qty=self._consume_inventory(inv_trial,o['mat'],o['qty'])
        if target_stock_used>0:
            self.add_trace(
                o['code'],'-','-','MATERIAL_CONSUME',req_mat_code,'','',target_stock_used,
                f'required={o["qty"]:.4f}; from_stock={target_stock_used:.4f}; level=order_target'
            )
        if net_target_qty<=0:
            msg='fulfilled_by_inventory'
            self.inv=inv_trial
            self.add_trace(o['code'],'-','-','DEMAND_DONE',req_mat_code,'','',o['qty'],msg)
            self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':'-','result':'planned','finish':self.start,'delay':0.0,'msg':msg}); return

        rid=self.route_for(o)
        if rid is None:
            reason='missing route'; analysis='Order has no explicit route and no route matches requested material'; suggestion='Provide orders.route_code or add target-material mapping in process_routes'
            self.add_prob('demand',o['code'],'missing_route','critical',reason,None,o['due'],o['code'],'','',analysis,suggestion)
            self.add_failed(o,'-','-','missing_route','critical',None,o['due'],reason,analysis,suggestion)
            self._rollback_order_state(order_snapshot)
            self._advance_step_bar('-', 'fail')
            self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':'-','result':'failed','finish':None,'delay':0.0,'msg':reason}); return

        route=self.d['routes'][rid]; rq=self.reqs(rid,o['mat'],net_target_qty,inv_trial)
        if rq is None:
            reason='missing sink step'; analysis='Route has no sink step producing the requested material'; suggestion='Check route_steps.output_material_code against order requested_material_code'
            self.add_prob('demand',o['code'],'missing_sink','critical',reason,None,o['due'],o['code'],route['code'],'',analysis,suggestion)
            self.add_failed(o,route['code'],'-','missing_sink','critical',None,o['due'],reason,analysis,suggestion)
            self._rollback_order_state(order_snapshot)
            self._advance_step_bar('-', 'fail')
            self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':route['code'],'result':'failed','finish':None,'delay':0.0,'msg':reason}); return
        req,reservation_plan,inv_after_trial=rq
        topo=self.d['topo'][rid]
        succ=defaultdict(list)
        for a,b in self.d['route_edges'].get(rid,[]): succ[a].append(b)
        step_start_min={}; step_end_max={}
        req_steps=[sid for sid in topo if sid in req]
        total_req_steps=max(1,len(req_steps))
        done_steps=0
        self._open_step_bar(o['code'], total_req_steps)
        try:
            for sid in reversed(topo):
                if sid not in req: continue
                stp=self.d['steps'][sid]; ex=req[sid]['ex']
                self._current_step_code = stp['code']
                self._current_candidate_count = len(self.d['cands'].get(sid, []))
                self._current_step_idx = done_steps
                self._current_step_total = total_req_steps
                succ_ready=[]
                for s in succ.get(sid,[]):
                    if s in step_start_min and s in req:
                        succ_ready.append(step_start_min[s])
                if succ_ready:
                    latest_end=min(succ_ready)
                else:
                    latest_end=o['due']
                ready=self.start
                self._step_status(stp['code'], ready=ready, latest_end=latest_end, extra='检查物料')
                for i in self.d['inputs'].get(sid,[]):
                    f=reservation_plan.get((sid,i['mat'])) or {}
                    mode=str(f.get('mode',i.get('mode',''))).strip().lower()
                    need=q(Decimal(str(f.get('need_qty',self._calc_input_need(mode,req[sid]['out'],stp['out_qty'],ex,i['qty'])))))
                    stock_used=q(Decimal(str(f.get('stock_reserved_qty',Decimal('0')))))
                    upstream_required=q(Decimal(str(f.get('upstream_required_qty',Decimal('0')))))
                    external_required=q(Decimal(str(f.get('external_required_qty',Decimal('0')))))
                    carrier_reserved=q(Decimal(str(f.get('carrier_reserved_qty',Decimal('0')))))
                    pv=f.get('provider_step_id')
                    mat_code=self.d['mats'][i['mat']]['code']

                    if stock_used>0:
                        self.add_trace(
                            o['code'],route['code'],stp['code'],'MATERIAL_CONSUME',
                            mat_code,'','',stock_used,
                            f'required={need:.4f}; from_stock={stock_used:.4f}; level=step_input'
                        )
                    if carrier_reserved>0 and pv is None:
                        reason=f'carrier provider missing at {stp["code"]}'
                        analysis='consumption_mode=carrier_transfer requires upstream provider; carrier reservation does not consume inventory or purchase'
                        suggestion='Add predecessor step producing this carrier material, or fix route_step_dependencies/input mapping'
                        self.add_prob('step',f"{route['code']}.{stp['code']}",'carrier_provider_missing','critical',reason,ready,latest_end,o['code'],route['code'],stp['code'],analysis,suggestion)
                        self.add_failed(o,route['code'],stp['code'],'carrier_provider_missing','critical',ready,latest_end,reason,analysis,suggestion,deadline=o['due'],latest_end_target=latest_end,material_ready=ready,attempt_window=f"{fdt(ready)} ~ {fdt(latest_end)}")
                        self._rollback_order_state(order_snapshot)
                        self._advance_step_bar(stp['code'], 'fail')
                        self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':route['code'],'result':'failed','finish':None,'delay':0.0,'msg':reason}); return
                    if upstream_required>0 and pv is not None:
                        up_step=self.d['steps'].get(pv,{}).get('code','')
                        self.add_trace(
                            o['code'],route['code'],stp['code'],'MATERIAL_TRANSFER',
                            mat_code,'','',upstream_required,
                            f'from_upstream_step={up_step}; required={need:.4f}; from_upstream={upstream_required:.4f}; mode={mode}'
                        )
                    if external_required>0:
                        rr=self.ext_mat(o,route['code'],stp['code'],i['mat'],external_required,order_purchase_plan)
                        if rr is None:
                            reason=f'material shortage at {stp["code"]}'; analysis='Inventory is insufficient and material is not purchasable'; suggestion='Add stock or purchase rule in material_purchases'
                            self.add_failed(o,route['code'],stp['code'],'material_unavailable','critical',ready,latest_end,reason,analysis,suggestion,deadline=o['due'],latest_end_target=latest_end,material_ready=ready,attempt_window=f"{fdt(ready)} ~ {fdt(latest_end)}")
                            self._rollback_order_state(order_snapshot)
                            self._advance_step_bar(stp['code'], 'fail')
                            self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':route['code'],'result':'failed','finish':None,'delay':0.0,'msg':reason}); return
                        ready=max(ready,rr)
                self._step_status(stp['code'], ready=ready, latest_end=latest_end, extra='分配资源')
                miss=self.missing_conversions(sid)
                if miss:
                    miss_txt='; '.join([f"{m['from_qty']}{m['from_uom']}->{m['to_qty']}{m['to_uom']}" for m in miss])
                    reason=f'conversion missing at {stp["code"]}'
                    analysis=f'UOM jump exists in this step but conversion is missing: {miss_txt}'
                    suggestion='Add per-execution conversion rows in step_quantity_conversions.csv'
                    self.add_prob('step',f"{route['code']}.{stp['code']}",'conversion_missing','critical',reason,ready,latest_end,o['code'],route['code'],stp['code'],analysis,suggestion)
                    self.add_failed(o,route['code'],stp['code'],'conversion_missing','critical',ready,latest_end,reason,analysis,suggestion,miss_txt,deadline=o['due'],latest_end_target=latest_end,material_ready=ready,attempt_window=f"{fdt(ready)} ~ {fdt(latest_end)}")
                    self._rollback_order_state(order_snapshot)
                    self._advance_step_bar(stp['code'], 'fail')
                    self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':route['code'],'result':'failed','finish':None,'delay':0.0,'msg':f"{reason}: {miss_txt}"}); return
                segs=[]
                if stp['mode']=='batch':
                    remaining=q(req[sid]['out'])
                    while remaining>0:
                        al=self.alloc(o,route,stp,sid,1,remaining,ready,latest_end,'batch')
                        if al is None:
                            cause,analysis,suggestion,snap=self.analyze_allocation_failure(sid,ready,latest_end)
                            reason=f'allocation failed at {stp["code"]}'
                            desc=f"{reason}; cause={cause}; ready={fdt(ready)}; latest_end={fdt(latest_end)}; deadline={fdt(o['due'])}; candidates={snap}"
                            self.add_prob('step',f"{route['code']}.{stp['code']}",'allocation_failed','critical',desc,ready,latest_end,o['code'],route['code'],stp['code'],analysis,suggestion)
                            self.add_failed(o,route['code'],stp['code'],'allocation_failed','critical',ready,latest_end,reason,analysis,suggestion,snap,deadline=o['due'],latest_end_target=latest_end,material_ready=ready,attempt_window=f"{fdt(ready)} ~ {fdt(latest_end)}")
                            self._rollback_order_state(order_snapshot)
                            self._advance_step_bar(stp['code'], 'fail')
                            self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':route['code'],'result':'failed','finish':None,'delay':0.0,'msg':f"{reason} | {analysis}"}); return
                        self.mbook[al['mid']].append((al['st'],al['en'])); self.ebook[al['eid']].append((al['st'],al['en'])); self.mbusy[al['mid']]+=al['dur']
                        self.tasks.append({'order_code':o['code'],'route_id':str(route['id']),'route_code':route['code'],'step_code':stp['code'],'machine_code':al['mcode'],'employee_code':al['ecode'],'planned_qty':f"{al['qty']:.4f}",'planned_start':fdt(al['st']),'planned_end':fdt(al['en']),'duration_min':str(al['dur']),'status':'planned'})
                        segs.append((al['st'],al['en']))
                        remaining=q(remaining-al['qty'])
                else:
                    base_dur=max(1, int(self.d['cands'].get(sid, [{'dur': 1}])[0]['dur']))
                    max_run_min=180
                    chunk_exec=max(1, max_run_min//base_dur)
                    remain=ex
                    while remain>0:
                        run_exec=min(chunk_exec,remain)
                        remain-=run_exec
                        pqty=q(stp['out_qty']*Decimal(run_exec))
                        if remain==0:
                            produced_before=stp['out_qty']*Decimal(ex-run_exec)
                            pqty=max(stp['out_qty'], q(req[sid]['out']-produced_before))
                        al=self.alloc(o,route,stp,sid,run_exec,pqty,ready,latest_end,'single')
                        if al is None:
                            cause,analysis,suggestion,snap=self.analyze_allocation_failure(sid,ready,latest_end)
                            reason=f'allocation failed at {stp["code"]}'
                            desc=f"{reason}; cause={cause}; ready={fdt(ready)}; latest_end={fdt(latest_end)}; deadline={fdt(o['due'])}; candidates={snap}"
                            self.add_prob('step',f"{route['code']}.{stp['code']}",'allocation_failed','critical',desc,ready,latest_end,o['code'],route['code'],stp['code'],analysis,suggestion)
                            self.add_failed(o,route['code'],stp['code'],'allocation_failed','critical',ready,latest_end,reason,analysis,suggestion,snap,deadline=o['due'],latest_end_target=latest_end,material_ready=ready,attempt_window=f"{fdt(ready)} ~ {fdt(latest_end)}")
                            self._rollback_order_state(order_snapshot)
                            self._advance_step_bar(stp['code'], 'fail')
                            self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':route['code'],'result':'failed','finish':None,'delay':0.0,'msg':f"{reason} | {analysis}"}); return
                        self.mbook[al['mid']].append((al['st'],al['en'])); self.ebook[al['eid']].append((al['st'],al['en'])); self.mbusy[al['mid']]+=al['dur']
                        self.tasks.append({'order_code':o['code'],'route_id':str(route['id']),'route_code':route['code'],'step_code':stp['code'],'machine_code':al['mcode'],'employee_code':al['ecode'],'planned_qty':f"{al['qty']:.4f}",'planned_start':fdt(al['st']),'planned_end':fdt(al['en']),'duration_min':str(al['dur']),'status':'planned'})
                        segs.append((al['st'],al['en']))
                if not segs:
                    continue
                step_start_min[sid]=min(s for s,_ in segs)
                step_end_max[sid]=max(e for _,e in segs)
                self.add_trace(o['code'],route['code'],stp['code'],'STEP_ALLOC',self.d['mats'][stp['out_mat']]['code'],'','',req[sid]['out'],f"mode={stp['mode']} executions={ex}; latest_end={fdt(latest_end)}")
                done_steps += 1
                self._advance_step_bar(stp['code'], 'ok', req[sid]['out'])
        finally:
            self._close_step_bar()
        if not step_end_max:
            self._rollback_order_state(order_snapshot)
            self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':route['code'],'result':'failed','finish':None,'delay':0.0,'msg':'无可排产工序'}); return
        sink_steps=[s for s in topo if s in step_end_max and all((n not in req) for n in succ.get(s,[]))]
        fin=max(step_end_max[s] for s in (sink_steps or list(step_end_max.keys())))
        delay=max(0.0,(fin-o['due']).total_seconds()/60.0); msg='按期完成' if delay<=0 else f'逾期 {delay:.1f} 分钟'
        penalty=self.order_penalty.get(o['code'],0.0)
        rk=self.order_rank.get(o['code'],{})
        penalty_basis=f"priority_rank={rk.get('priority_rank','')},due_rank={rk.get('due_rank','')},w=({self.priority_weight:.2f},{self.due_weight:.2f})"
        if delay>0:
            self.add_prob('demand',o['code'],'late_order','high',f"{msg}; penalty={penalty:.3f}; {penalty_basis}",o['due'],fin,o['code'],route['code'],'','订单可行但完工时间晚于截止时间（按加权优先级保护）','提高瓶颈资源能力或放宽截止时间')
            self.add_failed(
                o,route['code'],'-','late_order','high',o['due'],fin,msg,
                '资源冲突下触发晚交；已按priority+due加权保护更高优先级/更早截止订单',
                '提升瓶颈资源能力或调整订单交期/优先级',
                deadline=o['due'],latest_end_target=o['due'],material_ready=self.start,attempt_window=f"{fdt(self.start)} ~ {fdt(o['due'])}",
                lateness_penalty=f"{penalty:.3f}",penalty_basis=penalty_basis
            )
        self.inv=inv_after_trial
        self.merge_purchase_plan(order_purchase_plan)
        if target_stock_used>0:
            balance_msg=f'order_target_stock_used={target_stock_used:.4f}; net_production_qty={net_target_qty:.4f}'
            self.add_trace(o['code'],route['code'],'-','MATERIAL_BALANCE',req_mat_code,'','',o['qty'],balance_msg)
        self.add_trace(o['code'],route['code'],'-','DEMAND_DONE',self.d['mats'][o['mat']]['code'],'','',o['qty'],msg)
        self.outcomes.append({'seq':seq,'code':o['code'],'pri':o['pri'],'due':o['due'],'route':route['code'],'result':'planned','finish':fin,'delay':delay,'msg':msg})
    def purchase_rows(self):
        out=[]
        for mid,b in sorted(self.pool.items(),key=lambda x:str(x[1]['mat'])):
            out.append({
                'material_code':str(b['mat']),
                'purchase_qty':f"{q(Decimal(str(b['qty']))):.4f}",
                'purchase_start_time':fdt(b['purchase_start']),
                'lead_time_days':str(b['lead']),
                'expected_ready_time':fdt(b['ready']),
                'order_count':str(len(b['orders'])),
                'consumer_step_count':str(int(b['cnt'])),
                'ref_orders':'|'.join(sorted(b['orders'])),
                'ref_routes':'|'.join(sorted(b['routes'])),
                'ref_steps':'|'.join(sorted(b['steps']))
            })
        return out
    def scores(self):
        total=max(1,len(self.outcomes)); planned=[o for o in self.outcomes if o['result']=='planned']; pc=len(planned); otc=sum(1 for o in planned if o['delay']<=1e-6); avg_l=(sum(o['delay'] for o in planned)/pc) if pc>0 else 1440.0
        hard=sum(1 for p in self.problems if p.get('severity') in {'critical','high'})
        # 分级逻辑（按订单数计分）：每单贡献 1/total
        # 例如 total=20 时，每单贡献 5 分（换算到0-100口径）
        feas_raw=pc/total
        onr=otc/total
        feas=feas_raw*100.0; ons=onr*100.0; lats=clamp(1.0-avg_l/1440.0)*100.0
        starts=[pdt(t['planned_start']) for t in self.tasks if t.get('planned_start')]; ends=[pdt(t['planned_end']) for t in self.tasks if t.get('planned_end')]
        if starts and ends and self.mbusy:
            span=max(1.0,(max(ends)-min(starts)).total_seconds()/60.0); mc=max(1,len(self.mbusy)); util=clamp(sum(self.mbusy.values())/(span*mc))
        else: util=0.0
        machine_util_score=util*100.0
        if self.mbusy:
            vals=list(self.mbusy.values()); mean=sum(vals)/len(vals); var=sum((v-mean)**2 for v in vals)/len(vals); cv=math.sqrt(var)/mean if mean>0 else 1.0; bal=clamp(1.0-cv)
        else: bal=0.0
        machine_balance_score=bal*100.0
        ebusy=Counter()
        for t in self.tasks:
            ecode=str(t.get('employee_code','')).strip()
            if not ecode:
                continue
            try:
                ebusy[ecode]+=max(1,int(float(t.get('duration_min','0') or 0)))
            except Exception:
                try:
                    ebusy[ecode]+=max(1,int((pdt(t.get('planned_end',''))-pdt(t.get('planned_start',''))).total_seconds()/60.0))
                except Exception:
                    ebusy[ecode]+=0
        if starts and ends and ebusy:
            span=max(1.0,(max(ends)-min(starts)).total_seconds()/60.0); ec=max(1,len(ebusy)); eutil=clamp(sum(ebusy.values())/(span*ec))
        else:
            eutil=0.0
        employee_util_score=eutil*100.0
        # 资源效率：把设备利用、设备均衡、员工利用合并成一个指标
        resource_efficiency_score=0.4*machine_util_score + 0.3*machine_balance_score + 0.3*employee_util_score
        resource_efficiency_raw=resource_efficiency_score/100.0
        g=[]; bo=defaultdict(list)
        for t in self.tasks: bo[t['order_code']].append(t)
        for rs in bo.values():
            rs=sorted(rs,key=lambda x:x['planned_start'])
            for i in range(1,len(rs)): g.append(max(0.0,(pdt(rs[i]['planned_start'])-pdt(rs[i-1]['planned_end'])).total_seconds()/60.0))
        avg_g=(sum(g)/len(g)) if g else 0.0; cont=clamp(1.0-avg_g/240.0); conts=cont*100.0
        defs=[
            ('feasibility_score',20.0,feas_raw,feas,'planned_orders/total_orders*100'),
            ('on_time_rate',35.0,onr,ons,'on_time_orders/total_orders*100'),
            ('lateness_score',30.0,avg_l,lats,'max(0,1-avg_late_min/1440)*100'),
            ('resource_efficiency',10.0,resource_efficiency_raw,resource_efficiency_score,'0.4*machine_util + 0.3*machine_balance + 0.3*employee_util'),
            ('continuity',5.0,cont,conts,'max(0,1-avg_gap/240)*100')
        ]
        out=[]; overall=0.0
        for m,w,raw,sc,f in defs:
            ws=sc*w/100.0; overall+=ws; out.append({'metric':m,'weight_pct':f"{w:.2f}",'raw_value':f"{raw:.6f}",'score_0_100':f"{sc:.2f}",'weighted_score':f"{ws:.2f}",'formula':f})
        out.append({'metric':'overall_score','weight_pct':'100.00','raw_value':f"{overall:.6f}",'score_0_100':f"{overall:.2f}",'weighted_score':f"{overall:.2f}",'formula':'sum(weighted_score)'})
        return out

SHELL_STYLE = """
<style>
:root {
  --bg: #f4f8fb;
  --card: #ffffff;
  --card-soft: #f8fcff;
  --text: #0f172a;
  --muted: #475569;
  --line: #cbd5e1;
  --line-strong: #94a3b8;
  --head: #e6edf4;
  --accent: #0369a1;
  --accent-soft: #e0f2fe;
  --danger: #c2410c;
  --code-bg: #eef6fb;
  --focus: rgba(3, 105, 161, 0.18);
  --shadow: 0 8px 22px rgba(15, 23, 42, 0.06);
}
* { box-sizing: border-box; }
body {
  margin: 0;
  color: var(--text);
  background: var(--bg);
  font-family: "Segoe UI", Arial, sans-serif;
  line-height: 1.45;
}
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.page { max-width: 1280px; margin: 0 auto; padding: 18px; }
.page-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  margin-bottom: 12px;
}
.page-title { margin: 0; font-size: 24px; font-weight: 700; }
.page-subtitle { margin-top: 4px; color: var(--muted); font-size: 13px; }
.home-link {
  color: var(--accent);
  text-decoration: none;
  font-weight: 600;
  white-space: nowrap;
}
.home-link:hover { color: #075985; }
.card {
  border: 1px solid var(--line);
  border-radius: 12px;
  background: var(--card);
  padding: 14px;
  margin-bottom: 14px;
  box-shadow: var(--shadow);
}
h1 { margin: 0; font-size: 24px; }
h2 { margin: 0 0 8px 0; font-size: 18px; }
h3 { margin: 12px 0 6px 0; font-size: 15px; color: #1e293b; }
.meta, .muted { color: var(--muted); font-size: 12px; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; }
.grid4 { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
.kpi, .stat {
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--card-soft);
  padding: 10px;
}
.k, .stat .k { font-size: 12px; color: var(--muted); }
.v, .stat .v { font-size: 22px; font-weight: 700; margin-top: 4px; }
.navcards { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
.navcard {
  border: 1px solid var(--line);
  border-radius: 12px;
  padding: 12px;
  background: #fff;
}
.navcard a { font-size: 16px; font-weight: 700; color: #075985; text-decoration: none; }
.table-tools, .dag-tools {
  margin-top: 8px;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
}
.field-input {
  min-width: 260px;
  max-width: 420px;
  width: 100%;
  border: 1px solid var(--line-strong);
  border-radius: 9px;
  padding: 7px 10px;
  font-size: 13px;
  color: var(--text);
  background: #fff;
}
.field-input:focus {
  border-color: var(--accent);
  outline: 3px solid var(--focus);
  outline-offset: 0;
}
.btn {
  border: 1px solid var(--line-strong);
  background: #fff;
  color: #244264;
  border-radius: 9px;
  font-size: 12px;
  font-weight: 700;
  padding: 7px 10px;
  cursor: pointer;
}
.btn:hover { background: var(--accent-soft); }
.btn-soft { background: var(--card-soft); }
.table-wrap {
  margin-top: 8px;
  border: 1px solid var(--line);
  border-radius: 10px;
  overflow: auto;
  max-height: 72vh;
  background: #fff;
}
.table { width: 100%; border-collapse: collapse; min-width: 860px; }
.table th, .table td {
  border: 1px solid var(--line);
  padding: 6px 8px;
  text-align: left;
  font-size: 13px;
  vertical-align: top;
  word-break: break-word;
}
.table th {
  background: var(--head);
  position: sticky;
  top: 0;
  z-index: 2;
  white-space: nowrap;
}
.table tbody tr:nth-child(even) { background: #f8fcff; }
.table tbody tr:hover { background: #eef6fb; }
.table tbody tr.row-hide { display: none; }
.nav { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }
.nav a {
  padding: 5px 10px;
  border: 1px solid var(--line);
  border-radius: 999px;
  background: #fff;
  font-size: 12px;
  font-weight: 700;
}
.nav a:hover { background: var(--accent-soft); text-decoration: none; }
.parent-row td { background: #eef6ff; font-weight: 700; }
.child-row td:first-child { color: var(--muted); padding-left: 18px; }
.code {
  font-family: Consolas, "Cascadia Mono", monospace;
  background: var(--code-bg);
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 1px 8px;
  font-size: 12px;
  color: #0f3c5c;
  display: inline-block;
  white-space: nowrap;
}
.table-count { margin-left: auto; white-space: nowrap; }
.dag-wrap {
  overflow: auto;
  max-height: 78vh;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: #f8fcff;
}
.dag-node { transition: opacity .2s ease; }
.dag-node rect { transition: stroke .2s ease, fill .2s ease, stroke-width .2s ease; }
.dag-node.is-highlight rect {
  stroke: #0369a1;
  stroke-width: 2.2;
  fill: #f0f9ff;
}
.dag-node.is-dim { opacity: .35; }
.dag-table-row.is-highlight { background: #edf6ff !important; }
.dag-table-row.is-dim { opacity: .45; }
.gantt-wrap {
  margin-top: 8px;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: #f8fcff;
  overflow: auto;
  max-height: 76vh;
}
.gantt-table { width: max-content; border-collapse: collapse; min-width: 100%; table-layout: fixed; }
.gantt-table th, .gantt-table td { border: 1px solid var(--line); padding: 0; vertical-align: top; }
.gantt-table thead th { position: sticky; top: 0; z-index: 6; background: #f4f8fb; }
.gantt-lane-head, .gantt-lane-cell {
  min-width: 220px;
  max-width: 220px;
  width: 220px;
  background: #fff;
  position: sticky;
  left: 0;
  z-index: 7;
}
.gantt-lane-head { font-size: 12px; color: var(--muted); font-weight: 700; padding: 10px; }
.gantt-lane-cell {
  padding: 6px 10px;
  font-size: 12px;
  line-height: 1.35;
  color: #163a63;
}
.gantt-scale {
  position: relative;
  background: #eef5fd;
  border-bottom: 1px solid var(--line);
}
.gantt-scale-day { height: 30px; }
.gantt-scale-hour { height: 22px; background: #f6fafe; }
.gantt-scale-item {
  position: absolute;
  top: 0;
  bottom: 0;
  border-right: 1px solid #d3dfec;
  font-size: 11px;
  color: #33506f;
  display: flex;
  align-items: center;
  padding-left: 6px;
  overflow: hidden;
  white-space: nowrap;
}
.gantt-lane {
  position: relative;
  height: 30px;
  background-image: linear-gradient(to right, rgba(51, 80, 111, 0.10) 1px, transparent 1px);
  background-size: 48px 30px;
}
.gantt-row.is-dim .gantt-lane-cell,
.gantt-row.is-dim .gantt-lane { opacity: .45; }
.gantt-bar {
  position: absolute;
  top: 6px;
  height: 18px;
  border-radius: 4px;
  border: 1px solid #0369a1;
  background: linear-gradient(90deg, #38bdf8 0%, #0ea5e9 100%);
  box-shadow: 0 1px 0 rgba(255,255,255,0.45) inset;
  cursor: pointer;
}
.gantt-bar.is-late {
  border-color: #c2410c;
  background: linear-gradient(90deg, #fb923c 0%, #ea580c 100%);
}
.gantt-bar.is-highlight {
  outline: 2px solid #0369a1;
  outline-offset: 1px;
  z-index: 4;
}
.gantt-bar.is-dim { opacity: .22; }
@media (max-width: 760px) {
  .page-head { flex-direction: column; align-items: flex-start; }
  .home-link { width: 100%; text-align: left; }
  .field-input { min-width: 100%; }
  .table-count { margin-left: 0; }
  .grid4 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .navcards { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .gantt-lane-head, .gantt-lane-cell { min-width: 168px; max-width: 168px; width: 168px; }
}
@media (max-width: 520px) {
  .grid4, .navcards { grid-template-columns: 1fr; }
}
</style>
"""


SHELL_SCRIPT = """
<script>
(() => {
  const norm = (s) => (s || '').toString().toLowerCase().replace(/\\s+/g, ' ').trim();

  const initTableFilters = () => {
    document.querySelectorAll('[data-table-filter]').forEach((box) => {
      const table = document.getElementById(box.dataset.target || '');
      if (!table || !table.tBodies.length) return;
      const input = box.querySelector('[data-filter-input]');
      const clearBtn = box.querySelector('[data-filter-clear]');
      const counter = box.querySelector('[data-filter-count]');
      const rows = Array.from(table.tBodies[0].rows);
      const total = rows.length;
      const apply = () => {
        const q = norm(input ? input.value : '');
        let visible = 0;
        rows.forEach((row) => {
          const hit = !q || norm(row.textContent).includes(q);
          row.classList.toggle('row-hide', !hit);
          if (hit) visible += 1;
        });
        if (counter) counter.textContent = `${visible} / ${total}`;
      };
      if (input) input.addEventListener('input', apply);
      if (clearBtn && input) {
        clearBtn.addEventListener('click', () => {
          input.value = '';
          apply();
          input.focus();
        });
      }
      apply();
    });
  };

  const initDagPanels = () => {
    document.querySelectorAll('[data-dag-panel]').forEach((panel) => {
      const searchInput = panel.querySelector('[data-dag-search]');
      const clearBtn = panel.querySelector('[data-dag-clear]');
      const compactBtn = panel.querySelector('[data-dag-toggle]');
      const statEl = panel.querySelector('[data-dag-count]');
      const nodes = Array.from(panel.querySelectorAll('.dag-node'));
      const tableRows = Array.from(document.querySelectorAll('.dag-table-row'));
      let compact = true;

      const setCompact = () => {
        panel.querySelectorAll('[data-compact-line]').forEach((lineEl) => {
          const idx = Number(lineEl.getAttribute('data-line-index') || '0');
          lineEl.style.display = compact && idx >= 3 ? 'none' : 'block';
        });
        if (compactBtn) compactBtn.textContent = compact ? '展开节点详情' : '收起节点详情';
      };

      const apply = () => {
        const q = norm(searchInput ? searchInput.value : '');
        let matched = 0;
        nodes.forEach((node) => {
          const code = norm(node.dataset.stepCode || '');
          const text = norm(node.dataset.nodeSearch || '');
          const hit = !q || code.includes(q) || text.includes(q);
          node.classList.toggle('is-highlight', !!q && hit);
          node.classList.toggle('is-dim', !!q && !hit);
          if (hit) matched += 1;
        });
        tableRows.forEach((row) => {
          const code = norm(row.dataset.stepCode || '');
          const text = norm(row.textContent);
          const hit = !q || code.includes(q) || text.includes(q);
          row.classList.toggle('is-highlight', !!q && hit);
          row.classList.toggle('is-dim', !!q && !hit);
        });
        if (statEl) statEl.textContent = q ? `命中 ${matched} / ${nodes.length}` : `节点 ${nodes.length}`;
      };

      if (searchInput) searchInput.addEventListener('input', apply);
      if (clearBtn && searchInput) {
        clearBtn.addEventListener('click', () => {
          searchInput.value = '';
          apply();
          searchInput.focus();
        });
      }
      if (compactBtn) {
        compactBtn.addEventListener('click', () => {
          compact = !compact;
          setCompact();
        });
      }
      setCompact();
      apply();
    });
  };

  const initGanttBoards = () => {
    document.querySelectorAll('[data-gantt-board]').forEach((board) => {
      const searchInput = board.querySelector('[data-gantt-search]');
      const clearBtn = board.querySelector('[data-gantt-clear]');
      const statEl = board.querySelector('[data-gantt-count]');
      const bars = Array.from(board.querySelectorAll('.gantt-bar'));
      const rows = Array.from(board.querySelectorAll('.gantt-row'));
      const apply = () => {
        const q = norm(searchInput ? searchInput.value : '');
        let hitBars = 0;
        rows.forEach((row) => {
          row.classList.remove('is-dim');
          row.classList.remove('is-highlight');
        });
        bars.forEach((bar) => {
          const text = norm(bar.dataset.ganttSearch || '');
          const hit = !q || text.includes(q);
          bar.classList.toggle('is-highlight', !!q && hit);
          bar.classList.toggle('is-dim', !!q && !hit);
          if (hit) {
            hitBars += 1;
            const rowId = bar.dataset.ganttRow || '';
            if (rowId) {
              const rowEl = board.querySelector(`.gantt-row[data-gantt-row='${rowId}']`);
              if (rowEl) rowEl.classList.add('is-highlight');
            }
          }
        });
        if (q) {
          rows.forEach((row) => {
            if (!row.classList.contains('is-highlight')) row.classList.add('is-dim');
          });
        }
        if (statEl) statEl.textContent = q ? `命中 ${hitBars} / ${bars.length}` : `任务 ${bars.length}`;
      };
      if (searchInput) searchInput.addEventListener('input', apply);
      if (clearBtn && searchInput) {
        clearBtn.addEventListener('click', () => {
          searchInput.value = '';
          apply();
          searchInput.focus();
        });
      }
      apply();
    });
  };

  initTableFilters();
  initDagPanels();
  initGanttBoards();
})();
</script>
"""


def shell(title,body):
    return (
        "<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'/>"
        "<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'/>"
        f"<title>{escape(title)}</title>{SHELL_STYLE}</head><body><div class='page'>"
        "<div class='page-head'><div>"
        f"<h1 class='page-title'>{escape(title)}</h1>"
        "<div class='page-subtitle'>排产结果与资源约束可视化</div>"
        "</div><a class='home-link' href='./index.html'>返回总览</a></div>"
        f"{body}</div>{SHELL_SCRIPT}</body></html>"
    )

def _safe_dt(v: str) -> datetime:
    try:
        return pdt(v)
    except Exception:
        return datetime.max

def _clip(t: str, n: int = 18) -> str:
    t = t or ''
    return t if len(t) <= n else (t[:n - 1] + '…')

def _fmt_qty(v: object) -> str:
    try:
        d = dec(v if v is not None and str(v) != '' else '0')
    except Exception:
        return str(v or '')
    if d == d.to_integral_value():
        return str(int(d))
    s = format(d, 'f').rstrip('0').rstrip('.')
    return s if s else '0'

def _fmt_msg_numbers(msg: object) -> str:
    s = str(msg or '')
    def _repl(m):
        tok = m.group(0)
        try:
            return _fmt_qty(tok)
        except Exception:
            return tok
    return re.sub(r'(?<![\w])[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?', _repl, s)

def _render_table_tools(target_id: str, placeholder: str) -> str:
    return (
        f"<div class='table-tools' data-table-filter data-target='{_attr(target_id)}'>"
        f"<input class='field-input' type='search' data-filter-input placeholder='{_attr(placeholder)}'/>"
        "<button type='button' class='btn btn-soft' data-filter-clear>清空</button>"
        "<span class='muted table-count' data-filter-count>0 / 0</span>"
        "</div>"
    )


def _wrap_table(table_html: str, table_id: str = '', placeholder: str = '输入关键词筛选') -> str:
    tools = _render_table_tools(table_id, placeholder) if table_id else ''
    return f"{tools}<div class='table-wrap'>{table_html}</div>"

def _schedule_nav(include_index: bool = False) -> str:
    links = []
    if include_index:
        links.append(("./index.html", "总览"))
    links.extend(
        [
            ("./orders.html", "全部订单"),
            ("./failed_orders.html", "失败分析"),
            ("./routes.html", "工艺路线索引"),
            ("./machines.html", "设备负载"),
            ("./purchases.html", "采购计划"),
            ("./problems.html", "问题诊断"),
            ("./trace.html", "事件追踪"),
            ("./gantt.html", "甘特图"),
            ("./scheduling_process.html", "排产过程"),
        ]
    )
    return "<div class='nav'>" + "".join(f"<a href='{href}'>{label}</a>" for href, label in links) + "</div>"

def _is_contiguous(prev_end: object, next_start: object, tol_seconds: int = 1) -> bool:
    try:
        pe = pdt(prev_end)
        ns = pdt(next_start)
    except Exception:
        return False
    return abs((ns - pe).total_seconds()) <= tol_seconds

def _build_step_key_to_id(d):
    mp={}
    for rid,topo in d.get('topo',{}).items():
        rc=d.get('routes',{}).get(rid,{}).get('code','')
        if not rc:
            continue
        for sid in topo:
            sc=d.get('steps',{}).get(sid,{}).get('code','')
            if sc:
                mp[(rc,sc)]=sid
    return mp

def _cap_to_output_for_step(d, sid: int, cap_qty: Decimal, from_uom: str, to_uom: str) -> Decimal:
    if from_uom == to_uom:
        return q(cap_qty)
    for fu,fq,tu,tq,_ in d.get('conv',{}).get(sid,[]):
        if fu==from_uom and tu==to_uom and fq>0 and tq>0:
            return q(cap_qty*tq/fq)
    return Decimal('0')

def _build_step_meta(d):
    meta={}
    step_key_to_id=_build_step_key_to_id(d)
    for (rc,sc),sid in step_key_to_id.items():
        st=d.get('steps',{}).get(sid,{})
        out_u=st.get('out_uom','')
        cands=sorted(d.get('cands',{}).get(sid,[]), key=lambda x:(x.get('mt',0),x.get('mt_code','')))
        if cands:
            c0=cands[0]
            lot_max=_cap_to_output_for_step(d,sid,c0.get('cap',Decimal('0')),c0.get('cap_uom',''),out_u)
            base_dur=int(c0.get('dur',0))
        else:
            lot_max=Decimal('0')
            base_dur=0
        meta[(rc,sc)]={'sid':sid,'mode':st.get('mode','single'),'lot_max':lot_max,'base_dur':base_dur}
    return meta

def _batch_merge_tasks(tasks, d, show_progress: bool = True):
    if not tasks:
        return [], {'merged_rows':0}
    meta=_build_step_meta(d)
    rows=sorted(tasks, key=lambda x:(x.get('order_code',''),x.get('route_code',''),x.get('step_code',''),x.get('machine_code',''),x.get('employee_code',''),x.get('planned_start','')))
    out=[]
    merged=0
    merged_duration_cap_min=180
    row_iter = rows
    for t in row_iter:
        if not out:
            out.append(dict(t))
            continue
        cur=dict(t); prev=out[-1]
        k=(cur.get('route_code',''),cur.get('step_code',''))
        m=meta.get(k,{})
        can_merge=(
            m.get('mode')=='batch'
            and prev.get('order_code','')==cur.get('order_code','')
            and prev.get('route_code','')==cur.get('route_code','')
            and prev.get('step_code','')==cur.get('step_code','')
            and prev.get('machine_code','')==cur.get('machine_code','')
            and prev.get('employee_code','')==cur.get('employee_code','')
        )
        if can_merge:
            pq=dec(prev.get('planned_qty','0') or '0')
            cq=dec(cur.get('planned_qty','0') or '0')
            try:
                prev_dur=max(1,int(float(prev.get('duration_min','1') or 1)))
            except Exception:
                prev_dur=1
            try:
                cur_dur=max(1,int(float(cur.get('duration_min','1') or 1)))
            except Exception:
                cur_dur=1
            merged_dur=prev_dur + cur_dur
            if merged_dur <= merged_duration_cap_min:
                prev['planned_qty']=f"{q(pq+cq):.4f}"
                prev['duration_min']=str(merged_dur)
                try:
                    prev_start=pdt(prev.get('planned_start',''))
                    prev['planned_end']=fdt(prev_start + timedelta(minutes=merged_dur))
                except Exception:
                    pass
                merged += 1
                continue
        out.append(cur)
    return out, {'merged_rows':merged}

def _recompute_outcome_finish(d, outcomes, tasks):
    by_order=defaultdict(list)
    for t in tasks:
        by_order[t.get('order_code','')].append(t)
    step_key_to_id=_build_step_key_to_id(d)
    succ_by_sid=defaultdict(list)
    for rid,edges in d.get('route_edges',{}).items():
        for a,b in edges:
            succ_by_sid[a].append(b)
    for o in outcomes:
        if o.get('result') != 'planned':
            continue
        oc=o.get('code',''); rc=o.get('route','')
        rows=by_order.get(oc,[])
        if not rows:
            o['finish']=None; o['delay']=0.0; o['msg']='无任务'
            continue
        rid=d.get('route_id_by_code',{}).get(rc)
        sink_codes=set()
        if rid is not None:
            for sid in d.get('topo',{}).get(rid,[]):
                if not succ_by_sid.get(sid):
                    sink_codes.add(d.get('steps',{}).get(sid,{}).get('code',''))
        rel=[r for r in rows if r.get('step_code','') in sink_codes] if sink_codes else rows
        fin_candidates=[]
        for r in rel:
            en=pdt(r.get('planned_end',''))
            fin_candidates.append(en)
        fin=max(fin_candidates) if fin_candidates else max(pdt(r.get('planned_end','')) for r in rows)
        delay=max(0.0,(fin-o['due']).total_seconds()/60.0)
        o['finish']=fin
        o['delay']=delay
        o['msg']='按期完成' if delay<=0 else f'逾期 {delay:.1f} 分钟'

def _build_execution_blocks(d, tasks, show_progress: bool = False):
    step_meta=_build_step_meta(d)
    rows=sorted(tasks,key=lambda x:(x.get('machine_code',''),x.get('employee_code',''),x.get('route_code',''),x.get('step_code',''),x.get('planned_start',''),x.get('order_code','')))
    blocks=[]
    block_seq=1
    last_block_idx={}
    row_iter = rows
    for i,t in enumerate(row_iter):
        rc=t.get('route_code',''); sc=t.get('step_code',''); mc=t.get('machine_code',''); ec=t.get('employee_code','')
        key=(mc,ec,rc,sc)
        st=pdt(t.get('planned_start','')); en=pdt(t.get('planned_end',''))
        qty=dec(t.get('planned_qty','0') or '0')
        md=step_meta.get((rc,sc),{})
        mode=md.get('mode','single')
        lot_max=md.get('lot_max',Decimal('0'))
        dur=int(float(t.get('duration_min','0') or 0))
        prev_idx=last_block_idx.get(key)
        merged=False
        if prev_idx is not None:
            b=blocks[prev_idx]
            contiguous=_is_contiguous(b['planned_end'], st)
            if contiguous and mode=='single':
                b['total_qty']=q(b['total_qty'] + qty)
                b['planned_end']=en
                b['duration_min']=max(0,b['duration_min']) + max(0,dur)
                b['source_task_count']+=1
                b['order_codes'].add(t.get('order_code',''))
                b['merge_type']='single_cross_order' if len(b['order_codes'])>1 else 'single_same_order'
                t['execution_block_code']=b['block_code']
                merged=True
            elif contiguous and mode=='batch' and b['order_codes']=={t.get('order_code','')}:
                if lot_max>0 and q(b['total_qty'] + qty) <= lot_max:
                    b['total_qty']=q(b['total_qty'] + qty)
                    b['source_task_count']+=1
                    b['merge_type']='batch_same_order'
                    base=int(md.get('base_dur',dur) or dur or 0)
                    b['duration_min']=base
                    b['planned_end']=b['planned_start'] + timedelta(minutes=base)
                    t['execution_block_code']=b['block_code']
                    merged=True
        if not merged:
            bcode=f"exec_blk_{block_seq:05d}"; block_seq+=1
            route_id=d.get('route_id_by_code',{}).get(rc)
            sid=_build_step_key_to_id(d).get((rc,sc))
            blocks.append({
                'block_code':bcode,
                'merge_type':'none',
                'route_code':rc,
                'route_id':route_id,
                'step_code':sc,
                'step_id':sid,
                'machine_code':mc,
                'employee_code':ec,
                'total_qty':qty,
                'planned_start':st,
                'planned_end':en,
                'duration_min':max(0,dur),
                'source_task_count':1,
                'order_codes':{t.get('order_code','')},
                'note':'',
            })
            last_block_idx[key]=len(blocks)-1
            t['execution_block_code']=bcode
    for b in blocks:
        if b['merge_type']=='none' and len(b['order_codes'])>1:
            b['merge_type']='single_cross_order'
        if len(b['order_codes'])>1:
            b['note']=f"orders={','.join(sorted([x for x in b['order_codes'] if x]))}"
        else:
            b['note']=''
    return blocks

def _aggregate_step_stats(tasks, machine_to_mt, blocks_by_code=None):
    agg={}
    for t in tasks:
        sc=t.get('step_code','')
        if sc not in agg:
            agg[sc]={'segments':0,'dur':0,'qty':Decimal('0'),'machines':set(),'mts':set(),'starts':[],'ends':[],'segments_detail':[],'block_codes':set(),'merge_types':set(),'source_task_count':0}
        x=agg[sc]
        x['segments'] += 1
        try: x['dur'] += int(float(t.get('duration_min','0') or 0))
        except Exception: pass
        try: x['qty'] = q(x['qty'] + dec(t.get('planned_qty','0') or '0'))
        except Exception: pass
        mc=t.get('machine_code','')
        if mc: x['machines'].add(mc); x['mts'].add(machine_to_mt.get(mc,''))
        st = _safe_dt(t['planned_start']) if t.get('planned_start') else None
        en = _safe_dt(t['planned_end']) if t.get('planned_end') else None
        if st: x['starts'].append(st)
        if en: x['ends'].append(en)
        if st and en:
            x['segments_detail'].append({
                'start': st,
                'end': en,
                'machine': t.get('machine_code',''),
                'employee': t.get('employee_code',''),
                'qty': t.get('planned_qty',''),
            })
        bcode=(t.get('execution_block_code','') or '').strip()
        if bcode and bcode not in x['block_codes']:
            x['block_codes'].add(bcode)
            if blocks_by_code and bcode in blocks_by_code:
                b=blocks_by_code[bcode]
                x['merge_types'].add(b.get('merge_type','none'))
                try:
                    x['source_task_count'] += int(b.get('source_task_count',1))
                except Exception:
                    pass
    for x in agg.values():
        x['segments_detail'] = sorted(x['segments_detail'], key=lambda r: (r['start'], r['end']))
    return agg

def _topological_levels(step_ids: list[int], edges: list[tuple[int,int]]) -> dict[int,int]:
    succ=defaultdict(list); indeg={sid:0 for sid in step_ids}
    for src,dst in edges:
        if src in indeg and dst in indeg:
            succ[src].append(dst); indeg[dst]+=1
    dq=deque([sid for sid in step_ids if indeg[sid]==0]); level={sid:0 for sid in step_ids}
    visited=0
    while dq:
        cur=dq.popleft(); visited += 1
        for nxt in succ.get(cur,[]):
            level[nxt]=max(level[nxt],level[cur]+1)
            indeg[nxt]-=1
            if indeg[nxt]==0: dq.append(nxt)
    if visited != len(step_ids):
        raise ValueError('route DAG has cycle')
    return level

def _render_route_dag_svg(d, route_code: str, step_stats: dict, failed_steps: set[str]) -> str:
    rid=d['route_id_by_code'].get(route_code)
    if rid is None:
        return "<p class='muted'>未找到工艺路线。</p>"
    topo=d['topo'].get(rid,[])
    if not topo:
        return "<p class='muted'>该工艺路线无工序。</p>"
    level=_topological_levels(topo,d['route_edges'].get(rid,[]))
    by_level=defaultdict(list)
    for sid in topo: by_level[level[sid]].append(sid)
    for lev in by_level: by_level[lev].sort(key=lambda sid: topo.index(sid))

    node_lines={}
    node_meta={}
    max_line_count=0
    max_chars=0
    for sid in topo:
        st=d['steps'][sid]; sc=st['code']
        s=step_stats.get(sc,{})
        has=sc in step_stats; failed=sc in failed_steps
        mode='单件' if st['mode']=='single' else '批处理'
        mcs='|'.join(sorted([x for x in s.get('machines',set()) if x])) or '-'
        lines=[f"工序: {sc} | {_clip(st['name'], 18)}", f"执行方式: {mode}", f"设备: {_clip(mcs, 52)}"]
        if has:
            segs=s.get('segments_detail',[])
            seg_count=s.get('segments',0)
            qty_total=s.get('qty',Decimal('0'))
            merge_types='、'.join(sorted([x for x in s.get('merge_types',set()) if x and x!='none'])) or '无'

            lines.append(f"拆分数量: {seg_count} 段，共 {_fmt_qty(qty_total)} {st.get('out_uom','')}")
            lines.append(f"合并类型: {merge_types}")
            preview_n = len(segs) if len(segs) <= 8 else 3
            for i,row in enumerate(segs[:preview_n],1):
                lines.append(f"子{i}: {row['start']:%m-%d %H:%M} ~ {row['end']:%m-%d %H:%M}")
            if len(segs)>preview_n:
                lines.append(f"... 其余 {len(segs)-preview_n} 段")
        else:
            lines.append("拆分数量: 0 段")
        node_lines[sid]=lines
        node_meta[sid]={
            'step_code': sc,
            'step_name': st.get('name',''),
        }
        max_line_count=max(max_line_count,len(lines))
        for line in lines:
            max_chars=max(max_chars,len(line))

    # DAG 单工序展示宽度下调到当前的 60%
    box_w=max(204, int(min(520, max(340, 180 + max_chars * 5)) * 0.75))
    box_h=max(220, 52 + max_line_count * 20)
    col_gap=30
    row_gap=96
    margin_x=24
    margin_y=56
    title_h=28
    max_level=max(level.values()) if level else 0
    max_cols=max((len(v) for v in by_level.values()), default=1)
    graph_w=max_cols*box_w + (max_cols-1)*col_gap
    width=margin_x*2 + graph_w
    height=margin_y*2 + title_h + (max_level+1)*box_h + max_level*row_gap

    pos={}
    for lev in range(max_level + 1):
        nodes=by_level.get(lev,[])
        row_w=len(nodes)*box_w + max(0,len(nodes)-1)*col_gap
        row_start_x=margin_x + (graph_w-row_w)//2
        for idx,sid in enumerate(nodes):
            pos[sid]=(row_start_x + idx*(box_w+col_gap), margin_y+title_h + lev*(box_h+row_gap))

    parts=[]
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        'style="background:#f4f8fc;border:1px solid #c7d6e6;border-radius:12px">'
    )
    parts.append(
        '<defs><marker id="arrowV" markerWidth="10" markerHeight="7" refX="5" refY="3.5" orient="auto">'
        '<polygon points="0 0, 10 3.5, 0 7" fill="#64748b"/></marker></defs>'
    )
    parts.append(
        f'<text x="{margin_x}" y="{margin_y - 8}" fill="#0f172a" font-size="16" '
        f'font-family="Microsoft YaHei UI, Noto Sans SC, Segoe UI">{escape(route_code)} 工艺DAG树</text>'
    )

    for src,dst in d['route_edges'].get(rid,[]):
        if src not in pos or dst not in pos: continue
        x1,y1=pos[src][0] + box_w//2, pos[src][1] + box_h
        x2,y2=pos[dst][0] + box_w//2, pos[dst][1]
        c1y=y1 + row_gap//2
        c2y=y2 - row_gap//2
        parts.append(
            f'<path d="M {x1} {y1} C {x1} {c1y}, {x2} {c2y}, {x2} {y2}" '
            'stroke="#94a3b8" fill="none" stroke-width="2" marker-end="url(#arrowV)"/>'
        )

    for sid in topo:
        st=d['steps'][sid]; sc=st['code']; x,y=pos[sid]
        has=sc in step_stats; failed=sc in failed_steps
        if failed:
            fill,stroke='#fee2e2','#dc2626'
        elif has:
            fill,stroke='#ffffff','#cbd5e1'
        else:
            fill,stroke='#f8fafc','#94a3b8'
        searchable=' '.join([sc.lower(), str(node_meta.get(sid,{}).get('step_name','')).lower(), *[str(x).lower() for x in node_lines.get(sid,[])]])
        parts.append(f'<g class="dag-node" data-step-code="{_attr(sc.lower())}" data-node-search="{_attr(searchable)}">')
        parts.append(
            f'<rect x="{x}" y="{y}" width="{box_w}" height="{box_h}" rx="10" ry="10" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>'
        )
        yline=y+24
        lines=node_lines.get(sid,[])
        for idx,line in enumerate(lines):
            color="#0f172a" if idx==0 else "#334155"
            size="14" if idx==0 else "12"
            weight="700" if idx==0 else "400"
            parts.append(
                f'<text x="{x+10}" y="{yline}" fill="{color}" font-size="{size}" font-weight="{weight}" '
                f'data-compact-line="1" data-line-index="{idx}" '
                f'font-family="Microsoft YaHei UI, Noto Sans SC, Segoe UI">{escape(line)}</text>'
            )
            yline += 18
        parts.append("</g>")
    parts.append("</svg>")
    return ''.join(parts)

def write_html(d,tasks,outcomes,purchases,problems,trace,scores,sync_error: str=''):
    by_o=defaultdict(list); by_r=defaultdict(list); by_m=defaultdict(list)
    for t in tasks: by_o[t['order_code']].append(t); by_r[t['route_code']].append(t); by_m[t['machine_code']].append(t)
    overall=next((r.get('score_0_100','n/a') for r in scores if r.get('metric')=='overall_score'),'n/a'); planned=sum(1 for o in outcomes if o['result']=='planned')
    failed_cnt=sum(1 for o in outcomes if o.get('result')=='failed')
    nav=_schedule_nav()
    order_display = _build_order_display_stats(d, outcomes, tasks)
    sync_card = f"<div class='card' style='border-color:#dc2626;background:#fff1f2'><h2>结果回写异常</h2><div class='muted'>{escape(sync_error)}</div></div>" if sync_error else ""
    idx=(
         f"<div class='card'><div class='grid4'>"
         f"<div class='stat'><div class='k'>任务数</div><div class='v'>{len(tasks)}</div></div>"
         f"<div class='stat'><div class='k'>订单数</div><div class='v'>{len(outcomes)}</div></div>"
         f"<div class='stat'><div class='k'>已排产订单</div><div class='v'>{planned}</div></div>"
         f"<div class='stat'><div class='k'>失败订单</div><div class='v'>{failed_cnt}</div></div>"
         f"<div class='stat'><div class='k'>涉及路线</div><div class='v'>{len(by_r)}</div></div>"
         f"<div class='stat'><div class='k'>涉及设备</div><div class='v'>{len(by_m)}</div></div>"
         f"<div class='stat'><div class='k'>问题数</div><div class='v'>{len(problems)}</div></div>"
         f"<div class='stat'><div class='k'>总评分</div><div class='v'>{escape(str(overall))}</div></div>"
         f"</div></div>"
         "<div class='card'><h2>页面导航</h2><div class='navcards'>"
         "<div class='navcard'><a href='./orders.html'>订单主线</a><div class='muted'>查看订单结果、需求产品/数量、实际产量与开工时间。</div></div>"
         "<div class='navcard'><a href='./routes.html'>路线索引</a><div class='muted'>按路线查看任务规模、关联订单与基础 DAG 跳转。</div></div>"
         "<div class='navcard'><a href='./failed_orders.html'>失败诊断</a><div class='muted'>定位失败窗口、根因分析与建议动作。</div></div>"
         "<div class='navcard'><a href='./gantt.html'>甘特分析</a><div class='muted'>Frepple-Lite 资源泳道 + 双层时间轴 + 搜索高亮。</div></div>"
         "<div class='navcard'><a href='./scheduling_process.html'>排产过程</a><div class='muted'>评分构成与关键事件（前 400 条）。</div></div>"
         "<div class='navcard'><a href='../planning_viz/index.html' target='_blank'>基础主数据</a><div class='muted'>跳转到 planning_viz 总览和基础工艺路线 DAG。</div></div>"
         "</div></div>"
         f"<div class='card'><h2>快速导航</h2><div class='muted'>以订单、路线、失败定位为主线浏览结果。</div>{nav}</div>"
         + sync_card
    )
    (OUT/'index.html').write_text(shell('排产总览',idx),encoding='utf-8')
    order_rows = []
    for o in outcomes:
        od = order_display.get(o['code'], {})
        delay_text = _fmt_qty(f"{float(o.get('delay', 0.0) or 0.0):.1f}")
        req_mat = od.get('requested_material_code', '-')
        req_qty = _fmt_qty(od.get('requested_qty', '0'))
        actual_qty = _fmt_qty(od.get('actual_qty', '0'))
        order_start = od.get('order_start', '-')
        order_rows.append(
            f"<tr><td><a href='./order_{escape(o['code'])}.html'><span class='code'>{escape(o['code'])}</span></a></td>"
            f"<td><span class='code'>{escape(req_mat)}</span></td><td>{req_qty}</td><td>{actual_qty}</td><td>{escape(order_start)}</td>"
            f"<td>{o['pri']}</td><td>{o['due']:%Y-%m-%d %H:%M}</td><td><span class='code'>{escape(o['route'])}</span></td>"
            f"<td>{'已排产' if o['result']=='planned' else '失败'}</td>"
            f"<td>{o['finish'].strftime('%Y-%m-%d %H:%M') if o['finish'] else '-'}</td>"
            f"<td>{delay_text}</td><td>{len(by_o.get(o['code'],[]))}</td><td>{escape(o['msg'])}</td></tr>"
        )
    o_rows=''.join(order_rows) or "<tr><td colspan='13'>无数据</td></tr>"
    orders_table=f"<table class='table'><thead><tr><th>订单</th><th>需求产品</th><th>需求数量</th><th>实际产量</th><th>订单开始</th><th>优先级</th><th>截止时间</th><th>工艺路线</th><th>结果</th><th>最晚完成</th><th>逾期分钟</th><th>任务数</th><th>说明</th></tr></thead><tbody>{o_rows}</tbody></table>"
    (OUT/'orders.html').write_text(shell('全部订单',f"<div class='card'><h2>全部订单</h2>{nav}{_wrap_table(orders_table)}</div>"),encoding='utf-8')
    route_rows = []
    for rc,ts in sorted(by_r.items()):
        rid = ts[0].get('route_id', '')
        route_name = ''
        try:
            rid_int = int(rid)
            route_name = str(d.get('routes', {}).get(rid_int, {}).get('name', ''))
        except Exception:
            rid_int = None
        order_codes = sorted({x.get('order_code','') for x in ts if x.get('order_code')})
        shown_orders = order_codes[:8]
        order_links = ''.join(
            f"<a href='./order_{escape(oc)}.html'><span class='code'>{escape(oc)}</span></a> "
            for oc in shown_orders
        )
        if len(order_codes) > len(shown_orders):
            order_links += f"<span class='muted'>... 共 {len(order_codes)} 单</span>"
        base_link = (
            f"<a href='../planning_viz/route_{rid_int}.html' target='_blank'>查看基础DAG</a>"
            if rid_int is not None else "<span class='muted'>-</span>"
        )
        route_rows.append(
            f"<tr><td><span class='code'>{escape(rc)}</span></td><td>{escape(route_name or '-')}</td>"
            f"<td>{len(ts)}</td><td>{len(order_codes)}</td><td>{order_links or '-'}</td><td>{base_link}</td></tr>"
        )
    r_rows=''.join(route_rows) or "<tr><td colspan='6'>无数据</td></tr>"
    routes_table=f"<table class='table' id='schedule-routes-table'><thead><tr><th>路线</th><th>路线名称</th><th>任务数</th><th>订单数</th><th>关联订单</th><th>基础DAG</th></tr></thead><tbody>{r_rows}</tbody></table>"
    (OUT/'routes.html').write_text(
        shell('工艺路线索引',f"<div class='card'><h2>工艺路线索引</h2><div class='muted'>可跳转查看工艺路线DAG图</div>{nav}{_wrap_table(routes_table, table_id='schedule-routes-table', placeholder='筛选路线编码/名称/订单')}</div>"),
        encoding='utf-8',
    )
    tr=''.join([f"<tr><td>{escape(r.get('event_time',''))}</td><td>{escape(r.get('order_code',''))}</td><td>{escape(r.get('route_code',''))}</td><td>{escape(r.get('step_code',''))}</td><td>{escape(r.get('event_type',''))}</td><td>{escape(r.get('message',''))}</td></tr>" for r in trace[:400]]) or "<tr><td colspan='6'>无事件</td></tr>"
    sf=''.join([f"<tr><td>{escape(r.get('metric',''))}</td><td>{escape(r.get('weight_pct',''))}</td><td>{escape(r.get('raw_value',''))}</td><td>{escape(r.get('score_0_100',''))}</td><td>{escape(r.get('weighted_score',''))}</td><td>{escape(r.get('formula',''))}</td></tr>" for r in scores])
    late_cnt=sum(1 for o in outcomes if o.get('delay',0)>0)
    score_table=f"<table class='table'><thead><tr><th>指标</th><th>权重(%)</th><th>原始值</th><th>得分</th><th>加权得分</th><th>公式</th></tr></thead><tbody>{sf}</tbody></table>"
    event_table=f"<table class='table'><thead><tr><th>时间</th><th>订单</th><th>路线</th><th>工序</th><th>事件</th><th>消息</th></tr></thead><tbody>{tr}</tbody></table>"
    (OUT/'scheduling_process.html').write_text(
        shell(
            '排产过程',
            f"<div class='card'><h2>排产过程总览</h2>{nav}"
            f"<p>订单总数：{len(outcomes)}；已排产：{planned}；失败：{failed_cnt}；逾期订单：{late_cnt}；总评分：{escape(str(overall))}</p></div>"
            f"<div class='card'><h2>评分明细</h2>{_wrap_table(score_table)}</div>"
            f"<div class='card'><h2>关键事件（前400条）</h2>{_wrap_table(event_table)}</div>"
        ),
        encoding='utf-8',
    )
def _extract_purchase_events(trace):
    out=[]
    for r in trace:
        if str(r.get('event_type','')).upper() != 'PURCHASE':
            continue
        msg=str(r.get('message',''))
        parsed={}
        for p in msg.split(';'):
            if '=' not in p:
                continue
            k,v=p.split('=',1)
            parsed[k.strip()] = v.strip()
        out.append({
            'order_code': r.get('order_code',''),
            'route_code': r.get('route_code',''),
            'step_code': r.get('step_code',''),
            'material_code': r.get('material_code',''),
            'qty': r.get('qty',''),
            'purchase_start_time': parsed.get('purchase_start',''),
            'lead_time_days': parsed.get('lead_days',''),
            'expected_ready_time': parsed.get('ready',''),
        })
    return sorted(out, key=lambda x: (x.get('purchase_start_time',''), x.get('order_code',''), x.get('material_code','')))

def _render_purchase_table(rows):
    tr=''.join(
        f"<tr><td>{escape(r.get('material_code',''))}</td><td>{_fmt_qty(r.get('qty',''))}</td>"
        f"<td>{escape(r.get('purchase_start_time',''))}</td><td>{escape(r.get('lead_time_days',''))}</td>"
        f"<td>{escape(r.get('expected_ready_time',''))}</td><td>{escape(r.get('step_code',''))}</td><td>{escape(r.get('order_code',''))}</td></tr>"
        for r in rows
    ) or "<tr><td colspan='7'>无采购触发记录</td></tr>"
    table_html = (
        "<table class='table'><thead><tr><th>物料</th><th>数量</th><th>采购下达时间</th><th>提前期(天)</th>"
        "<th>预计到料时间</th><th>触发工序</th><th>关联订单</th></tr></thead>"
        f"<tbody>{tr}</tbody></table>"
    )
    return _wrap_table(table_html)

def _build_parent_child_rows(tasks, topo_order: dict[str,int], show_order: bool):
    by_parent=defaultdict(list)
    for t in tasks:
        by_parent[(t.get('order_code',''),t.get('route_code',''),t.get('step_code',''))].append(t)
    parent_keys=sorted(
        by_parent.keys(),
        key=lambda k: (k[0], topo_order.get(k[2], 9999), _safe_dt(min(x.get('planned_start','') for x in by_parent[k])), k[2]),
    )
    rows=[]
    for order_code,route_code,step_code in parent_keys:
        children=sorted(by_parent[(order_code,route_code,step_code)], key=lambda x: _safe_dt(x.get('planned_start','')))
        starts=[_safe_dt(x.get('planned_start','')) for x in children if x.get('planned_start')]
        ends=[_safe_dt(x.get('planned_end','')) for x in children if x.get('planned_end')]
        total_qty=q(sum((dec(x.get('planned_qty','0') or '0') for x in children), Decimal('0')))
        total_dur=sum(int(float(x.get('duration_min','0') or 0)) for x in children)
        block_codes=sorted({(x.get('execution_block_code','') or '').strip() for x in children if (x.get('execution_block_code','') or '').strip()})
        parent_cols=[
            "<td>父任务</td>",
            (f"<td>{escape(order_code)}</td>" if show_order else ""),
            f"<td>{escape(step_code)}</td>",
            f"<td>{len(children)}</td>",
            f"<td>{len(block_codes)}</td>",
            "<td>-</td><td>-</td>",
            f"<td>{_fmt_qty(total_qty)}</td>",
            f"<td>{starts[0].strftime('%Y-%m-%d %H:%M:%S') if starts else '-'}</td>",
            f"<td>{ends[-1].strftime('%Y-%m-%d %H:%M:%S') if ends else '-'}</td>",
            f"<td>{total_dur}</td>",
        ]
        rows.append(f"<tr class='parent-row dag-table-row' data-step-code='{_attr(step_code.lower())}'>{''.join(parent_cols)}</tr>")
        for i,ch in enumerate(children,1):
            child_cols=[
                "<td>子任务</td>",
                (f"<td>{escape(order_code)}</td>" if show_order else ""),
                f"<td>{escape(step_code)}#{i}</td>",
                "<td>-</td>",
                f"<td>{escape((ch.get('execution_block_code','') or '-'))}</td>",
                f"<td>{escape(ch.get('machine_code',''))}</td>",
                f"<td>{escape(ch.get('employee_code',''))}</td>",
                f"<td>{_fmt_qty(ch.get('planned_qty',''))}</td>",
                f"<td>{escape(ch.get('planned_start',''))}</td>",
                f"<td>{escape(ch.get('planned_end',''))}</td>",
                f"<td>{escape(ch.get('duration_min',''))}</td>",
            ]
            rows.append(f"<tr class='child-row dag-table-row' data-step-code='{_attr(step_code.lower())}'>{''.join(child_cols)}</tr>")
    return ''.join(rows) or f"<tr><td colspan='{'11' if show_order else '10'}'>无任务</td></tr>"

def _route_sink_step_codes(d) -> dict[str, set[str]]:
    succ_by_sid=defaultdict(set)
    for edges in d.get('route_edges',{}).values():
        for a,b in edges:
            succ_by_sid[a].add(b)
    out={}
    for rc,rid in d.get('route_id_by_code',{}).items():
        sink_codes=set()
        for sid in d.get('topo',{}).get(rid,[]):
            if not succ_by_sid.get(sid):
                sc=d.get('steps',{}).get(sid,{}).get('code','')
                if sc:
                    sink_codes.add(sc)
        out[rc]=sink_codes
    return out

def _build_order_display_stats(d, outcomes, tasks):
    mats=d.get('mats',{})
    order_req={}
    for o in d.get('orders',[]):
        oc=str(o.get('code',''))
        mat_code=mats.get(int(o.get('mat',0)) if str(o.get('mat','')).isdigit() else o.get('mat'),{}).get('code','')
        order_req[oc]={
            'requested_material_code': mat_code or '-',
            'requested_qty': o.get('qty',Decimal('0')),
        }
    by_order=defaultdict(list)
    for t in tasks:
        by_order[t.get('order_code','')].append(t)
    sink_by_route=_route_sink_step_codes(d)
    out={}
    for o in outcomes:
        oc=o.get('code','')
        rc=o.get('route','')
        rows=by_order.get(oc,[])
        starts=[_safe_dt(r.get('planned_start','')) for r in rows if r.get('planned_start')]
        starts=[x for x in starts if x != datetime.max]
        start_text=starts[0].strftime('%Y-%m-%d %H:%M') if starts else '-'
        sink_codes=sink_by_route.get(rc,set())
        sink_rows=[r for r in rows if r.get('step_code','') in sink_codes] if sink_codes else []
        qty_rows=sink_rows if sink_rows else []
        actual_qty=q(sum((dec(r.get('planned_qty','0') or '0') for r in qty_rows), Decimal('0'))) if qty_rows else Decimal('0')
        req=order_req.get(oc,{'requested_material_code':'-','requested_qty':Decimal('0')})
        out[oc]={
            'requested_material_code': req.get('requested_material_code','-'),
            'requested_qty': req.get('requested_qty',Decimal('0')),
            'actual_qty': actual_qty,
            'order_start': start_text,
        }
    return out

def _build_gantt_card(tasks, outcomes, nav: str) -> str:
    if not tasks:
        return f"<div class='card'><h2>甘特图</h2>{nav}<p class='muted'>无任务</p></div>"
    parsed=[]
    for t in tasks:
        try:
            st=pdt(t.get('planned_start',''))
            en=pdt(t.get('planned_end',''))
        except Exception:
            continue
        if en <= st:
            continue
        parsed.append({
            'order_code': t.get('order_code',''),
            'route_code': t.get('route_code',''),
            'step_code': t.get('step_code',''),
            'machine_code': t.get('machine_code',''),
            'employee_code': t.get('employee_code',''),
            'qty': _fmt_qty(t.get('planned_qty','0')),
            'start': st,
            'end': en,
        })
    if not parsed:
        return f"<div class='card'><h2>甘特图</h2>{nav}<p class='muted'>无有效任务时间窗</p></div>"
    t0=min(x['start'] for x in parsed)
    t1=max(x['end'] for x in parsed)
    span_min=max(60.0,(t1-t0).total_seconds()/60.0)
    timeline_w=max(1200,min(7200,int(span_min*0.55)))

    def _x(dt_obj: datetime) -> int:
        return int(max(0.0, min(1.0, (dt_obj-t0).total_seconds()/60.0/span_min))*timeline_w)

    hour_step=1 if span_min <= 72*60 else (4 if span_min <= 14*24*60 else (12 if span_min <= 40*24*60 else 24))
    day_cursor=datetime(t0.year,t0.month,t0.day,0,0,0)
    day_cells=[]
    while day_cursor < t1:
        seg_start=max(day_cursor,t0)
        seg_end=min(day_cursor+timedelta(days=1),t1)
        left=_x(seg_start); right=max(left+1,_x(seg_end))
        day_cells.append(
            f"<div class='gantt-scale-item' style='left:{left}px;width:{max(1,right-left)}px'>{seg_start:%m-%d}</div>"
        )
        day_cursor += timedelta(days=1)

    hour_base=t0.replace(minute=0,second=0,microsecond=0)
    if hour_step > 1:
        hour_base -= timedelta(hours=hour_base.hour % hour_step)
    hour_cells=[]
    hour_cursor=hour_base
    while hour_cursor < t1:
        seg_start=max(hour_cursor,t0)
        seg_end=min(hour_cursor+timedelta(hours=hour_step),t1)
        left=_x(seg_start); right=max(left+1,_x(seg_end))
        hour_cells.append(
            f"<div class='gantt-scale-item' style='left:{left}px;width:{max(1,right-left)}px'>{seg_start:%m-%d %H:%M}</div>"
        )
        hour_cursor += timedelta(hours=hour_step)

    lane_name=lambda r: f"{r['machine_code'] or '-'} | {r['employee_code'] or '-'}"
    lanes=sorted({lane_name(r) for r in parsed})
    lane_idx={k:i for i,k in enumerate(lanes)}
    late_orders={o.get('code','') for o in outcomes if float(o.get('delay',0.0) or 0.0) > 0}
    by_lane=defaultdict(list)
    for r in parsed:
        by_lane[lane_name(r)].append(r)

    row_html=[]
    for idx,lane in enumerate(lanes):
        row_id=f"lane-{idx}"
        bars=[]
        for r in sorted(by_lane.get(lane,[]), key=lambda x: (x['start'], x['end'], x['order_code'])):
            left=_x(r['start']); right=max(left+2,_x(r['end']))
            width=max(2,right-left)
            is_late=' is-late' if r['order_code'] in late_orders else ''
            title=(
                f"订单: {r['order_code']}\n路线: {r['route_code']}\n工序: {r['step_code']}\n"
                f"资源: {lane}\n数量: {r['qty']}\n开始: {r['start']:%Y-%m-%d %H:%M}\n结束: {r['end']:%Y-%m-%d %H:%M}"
            )
            search_text=' '.join([r['order_code'],r['route_code'],r['step_code'],r['machine_code'],r['employee_code']]).lower()
            bars.append(
                f"<div class='gantt-bar{is_late}' style='left:{left}px;width:{width}px' "
                f"title='{_attr(title)}' data-gantt-row='{_attr(row_id)}' data-gantt-search='{_attr(search_text)}'></div>"
            )
        row_html.append(
            f"<tr class='gantt-row' data-gantt-row='{_attr(row_id)}'>"
            f"<td class='gantt-lane-cell'>{escape(lane)}</td>"
            f"<td><div class='gantt-lane' style='width:{timeline_w}px'>{''.join(bars)}</div></td></tr>"
        )

    tools=(
        "<div class='gantt-tools table-tools'>"
        "<input class='field-input' type='search' data-gantt-search placeholder='筛选订单/路线/工序/资源（设备|员工）'/>"
        "<button type='button' class='btn btn-soft' data-gantt-clear>清空</button>"
        f"<span class='muted table-count' data-gantt-count>任务 {sum(len(v) for v in by_lane.values())}</span>"
        "</div>"
    )
    table=(
        "<div class='gantt-wrap'><table class='gantt-table'><thead>"
        "<tr><th class='gantt-lane-head'>资源泳道</th>"
        f"<th><div class='gantt-scale gantt-scale-day' style='width:{timeline_w}px'>{''.join(day_cells)}</div></th></tr>"
        "<tr><th class='gantt-lane-head'>时间轴</th>"
        f"<th><div class='gantt-scale gantt-scale-hour' style='width:{timeline_w}px'>{''.join(hour_cells)}</div></th></tr>"
        f"</thead><tbody>{''.join(row_html)}</tbody></table></div>"
    )
    return (
        "<div class='card' data-gantt-board><h2>甘特图（Frepple-Lite）</h2>"
        "<div class='muted'>左侧资源泳道，右侧双层时间轴（日/小时）；支持关键词高亮与命中统计。</div>"
        f"{nav}{tools}{table}</div>"
    )

def write_more_html(d,tasks,outcomes,purchases,problems,trace,failed_rows,execution_blocks=None):
    for old in OUT.glob("order_*.html"):
        old.unlink(missing_ok=True)
    for old in OUT.glob("route_*.html"):
        old.unlink(missing_ok=True)
    by_o=defaultdict(list); by_r=defaultdict(list); by_m=defaultdict(list)
    for t in tasks: by_o[t['order_code']].append(t); by_r[t['route_code']].append(t); by_m[t['machine_code']].append(t)
    blocks_by_code={(b.get('block_code') or '').strip():b for b in (execution_blocks or []) if (b.get('block_code') or '').strip()}
    nav=_schedule_nav(include_index=True)
    m_rows=[]
    for m,ts in sorted(by_m.items()):
        sts=[pdt(t['planned_start']) for t in ts]; ens=[pdt(t['planned_end']) for t in ts]; h=sum((e-s).total_seconds()/3600.0 for s,e in zip(sts,ens)); m_rows.append(f"<tr><td>{escape(m)}</td><td>{len(ts)}</td><td>{h:.2f}</td><td>{min(sts):%Y-%m-%d %H:%M}</td><td>{max(ens):%Y-%m-%d %H:%M}</td></tr>")
    machine_table=f"<table class='table'><thead><tr><th>设备</th><th>任务数</th><th>总工时(小时)</th><th>最早开始</th><th>最晚结束</th></tr></thead><tbody>{''.join(m_rows) if m_rows else '<tr><td colspan=5>无数据</td></tr>'}</tbody></table>"
    (OUT/'machines.html').write_text(shell('设备负载',f"<div class='card'><h2>设备负载</h2>{nav}{_wrap_table(machine_table)}</div>"),encoding='utf-8')
    p_rows=''.join([f"<tr><td>{escape(r.get('material_code',''))}</td><td>{_fmt_qty(r.get('purchase_qty',''))}</td><td>{escape(r.get('purchase_start_time',''))}</td><td>{escape(r.get('lead_time_days',''))}</td><td>{escape(r.get('expected_ready_time',''))}</td><td>{escape(r.get('ref_orders',''))}</td><td>{escape(r.get('ref_steps',''))}</td></tr>" for r in purchases]) or "<tr><td colspan='7'>无采购需求</td></tr>"
    purchase_table=f"<table class='table'><thead><tr><th>物料</th><th>数量</th><th>采购下达时间</th><th>提前期(天)</th><th>预计到料时间</th><th>订单</th><th>触发工序</th></tr></thead><tbody>{p_rows}</tbody></table>"
    (OUT/'purchases.html').write_text(shell('采购计划',f"<div class='card'><h2>采购计划</h2>{nav}{_wrap_table(purchase_table)}</div>"),encoding='utf-8')
    pr_rows=''.join([f"<tr><td>{escape(r.get('order_code',''))}</td><td>{escape(r.get('route_code',''))}</td><td>{escape(r.get('step_code',''))}</td><td>{escape(r.get('problem_type',''))}</td><td>{escape(r.get('severity',''))}</td><td>{escape(r.get('start',''))}</td><td>{escape(r.get('end',''))}</td><td>{escape(r.get('description',''))}</td><td>{escape(r.get('analysis',''))}</td><td>{escape(r.get('suggestion',''))}</td></tr>" for r in problems]) or "<tr><td colspan='10'>无问题</td></tr>"
    problem_table=f"<table class='table' id='problems-table'><thead><tr><th>订单</th><th>路线</th><th>工序</th><th>问题类型</th><th>级别</th><th>开始</th><th>结束</th><th>描述</th><th>分析</th><th>建议</th></tr></thead><tbody>{pr_rows}</tbody></table>"
    (OUT/'problems.html').write_text(
        shell('问题诊断',f"<div class='card'><h2>问题诊断</h2>{nav}{_wrap_table(problem_table, table_id='problems-table', placeholder='筛选订单/路线/工序/问题类型')}</div>"),
        encoding='utf-8',
    )
    t_rows=''.join([f"<tr><td>{escape(r.get('event_time',''))}</td><td>{escape(r.get('order_code',''))}</td><td>{escape(r.get('route_code',''))}</td><td>{escape(r.get('step_code',''))}</td><td>{escape(r.get('event_type',''))}</td><td>{escape(r.get('material_code',''))}</td><td>{escape(r.get('machine_code',''))}</td><td>{escape(r.get('employee_code',''))}</td><td>{_fmt_qty(r.get('qty',''))}</td><td>{escape(_fmt_msg_numbers(r.get('message','')))}</td></tr>" for r in trace]) or "<tr><td colspan='10'>无事件</td></tr>"
    trace_table=f"<table class='table' id='trace-table'><thead><tr><th>时间</th><th>订单</th><th>路线</th><th>工序</th><th>事件</th><th>物料</th><th>设备</th><th>员工</th><th>数量</th><th>消息</th></tr></thead><tbody>{t_rows}</tbody></table>"
    (OUT/'trace.html').write_text(
        shell('事件追踪',f"<div class='card'><h2>事件追踪</h2>{nav}{_wrap_table(trace_table, table_id='trace-table', placeholder='筛选事件类型/订单/工序/消息')}</div>"),
        encoding='utf-8',
    )
    machine_to_mt={v['code']:v.get('mt_code','') for v in d.get('mac_info',{}).values()}
    purchase_events=_extract_purchase_events(trace)
    failed_by_order=defaultdict(set)
    for r in failed_rows:
        if r.get('order_code') and r.get('step_code'): failed_by_order[r['order_code']].add(r['step_code'])
    topo_code_order={}
    for rc,rid in d.get('route_id_by_code',{}).items():
        topo_code_order[rc]={d['steps'][sid]['code']:idx for idx,sid in enumerate(d['topo'].get(rid,[]))}

    for o in outcomes:
        route_code=o.get('route','')
        route_id=d.get('route_id_by_code',{}).get(route_code)
        base_route_link = (
            f"<a href='../planning_viz/route_{route_id}.html' target='_blank'>查看基础工艺路线页面</a>"
            if route_id is not None else "无基础路线页面"
        )
        ts=sorted(by_o.get(o['code'],[]),key=lambda x:x.get('planned_start',''))
        stats=_aggregate_step_stats(ts,machine_to_mt,blocks_by_code=blocks_by_code)
        failed_steps=failed_by_order.get(o['code'],set())
        dag=_render_route_dag_svg(d,route_code,stats,failed_steps) if route_code and route_code!='-' else "<p class='muted'>该订单无可用工艺路线。</p>"
        order_purchase=[x for x in purchase_events if x.get('order_code')==o['code']]
        parent_child_rows=_build_parent_child_rows(ts, topo_code_order.get(route_code,{}), show_order=False)
        delay_text = _fmt_qty(f"{float(o.get('delay', 0.0) or 0.0):.1f}")
        order_table_id = "order-detail-" + re.sub(r"[^0-9A-Za-z_-]", "-", str(o.get('code', '')))
        order_detail_table = _wrap_table(
            f"<table class='table' id='{order_table_id}'><thead><tr><th>层级</th><th>工序</th><th>子任务数</th><th>执行块</th><th>设备</th><th>员工</th><th>数量</th><th>开始</th><th>结束</th><th>时长(分钟)</th></tr></thead><tbody>{parent_child_rows}</tbody></table>",
            table_id=order_table_id,
            placeholder='筛选工序/设备/员工/执行块',
        )
        dag_card = (
            "<div class='card' data-dag-panel><h2>工艺路线DAG</h2><div class='muted'>节点展示执行方式、设备、任务拆分与子任务时间窗。支持工序搜索联动与节点折叠。</div>"
            "<div class='dag-tools'>"
            "<input class='field-input' type='search' data-dag-search placeholder='搜索工序编码/名称并联动高亮节点与任务明细'/>"
            "<button type='button' class='btn btn-soft' data-dag-clear>清空</button>"
            "<button type='button' class='btn' data-dag-toggle>展开节点详情</button>"
            "<span class='muted table-count' data-dag-count>节点 0</span>"
            "</div>"
            f"<div class='dag-wrap'>{dag}</div></div>"
        )
        body=(
            f"<div class='card'><h2>订单路线详情：<span class='code'>{escape(o['code'])}</span></h2>{nav}"
            f"<p>工艺路线：<span class='code'>{escape(route_code or '-')}</span> | 结果：{'已排产' if o['result']=='planned' else '失败'} | 截止时间：{o['due']:%Y-%m-%d %H:%M} | 最晚完成：{o['finish'].strftime('%Y-%m-%d %H:%M') if o['finish'] else '-'} | 逾期分钟：{delay_text}</p>"
            f"<p class='muted'>{base_route_link}</p>"
            f"<p class='muted'>{escape(o['msg'])}</p></div>"
            f"{dag_card}"
            f"<div class='card'><h2>采购记录</h2>{_render_purchase_table(order_purchase)}</div>"
            "<div class='card'><h2>任务明细（父子任务）</h2>"
            "<div class='muted'>父任务为同订单同工序的聚合行（无独立执行意义），子任务为每次执行分段。</div>"
            f"{order_detail_table}</div>"
        )
        (OUT/f"order_{o['code']}.html").write_text(shell(f"订单_{o['code']}",body),encoding='utf-8')

    g=_build_gantt_card(tasks, outcomes, nav)
    (OUT/'gantt.html').write_text(shell('甘特图',g),encoding='utf-8')
    fr_rows=[]
    for r in failed_rows:
        loc='.'.join([x for x in [str(r.get('route_code','')).strip(),str(r.get('step_code','')).strip()] if x]) or '-'
        fr_rows.append(
            f"<tr><td>{escape(r.get('order_code',''))}</td><td>{escape(r.get('priority',''))}</td><td>{escape(r.get('due_time',''))}</td>"
            f"<td>{escape(loc)}</td><td>{escape(r.get('problem_type',''))}</td><td>{escape(r.get('window_start',''))}</td><td>{escape(r.get('window_end',''))}</td>"
            f"<td>{escape(r.get('latest_end_target',''))}</td><td>{escape(r.get('material_ready',''))}</td><td>{escape(r.get('attempt_window',''))}</td>"
            f"<td>{escape(r.get('reason',''))}</td><td>{escape(r.get('analysis',''))}</td><td>{escape(r.get('suggestion',''))}</td><td>{escape(r.get('resource_snapshot',''))}</td>"
            f"<td>{escape(r.get('lateness_penalty',''))}</td><td>{escape(r.get('penalty_basis',''))}</td></tr>"
        )
    fr=''.join(fr_rows) or "<tr><td colspan='16'>无失败订单</td></tr>"
    failed_table=f"<table class='table' id='failed-orders-table'><thead><tr><th>订单</th><th>优先级</th><th>截止时间</th><th>报错位置</th><th>问题类型</th><th>窗口开始</th><th>窗口结束</th><th>latest_end目标</th><th>物料可用时间</th><th>尝试窗口</th><th>原因</th><th>分析</th><th>建议</th><th>资源快照</th><th>逾期惩罚值</th><th>逾期分配依据</th></tr></thead><tbody>{fr}</tbody></table>"
    (OUT/'failed_orders.html').write_text(
        shell('失败分析',f"<div class='card'><h2>失败订单分析</h2>{nav}{_wrap_table(failed_table, table_id='failed-orders-table', placeholder='筛选订单/报错位置/问题类型/原因')}</div>"),
        encoding='utf-8',
    )

def sync_schedule_tasks(tasks, execution_blocks):
    con=sqlite3.connect(str(DB))
    con.row_factory=sqlite3.Row
    try:
        con.execute("PRAGMA foreign_keys = ON")
        order_by_code={str(r['code']):int(r['id']) for r in con.execute("select id,code from orders").fetchall()}
        route_by_code={str(r['code']):int(r['id']) for r in con.execute("select id,code from process_routes").fetchall()}
        step_by_key={(str(r['route_code']),str(r['step_code'])):int(r['id']) for r in con.execute("select rs.id, pr.code as route_code, rs.code as step_code from route_steps rs join process_routes pr on pr.id=rs.route_id").fetchall()}
        machine_by_code={str(r['code']):int(r['id']) for r in con.execute("select id,code from machines").fetchall()}
        employee_by_code={str(r['code']):int(r['id']) for r in con.execute("select id,code from employees").fetchall()}

        valid_status={"PENDING","PLANNED","RUNNING","DONE","CANCELLED"}
        block_rows=[]
        for b in execution_blocks:
            route_code=str(b.get('route_code',''))
            step_code=str(b.get('step_code',''))
            key=(route_code,step_code)
            if route_code not in route_by_code or key not in step_by_key:
                continue
            mcode=str(b.get('machine_code','')).strip()
            ecode=str(b.get('employee_code','')).strip()
            block_rows.append((
                str(b.get('block_code','')),
                str(b.get('merge_type','none') or 'none'),
                route_by_code[route_code],
                step_by_key[key],
                machine_by_code.get(mcode),
                employee_by_code.get(ecode),
                str(b.get('total_qty','0')),
                fdt(b.get('planned_start')) if isinstance(b.get('planned_start'),datetime) else (str(b.get('planned_start','')) or None),
                fdt(b.get('planned_end')) if isinstance(b.get('planned_end'),datetime) else (str(b.get('planned_end','')) or None),
                int(b.get('duration_min',0) or 0),
                int(b.get('source_task_count',1) or 1),
                str(b.get('note','') or '') or None,
            ))
        rows=[]
        for t in tasks:
            order_code=str(t.get('order_code',''))
            route_code=str(t.get('route_code',''))
            step_code=str(t.get('step_code',''))
            key=(route_code,step_code)
            if order_code not in order_by_code:
                raise KeyError(f"schedule_tasks回写失败：订单不存在 {order_code}")
            if route_code not in route_by_code:
                raise KeyError(f"schedule_tasks回写失败：路线不存在 {route_code}")
            if key not in step_by_key:
                raise KeyError(f"schedule_tasks回写失败：工序不存在 {route_code}.{step_code}")
            mcode=str(t.get('machine_code','')).strip()
            ecode=str(t.get('employee_code','')).strip()
            status=str(t.get('status','planned')).strip().upper()
            if status not in valid_status:
                status="PLANNED"
            rows.append((
                order_by_code[order_code],
                route_by_code[route_code],
                step_by_key[key],
                machine_by_code.get(mcode),
                employee_by_code.get(ecode),
                str(t.get('execution_block_code','') or ''),
                str(t.get('planned_qty','0')),
                str(t.get('planned_start','')) or None,
                str(t.get('planned_end','')) or None,
                status,
            ))
        with con:
            con.execute("delete from schedule_tasks")
            con.execute("delete from schedule_execution_blocks")
            con.executemany(
                "insert into schedule_execution_blocks(block_code,merge_type,route_id,step_id,machine_id,employee_id,total_qty,planned_start,planned_end,duration_min,source_task_count,note) values (?,?,?,?,?,?,?,?,?,?,?,?)",
                block_rows,
            )
            block_id_by_code={str(r['block_code']):int(r['id']) for r in con.execute("select id,block_code from schedule_execution_blocks").fetchall()}
            rows_with_block=[(
                x[0],x[1],x[2],x[3],x[4],
                block_id_by_code.get(x[5]) if x[5] else None,
                x[6],x[7],x[8],x[9]
            ) for x in rows]
            con.executemany(
                "insert into schedule_tasks(order_id,route_id,step_id,machine_id,employee_id,execution_block_id,planned_qty,planned_start,planned_end,status) values (?,?,?,?,?,?,?,?,?,?)",
                rows_with_block,
            )
        return len(rows), ""
    except Exception as e:
        con.rollback()
        return 0, str(e)
    finally:
        con.close()

def _parse_args():
    ap = argparse.ArgumentParser(description='Generate schedule and HTML reports from DB.')
    ap.add_argument('--verbose', action='store_true', help='显示详细调试日志（默认关闭）')
    return ap.parse_args()

def _stage_start(stage_idx: int, stage_name: str, extra: str = ''):
    _progress_write(
        f"阶段开始 | 阶段={stage_idx}/6 | 名称={stage_name}" + (f" | 说明={extra}" if extra else '')
    )

def _stage_done(stage_idx: int, stage_name: str, stage_started: datetime, run_started: datetime, extra: str = ''):
    stage_cost = (datetime.now() - stage_started).total_seconds() / 60.0
    total_cost = (datetime.now() - run_started).total_seconds() / 60.0
    _progress_write(
        f"阶段完成 | 阶段={stage_idx}/6 | 名称={stage_name} | 阶段耗时={stage_cost:.1f}m | 累计={total_cost:.1f}m" + (f" | 说明={extra}" if extra else ''),
        level='success'
    )

def main():
    global VERBOSE
    args = _parse_args()
    VERBOSE = bool(args.verbose)
    log_dir = _setup_logging(VERBOSE)
    run_started = datetime.now()
    _progress_write(f"脚本启动 | 输出目录={OUT} | 日志目录={log_dir} | verbose={VERBOSE}")

    _stage_start(1, '数据库读取')
    s = datetime.now()
    d=read_db()
    _stage_done(1, '数据库读取', s, run_started, extra=f"orders={len(d.get('orders',[]))}")

    _stage_start(2, '订单求解')
    s = datetime.now()
    core=Core(d); core.solve()
    _stage_done(2, '订单求解', s, run_started, extra=f"outcomes={len(core.outcomes)} tasks={len(core.tasks)}")

    _stage_start(3, '准备输出目录', extra='清理旧结果')
    s = datetime.now()
    OUT.mkdir(parents=True,exist_ok=True)
    _stage_done(3, '准备输出目录', s, run_started)

    legacy = [
        'backward_trace.csv',
        'purchase_plan.csv',
        'problem_report.csv',
        'failed_analysis.csv',
        'schedule_score.csv',
        'clean_resources.csv',
        'clean_employee_weekly_calendars.csv',
        'clean_employee_calendars.csv',
        'clean_resources.html',
    ]
    for fn in legacy:
        fp = OUT / fn
        if fp.exists():
            fp.unlink()

    _stage_start(4, '后处理与执行块')
    s = datetime.now()
    tasks=sorted(core.tasks,key=lambda r:(r.get('planned_start',''),r.get('machine_code',''),r.get('order_code',''),r.get('step_code','')))
    merge_info=dict(core.merge_stats)
    _recompute_outcome_finish(d, core.outcomes, tasks)
    execution_blocks=_build_execution_blocks(d, tasks, show_progress=False)
    _stage_done(4, '后处理与执行块', s, run_started, extra=f"merged_rows={merge_info.get('merged_rows',0)} execution_blocks={len(execution_blocks)}")

    _stage_start(5, '回写数据库', extra='同步 schedule_tasks')
    s = datetime.now()
    trace=sorted(core.trace,key=lambda r:(r.get('event_time',''),r.get('order_code',''),r.get('event_type','')))
    probs=sorted(core.problems,key=lambda r:(r.get('severity',''),r.get('entity_type',''),r.get('entity_code','')))
    failed_rows=sorted(core.failed,key=lambda r:(r.get('priority',''),r.get('due_time',''),r.get('order_code','')))
    purchases=core.purchase_rows(); scores=core.scores()
    synced_rows, sync_error = sync_schedule_tasks(tasks, execution_blocks)
    _stage_done(5, '回写数据库', s, run_started, extra=f"rows={synced_rows if not sync_error else 0}")

    _stage_start(6, '生成HTML')
    s = datetime.now()
    if sync_error:
        _progress_write(f"数据库同步失败 | 错误={sync_error}", level='error')
    else:
        _progress_write(f"数据库同步完成 | rows={synced_rows}", level='success')
        _progress_write(f"执行块整理 | merged_rows={merge_info.get('merged_rows',0)} | execution_blocks={len(execution_blocks)}", level='info')
    write_html(d,tasks,core.outcomes,purchases,probs,trace,scores,sync_error=sync_error)
    write_more_html(d,tasks,core.outcomes,purchases,probs,trace,failed_rows,execution_blocks=execution_blocks)
    _stage_done(6, '生成HTML', s, run_started)

    overall=next((r.get('score_0_100','') for r in scores if r.get('metric')=='overall_score'),'')
    _progress_write(f"排产完成 | 输出目录={OUT} | 订单={len(core.outcomes)} | 任务={len(tasks)}", level='success')
    _progress_write(f"结果汇总 | 问题={len(probs)} | 评分={overall or 'n/a'} | 采购={len(purchases)} | 失败={len(failed_rows)}", level='success')

if __name__=='__main__':
    main()
