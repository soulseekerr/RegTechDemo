from abc import ABC, abstractmethod
from typing import Type, TypeVar, cast
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
import json
import time
import uuid
import traceback

from api.config import Config
from api.tradebuilder import TradeBuilder
from api.cptybuilder import CptyBuilder

# placeholder for some concrete type that will be inferred later
T = TypeVar('T')

class Context(dict[str, object]):
    """
    Context dictionary passed to tasks during execution.
    """
    def get_t(self, key: str, typ: Type[T]) -> T:
        val = self[key]
        if not isinstance(val, typ):
            raise TypeError(f"Expected type {typ} for key '{key}', got {type(val)}")
        return cast(T, val)

class Task(ABC):
    """
    Abstract base class for orchestrator tasks.
    """
    name: str
    max_retries: int = 0
    retry_sleep_s: float = 1.0

    def __init__(self, name: str, *, max_retries: int = 0, retry_sleep_s: float = 1.0) -> None:
        self.name = name
        self.max_retries = max_retries
        self.retry_sleep_s = retry_sleep_s
    
    @abstractmethod
    def run(self, ctx: Context) -> dict[str, object]:
        pass

    def on_start(self, ctx: Context) -> None:
        """ Called before task execution. """
        pass

    def on_finish(self, ctx: Context) -> None:
        """ Called after task execution, regardless of success or failure. """
        pass
    
    def on_fail(self, ctx: Context, error: Exception) -> None:
        """ Called when task execution fails. """
        pass
    
    def on_success(self, ctx: Context, updates: dict[str, object]) -> None:
        """ Called when task execution succeeds. """
        pass

@dataclass
class DAG:
    """
    Directed Acyclic Graph of tasks for orchestration.
    """
    tasks: dict[str, Task] = field(default_factory=dict)
    edges: dict[str, list[str]] = field(default_factory=dict)
    
    def add_task(self, task: Task) -> "DAG":
        if task.name in self.tasks:
            raise ValueError(f"Task already exists: {task.name}")
        self.tasks[task.name] = task
        self.edges.setdefault(task.name, [])
        return self

    def add_edge(self, upstream: str, downstream: str) -> "DAG":
        if upstream not in self.tasks or downstream not in self.tasks:
            raise KeyError("Add tasks before edges.")
        self.edges.setdefault(upstream, []).append(downstream)
        return self
    
    def topo_order(self) -> list[str]:
        """
            Ensure we execute tasks in a valid order that respects all dependencies.

            Kahn algorithm:
            It processes the graph layer by layer:
            1. Start with all nodes that have in-degree = 0.
            2. Remove one of those nodes from the graph (it is “done”).
            3. Decrease the in-degree of its dependents.
            4. If any dependent is in-degree becomes 0, add it to the queue.
            5. Repeat until all nodes are processed.
            If at the end not all nodes are processed → there is a cycle.
        """
        indeg = {n: 0 for n in self.tasks}
        for u, outs in self.edges.items():
            for v in outs:
                indeg[v] += 1
        q = [n for n, d in indeg.items() if d == 0]
        order: list[str] = []
        while q:
            n = q.pop()
            order.append(n)
            for v in self.edges.get(n, []):
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
        if len(order) != len(self.tasks):
            raise ValueError("Cycle detected in DAG.")
        return order

def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

@dataclass
class FlowState:
    """Represents the state of a flow execution."""
    flow_id: str
    status: str = "accepted"  # accepted|running|succeeded|failed
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
    context: dict[str, object] = field(default_factory=dict)
    task_states: dict[str, dict[str, object]] = field(default_factory=dict)

    def as_dict(self) -> dict:
        d = asdict(self)
        d["updated_at"] = _now_iso()
        return d

def _dump_json(p: Path, obj: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2))

def _read_json(p: Path) -> dict:
    return json.loads(p.read_text())

class Runner:
    """
    Orchestrator runner to execute DAGs.
    """
    def __init__(self, dag: DAG, flow_id: str):
        self.dag = dag
        self.flow_id = flow_id
        self.dir = Config.FLOWS_DIR / flow_id
        self.dir.mkdir(parents=True, exist_ok=True)
        self.manifest = self.dir / "manifest.json"
        self.log = self.dir / "flow.log"
        self.dir.mkdir(parents=True, exist_ok=True)

    def _log(self, msg: str) -> None:
        with self.log.open("a") as f:
            f.write(f"[{_now_iso()}] {msg}\n")

    def _save(self, state: FlowState) -> None:
        _dump_json(self.manifest, state.as_dict())

    def run(self, initial_ctx: dict[str, object] | None = None) -> FlowState:
        ctx = Context(initial_ctx or {})
        state = FlowState(flow_id=self.flow_id, status="running", context=dict(ctx))
        self._save(state); self._log("Flow started.")
        
        try:
            order = self.dag.topo_order()
            self._log(f"Topo order: {order}")

            # Build prereq map
            prereqs: dict[str, list[str]] = {n: set() for n in self.dag.tasks}
            for u, outs in self.dag.edges.items():
                for v in outs:
                    prereqs[v].add(u)

            done: set[str] = set()
            for name in order:
                missing = prereqs[name] - done
                if missing:
                    raise RuntimeError(f"Task {name} prerequisites not satisfied: {missing}")

                task = self.dag.tasks[name]
                task.on_start(ctx)
                tstate: dict[str, object] = {"status": "running", "started_at": _now_iso(), "retries": 0}
                state.task_states[name] = tstate
                self._save(state); self._log(f"Task START {name}")

                attempt = 0
                while True:
                    try:
                        updates = task.run(ctx) or {}
                        # merge updates into context
                        for k, v in updates.items():
                            ctx[k] = v
                        tstate.update({"status": "succeeded", "finished_at": _now_iso(), "updates": updates})
                        task.on_success(ctx, updates)
                        self._log(f"Task OK   {name}")
                        break
                    except Exception as e:
                        attempt += 1
                        tstate["retries"] = attempt
                        task.on_fail(ctx, e)
                        self._log(f"Task FAIL {name}: {e}\n{traceback.format_exc()}")
                        if attempt > task.max_retries:
                            tstate.update({
                                "status": "failed", 
                                "finished_at": _now_iso(), 
                                "error": str(e)
                            })
                            # mark flow failed but don't raise
                            state.status = "failed"
                            self._save(state)
                            return state   # ← graceful exit instead of raise
                        time.sleep(task.retry_sleep_s)
                    finally:
                        task.on_finish(ctx)
                
                done.add(name)
                state.context = dict(ctx)
                self._save(state)

            state.status = "succeeded"
            self._save(state); self._log("Flow SUCCEEDED.")
            return state

        except Exception as e:
            state.status = "failed"
            self.context = dict(ctx)
            self._save(state); self._log(f"Flow FAILED: {e}\n{traceback.format_exc()}")
            return state

class MakeTradesTask(Task):
    def __init__(self, name: str, *, max_retries: int = 0, retry_sleep_s: float = 1.0):
        super().__init__(name, max_retries=max_retries, retry_sleep_s=retry_sleep_s)

    def run(self, ctx: Context) -> dict[str, object]:
        cob_dt  = ctx.get_t("cob_dt", date)
        rows    = int(ctx.get("trades_rows", 1_000_000))
        chunk   = int(ctx.get("trades_chunk", 100_000))

        tb = TradeBuilder(cob_dt, str(uuid.uuid4()))
        m  = tb.build(rows, chunk)  # returns a dict manifest
        if m.get("status") != "succeeded":
            raise RuntimeError(f"make_trades failed: {m}")

        return {
            "trades": {
                "run_id": tb.run_id,
                "manifest_path": Path(tb.manifest),   # store typed objects if you like
                "parquet_path": Path(tb.parquet),
                "rows": int(m.get("rows", 0)),
                "duration_s": float(m.get("duration_s", 0.0)),
            }
        }

class MakeCptiesTask(Task):
    def __init__(self, name: str, *, max_retries: int = 0, retry_sleep_s: float = 1.0):
        super().__init__(name, max_retries=max_retries, retry_sleep_s=retry_sleep_s)

    def run(self, ctx: Context) -> dict[str, object]:
        cob_dt  = ctx.get_t("cob_dt", date)
        rows    = int(ctx.get("cpty_rows", 100_000))
        chunk   = int(ctx.get("cpty_chunk", 10_000))

        cb = CptyBuilder(cob_dt, str(uuid.uuid4()))
        m  = cb.build(rows, chunk)
        if m.get("status") != "succeeded":
            raise RuntimeError(f"make_cpties failed: {m}")

        return {
            "cpties": {
                "run_id": cb.run_id,
                "manifest_path": Path(cb.manifest),
                "parquet_path": Path(cb.parquet),
                "rows": int(m.get("rows", 0)),
                "duration_s": float(m.get("duration_s", 0.0)),
            }
        }