from __future__ import annotations
import json
from pathlib import Path
from typing import Callable

from api.orchestrator import DAG, Runner, FlowState, _dump_json
from api.orchestrator import MakeTradesTask, MakeCptiesTask
from api.config import Config

# 1) registry: map JSON "type" -> Task class constructor
TASK_REGISTRY: dict[str, Callable[..., object]] = {
    "MakeTradesTask": MakeTradesTask,
    "MakeCptiesTask": MakeCptiesTask,
    # "LoadTradesTask": LoadTradesTask, etc.
}
 
def load_graph_from_json(path: str | Path) -> dict:
    data = json.loads(Path(path).read_text())
    # (optional) validate with jsonschema.validate(data, GRAPH_SCHEMA)
    return data

def build_dag_from_dict(spec: dict) -> tuple[DAG, dict[str, object]]:
    defaults = spec.get("defaults", {})
    dag = DAG()

    # add tasks
    for t in spec["tasks"]:
        tid   = t["id"]
        ttype = t["type"]
        ctor  = TASK_REGISTRY.get(ttype)
        if ctor is None:
            raise ValueError(f"Unknown task type: {ttype}")
        task = ctor(tid,
                    max_retries=t.get("max_retries", defaults.get("max_retries", 0)),
                    retry_sleep_s=t.get("retry_sleep_s", defaults.get("retry_sleep_s", 1.0)))
        # if your Task subclasses accept params in __init__, wire them here:
        # task = ctor(tid, **t.get("params", {}), max_retries=..., retry_sleep_s=...)
        dag.add_task(task)

    # add edges
    for u, v in spec["edges"]:
        dag.add_edge(u, v)

    # initial context
    ctx: dict[str, object] = dict(spec.get("context", {}))
    return dag, ctx

def start_runner_from_file(path: str | Path) -> Runner:
    spec = load_graph_from_json(path)
    dag, ctx = build_dag_from_dict(spec)
    flow_id = spec.get("name") or "flow"
    # make unique; keep human-readable prefix
    import uuid
    flow_id = f"{flow_id}-{uuid.uuid4()}"
    runner = Runner(dag, flow_id)
    _dump_json(runner.manifest, FlowState(flow_id=flow_id, status="accepted", context=ctx).as_dict())
    return runner
