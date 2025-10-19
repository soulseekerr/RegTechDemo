import pytest
from api.orchestrator import DAG, Task, Runner
from tests.utils import MockTask
from api.config import Config
import json

def test_runner_single_success(tmp_path):
    print('Running test_runner_single_success')
    dag = DAG()
    t = MockTask("A")
    dag.add_task(t)
    flow_id = "flow1"
    runner = Runner(dag, flow_id)
    result = runner.run()
    assert result.status == "succeeded"
    assert "A" in result.context
    # assert True

def test_topo_order_linear():
    dag = DAG()
    dag.add_task(MockTask("A"))
    dag.add_task(MockTask("B"))
    dag.add_edge("A", "B")
    assert dag.topo_order() == ["A", "B"]

def test_runner_retries_until_success():
    dag = DAG()
    t = MockTask("A", should_fail=True, max_retries=2)
    dag.add_task(t)
    flow_id = "retryflow"
    runner = Runner(dag, flow_id)
    res = runner.run()
    assert res.status == "succeeded"
    assert t.runs == 3  # first 2 fail, then succeed

def test_runner_failure_after_retries():
    dag = DAG()
    t = MockTask("A", should_fail=True, max_retries=1, permanent_fail=True)
    dag.add_task(t)

    runner = Runner(dag, "fail_test")
    state = runner.run()

    assert state.status == "failed"
    assert state.task_states["A"]["status"] == "failed"

def test_runner_respects_dependencies(tmp_path):
    dag = DAG()
    t1 = MockTask("A")
    t2 = MockTask("B")
    dag.add_task(t1)
    dag.add_task(t2)
    dag.add_edge("A", "B")
    runner = Runner(dag, "orderflow")
    res = runner.run()
    assert res.status == "succeeded"
    assert res.context["A"].startswith("done")
    assert res.context["B"].startswith("done")
    assert list(res.task_states) == ["A", "B"]

def test_runner_writes_manifest_and_log(tmp_path):
    dag = DAG()
    dag.add_task(MockTask("A"))
    flow_id = "manifestflow"
    runner = Runner(dag, flow_id)
    res = runner.run()
    manifest = (Config.FLOWS_DIR / flow_id / "manifest.json")
    log = (Config.FLOWS_DIR / flow_id / "flow.log")
    assert manifest.exists()
    assert log.exists()
    data = json.loads(manifest.read_text())
    assert data["status"] in ("succeeded", "failed")

def test_build_dag_from_dict(tmp_path):
    spec = {
        "version": 1,
        "tasks": [{"id": "A", "type": "MockTask"},
                  {"id": "B", "type": "MockTask"}],
        "edges": [["A", "B"]],
        "context": {"foo": 123}
    }
    from api.graphloader import TASK_REGISTRY, build_dag_from_dict
    TASK_REGISTRY["MockTask"] = MockTask
    dag, ctx = build_dag_from_dict(spec)
    assert set(dag.tasks) == {"A", "B"}
    assert ctx["foo"] == 123

def test_unknown_task_type_raises():
    from api.graphloader import build_dag_from_dict
    spec = {"version": 1, "tasks": [{"id": "A", "type": "Nope"}], "edges": []}
    with pytest.raises(ValueError, match="Unknown task type"):
        build_dag_from_dict(spec)

def test_start_runner_from_file(tmp_path, monkeypatch):
    f = tmp_path / "graph.json"
    spec = {
        "version": 1,
        "tasks": [{"id": "A", "type": "MockTask"}],
        "edges": [],
        "context": {}
    }
    f.write_text(json.dumps(spec))
    from api.graphloader import TASK_REGISTRY, start_runner_from_file
    TASK_REGISTRY["MockTask"] = MockTask
    runner = start_runner_from_file(f)
    res = runner.run()
    assert res.status == "succeeded"
