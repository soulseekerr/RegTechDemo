import pytest
from api.orchestrator import DAG, Runner
from tests.utils import MockTask

""" hooks (on_start, on_success, on_fail, on_finish) deserve explicit checks."""

class HookedTask(MockTask):
    def on_start(self, ctx): ctx["start"] = True
    def on_success(self, ctx, updates): ctx["success"] = True
    def on_fail(self, ctx, error): ctx["fail"] = str(error)
    def on_finish(self, ctx): ctx["finish"] = True

def test_task_hooks_called():
    dag = DAG()
    t = HookedTask("A")
    dag.add_task(t)
    runner = Runner(dag, "hooks_test")
    state = runner.run()

    assert "start" in state.context
    assert "success" in state.context
    assert "finish" in state.context
