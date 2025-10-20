import pytest
from api.orchestrator import DAG, Runner
from tests.utils import MockTask
from api.config import Config
import json

"""Test that manifests and logs are correctly written."""

def test_flowstate_saves_manifest(tmp_path):
    dag = DAG()
    t = MockTask("A")
    dag.add_task(t)
    runner = Runner(dag, "manifest_test")
    state = runner.run()

    # Manifest exists
    manifest_path = runner.manifest
    assert manifest_path.exists()

    data = json.loads(manifest_path.read_text())
    assert data["status"] in ("succeeded", "failed")
    assert "updated_at" in data

def test_runner_creates_unique_run_dirs(tmp_path):
    dag = DAG()
    t = MockTask("A")
    dag.add_task(t)

    r1 = Runner(dag, "run_1")
    r2 = Runner(dag, "run_2")
    s1 = r1.run()
    s2 = r2.run()

    assert r1.manifest != r2.manifest
    assert r1.manifest.exists()
    assert r2.manifest.exists()
