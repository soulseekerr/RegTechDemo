import pytest

import sys
from pathlib import Path

# Ensure project root (where api/ lives) is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    print("Adding project root to sys.path:", ROOT)
    sys.path.insert(0, str(ROOT))

from api.orchestrator import DAG, Context
from tests.utils import MockTask

@pytest.fixture
def simple_dag() -> DAG:
    dag = DAG()
    dag.add_task(MockTask("A"))
    dag.add_task(MockTask("B"))
    dag.add_edge("A", "B")
    return dag

@pytest.fixture
def ctx() -> Context:
    return Context({"foo": "bar"})
