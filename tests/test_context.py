import pytest
from api.orchestrator import DAG, Task, Runner, Context
from tests.utils import MockTask
from api.config import Config
import json
from pathlib import Path

"""Ensures your typed accessor always works and prevents type bugs down the road."""

def test_context_get_t_success():
    ctx = Context({"path": Path("/tmp/file.txt")})
    p = ctx.get_t("path", Path)
    assert isinstance(p, Path)

def test_context_get_t_wrong_type():
    ctx = Context({"count": "not_an_int"})
    with pytest.raises(TypeError):
        ctx.get_t("count", int)
