"""Pytest 共用 fixture。"""
import os
import sys
import tempfile
from pathlib import Path

import pytest

# 確保 tests 能 import main.py
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def tmp_db(monkeypatch):
    """每個測試用獨立的 SQLite 臨時 DB；測試結束 monkeypatch 會還原 DB_PATH。"""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    monkeypatch.setenv("DB_PATH", tmp.name)
    # 延遲 import 以吃到 DB_PATH env
    import importlib
    import main
    importlib.reload(main)
    main.init_db()
    yield main, tmp.name
    try:
        os.unlink(tmp.name)
    except OSError:
        pass
