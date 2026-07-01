import json
import sys
import tempfile
from pathlib import Path

import pytest

# Make the dags module importable without Airflow being installed
sys.modules.setdefault("airflow", type(sys)("airflow"))
sys.modules.setdefault("airflow.decorators", type(sys)("airflow.decorators"))
sys.modules.setdefault("airflow.models", type(sys)("airflow.models"))
sys.modules.setdefault("airflow.models.param", type(sys)("airflow.models.param"))
sys.modules.setdefault("mlflow", type(sys)("mlflow"))

# Patch decorators so the module loads without executing task bodies
_fake_airflow = sys.modules["airflow.decorators"]
_fake_airflow.dag  = lambda **kw: (lambda f: f)          # @dag: pass through
_fake_airflow.task = lambda f: (lambda *a, **kw: None)   # @task: swallow calls
sys.modules["airflow.models.param"].Param = lambda *a, **kw: None

# Now import helpers directly from the dag file
import importlib.util, os

_dag_path = Path(__file__).resolve().parents[1] / "dags" / "evaluate_agent.py"
_spec = importlib.util.spec_from_file_location("evaluate_agent", _dag_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

build_run_config   = _mod.build_run_config
prepare_run_dir    = _mod.prepare_run_dir
collect_metrics    = _mod.collect_metrics


# ---------------------------------------------------------------------------
# build_run_config
# ---------------------------------------------------------------------------

class TestBuildRunConfig:
    BASE_PARAMS = {
        "split":      "test",
        "subset":     "verified",
        "workers":    2,
        "model":      "nebius/moonshotai/Kimi-K2.6",
        "task_slice": "0:3",
        "run_id":     "",
        "cost_limit": "0",
    }

    def test_auto_generates_run_id_when_empty(self):
        cfg = build_run_config(self.BASE_PARAMS)
        assert cfg["run_id"].startswith("run-")

    def test_uses_provided_run_id(self):
        params = {**self.BASE_PARAMS, "run_id": "my-custom-run"}
        cfg = build_run_config(params)
        assert cfg["run_id"] == "my-custom-run"

    def test_workers_cast_to_int(self):
        cfg = build_run_config(self.BASE_PARAMS)
        assert isinstance(cfg["workers"], int)

    def test_all_required_keys_present(self):
        cfg = build_run_config(self.BASE_PARAMS)
        for key in ("run_id", "split", "subset", "workers", "model", "task_slice", "cost_limit", "timestamp"):
            assert key in cfg, f"Missing key: {key}"

    def test_values_match_params(self):
        cfg = build_run_config(self.BASE_PARAMS)
        assert cfg["split"]      == "test"
        assert cfg["subset"]     == "verified"
        assert cfg["model"]      == "nebius/moonshotai/Kimi-K2.6"
        assert cfg["task_slice"] == "0:3"


# ---------------------------------------------------------------------------
# prepare_run_dir
# ---------------------------------------------------------------------------

class TestPrepareRunDir:
    def _make_config(self, tmp_path):
        return {
            "run_id":     "test-run-001",
            "split":      "test",
            "subset":     "verified",
            "workers":    2,
            "model":      "nebius/moonshotai/Kimi-K2.6",
            "task_slice": "0:3",
            "cost_limit": "0",
            "timestamp":  "2026-06-30T00:00:00",
        }

    def test_creates_expected_directories(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_mod, "RUNS_DIR", tmp_path)
        cfg = self._make_config(tmp_path)
        run_dir = prepare_run_dir(cfg)

        assert (run_dir / "run-agent" / "trajectories").is_dir()
        assert (run_dir / "run-eval" / "logs").is_dir()
        assert (run_dir / "run-eval" / "reports").is_dir()

    def test_writes_config_json(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_mod, "RUNS_DIR", tmp_path)
        cfg = self._make_config(tmp_path)
        run_dir = prepare_run_dir(cfg)

        config_file = run_dir / "config.json"
        assert config_file.exists()
        saved = json.loads(config_file.read_text())
        assert saved["run_id"] == "test-run-001"
        assert saved["model"]  == "nebius/moonshotai/Kimi-K2.6"

    def test_returns_correct_run_dir_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_mod, "RUNS_DIR", tmp_path)
        cfg = self._make_config(tmp_path)
        run_dir = prepare_run_dir(cfg)
        assert run_dir == tmp_path / "test-run-001"


# ---------------------------------------------------------------------------
# collect_metrics
# ---------------------------------------------------------------------------

class TestCollectMetrics:
    def _make_eval_dir(self, tmp_path, report_data: dict) -> Path:
        reports = tmp_path / "reports"
        reports.mkdir(parents=True)
        (reports / "results.json").write_text(json.dumps(report_data))
        return tmp_path

    def test_parses_resolved_instances(self, tmp_path):
        eval_dir = self._make_eval_dir(tmp_path, {
            "submitted_instances": 3,
            "resolved_instances":  1,
            "total_instances":     500,
        })
        metrics = collect_metrics(eval_dir)
        assert metrics["resolved_instances"]  == 1
        assert metrics["submitted_instances"] == 3

    def test_calculates_resolve_rate(self, tmp_path):
        eval_dir = self._make_eval_dir(tmp_path, {
            "submitted_instances": 4,
            "resolved_instances":  1,
        })
        metrics = collect_metrics(eval_dir)
        assert metrics["resolve_rate"] == 0.25

    def test_returns_zeros_when_no_reports(self, tmp_path):
        (tmp_path / "reports").mkdir()
        metrics = collect_metrics(tmp_path)
        assert metrics["resolved_instances"] == 0
        assert metrics["resolve_rate"]       == 0.0

    def test_handles_malformed_json_gracefully(self, tmp_path):
        reports = tmp_path / "reports"
        reports.mkdir()
        (reports / "bad.json").write_text("not json {{{")
        metrics = collect_metrics(tmp_path)
        assert metrics["resolved_instances"] == 0

    def test_resolve_rate_zero_when_no_submissions(self, tmp_path):
        eval_dir = self._make_eval_dir(tmp_path, {
            "submitted_instances": 0,
            "resolved_instances":  0,
        })
        metrics = collect_metrics(eval_dir)
        assert metrics["resolve_rate"] == 0.0