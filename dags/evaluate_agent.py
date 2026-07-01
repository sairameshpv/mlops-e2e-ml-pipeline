import json
import os
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = PROJECT_ROOT / "runs"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def build_run_config(params: dict) -> dict:
    now = datetime.now(UTC)
    run_id = params.get("run_id") or (
        f"run-{now.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
    )
    return {
        "run_id":     run_id,
        "split":      params["split"],
        "subset":     params["subset"],
        "workers":    int(params["workers"]),
        "model":      params["model"],
        "task_slice": params["task_slice"],
        "cost_limit": params["cost_limit"],
        "timestamp":  now.isoformat(),
    }


def prepare_run_dir(run_config: dict) -> Path:
    run_dir = RUNS_DIR / run_config["run_id"]
    (run_dir / "run-agent" / "trajectories").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "reports").mkdir(parents=True, exist_ok=True)
    (run_dir / "config.json").write_text(json.dumps(run_config, indent=2))
    return run_dir


def run_agent_batch(run_config: dict, run_dir: Path) -> Path:
    traj_dir = run_dir / "run-agent" / "trajectories"

    cmd = [
        "uv", "run", "mini-extra", "swebench",
        "--subset",  run_config["subset"],
        "--split",   run_config["split"],
        "--model",   run_config["model"],
        "--slice",   run_config["task_slice"],
        "--workers", str(run_config["workers"]),
        "-o",        str(traj_dir),
    ]

    # Use the bundled swebench config if the upstream repo was cloned
    config_path = PROJECT_ROOT / "mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml"
    if config_path.exists():
        cmd += ["--config", str(config_path)]

    subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        env={**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"},
        check=True,
    )

    # mini-swe-agent writes preds.json inside the trajectory output dir;
    # copy it one level up so run-agent/preds.json is the canonical location.
    traj_preds  = traj_dir / "preds.json"
    agent_preds = run_dir / "run-agent" / "preds.json"
    if traj_preds.exists() and not agent_preds.exists():
        agent_preds.write_text(traj_preds.read_text())

    return agent_preds


def run_swebench_eval(run_config: dict, preds_path: Path, run_dir: Path) -> Path:
    eval_dir    = run_dir / "run-eval"
    reports_dir = eval_dir / "reports"
    subset_cap  = run_config["subset"].capitalize()

    subprocess.run(
        [
            "python", "-m", "swebench.harness.run_evaluation",
            "--dataset_name",    f"princeton-nlp/SWE-bench_{subset_cap}",
            "--predictions_path", str(preds_path),
            "--max_workers",     str(run_config["workers"]),
            "--run_id",          run_config["run_id"],
        ],
        # Run from reports_dir so SWE-bench drops its output JSON there
        cwd=str(reports_dir),
        check=True,
    )

    return eval_dir


def collect_metrics(eval_dir: Path) -> dict:
    metrics: dict = {
        "total_instances":      0,
        "submitted_instances":  0,
        "completed_instances":  0,
        "resolved_instances":   0,
        "unresolved_instances": 0,
        "empty_patch_instances": 0,
        "error_instances":      0,
        "resolve_rate":         0.0,
    }

    for report_file in (eval_dir / "reports").glob("*.json"):
        try:
            data = json.loads(report_file.read_text())
            for key in list(metrics.keys()):
                if key in data:
                    metrics[key] = data[key]
            submitted = metrics["submitted_instances"]
            if submitted > 0:
                metrics["resolve_rate"] = round(
                    metrics["resolved_instances"] / submitted, 4
                )
        except (json.JSONDecodeError, KeyError):
            continue

    return metrics


def log_mlflow_run(run_config: dict, metrics: dict, run_dir: Path) -> None:
    import mlflow  # lazy import — only needed at task runtime, not DAG parse time
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("swe-bench-evaluation")

    with mlflow.start_run(run_name=run_config["run_id"]):
        mlflow.log_params(run_config)
        mlflow.log_metrics({k: float(v) for k, v in metrics.items()})
        mlflow.log_artifact(str(run_dir / "config.json"))
        mlflow.log_artifact(str(run_dir / "metrics.json"))
        mlflow.log_artifact(str(run_dir / "manifest.json"))
        mlflow.set_tag("artifact_path", str(run_dir))


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split":      Param("test",                         type="string",  description="SWE-bench split: test or dev"),
        "subset":     Param("verified",                     type="string",  description="SWE-bench subset: verified, lite, or full"),
        "workers":    Param(2,                              type="integer", description="Number of parallel workers"),
        "model":      Param("nebius/moonshotai/Kimi-K2.6", type="string",  description="Inference model to use"),
        "task_slice": Param("0:3",                          type="string",  description="Python-style slice of tasks e.g. 0:3"),
        "run_id":     Param("",                             type="string",  description="Custom run ID; auto-generated if left empty"),
        "cost_limit": Param("0",                            type="string",  description="Per-task cost limit (0 = no limit)"),
    },
)
def evaluate_agent():

    @task
    def prepare_run(params=None) -> str:
        """Read Airflow params and create runs/<run-id>/config.json."""
        run_config = build_run_config(params)
        prepare_run_dir(run_config)
        return run_config["run_id"]

    @task
    def run_agent(pipeline_run_id: str) -> str:
        """Run mini-swe-agent batch and write trajectories + preds.json."""
        run_dir    = RUNS_DIR / pipeline_run_id
        run_config = json.loads((run_dir / "config.json").read_text())
        preds_path = run_agent_batch(run_config, run_dir)
        return str(preds_path)

    @task
    def run_eval(pipeline_run_id: str, preds_path: str) -> str:
        """Run SWE-bench evaluation on the produced preds.json."""
        run_dir    = RUNS_DIR / pipeline_run_id
        run_config = json.loads((run_dir / "config.json").read_text())
        eval_dir   = run_swebench_eval(run_config, Path(preds_path), run_dir)
        return str(eval_dir)

    @task
    def summarize_and_log(pipeline_run_id: str, eval_dir: str) -> None:
        """Parse evaluation reports, write metrics.json + manifest.json, log to MLflow."""
        run_dir    = RUNS_DIR / pipeline_run_id
        run_config = json.loads((run_dir / "config.json").read_text())

        metrics = collect_metrics(Path(eval_dir))
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

        manifest = {
            "run_id":       pipeline_run_id,
            "config":       str(run_dir / "config.json"),
            "predictions":  str(run_dir / "run-agent" / "preds.json"),
            "trajectories": str(run_dir / "run-agent" / "trajectories"),
            "eval_logs":    str(run_dir / "run-eval" / "logs"),
            "eval_reports": str(run_dir / "run-eval" / "reports"),
            "metrics":      str(run_dir / "metrics.json"),
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

        log_mlflow_run(run_config, metrics, run_dir)

    # Wire tasks in order: prepare → agent → eval → log
    rid      = prepare_run()
    preds    = run_agent(rid)
    eval_out = run_eval(rid, preds)
    summarize_and_log(rid, eval_out)


evaluate_agent()
