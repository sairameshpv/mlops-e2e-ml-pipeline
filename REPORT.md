# Evaluation Pipeline Report

## Architecture

The pipeline is implemented as an Airflow DAG (`dags/evaluate_agent.py`) with four sequential tasks:

```
prepare_run → run_agent → run_eval → summarize_and_log
```

### Task Responsibilities

| Task | What it does |
|---|---|
| `prepare_run` | Reads Airflow params, auto-generates a `run_id`, creates the `runs/<run-id>/` directory tree, writes `config.json` |
| `run_agent` | Calls `mini-extra swebench` with DAG params, writes trajectories and `preds.json` to `runs/<run-id>/run-agent/` |
| `run_eval` | Runs `python -m swebench.harness.run_evaluation` against the produced `preds.json`, writes logs and reports to `runs/<run-id>/run-eval/` |
| `summarize_and_log` | Parses evaluation report, writes `metrics.json` and `manifest.json`, logs params + metrics + artifact paths to MLflow |

Helper functions (`build_run_config`, `prepare_run_dir`, `run_agent_batch`, `run_swebench_eval`, `collect_metrics`, `log_mlflow_run`) are defined at module level so they can be unit-tested independently of Airflow.

---

## How to Start the Services

### MLflow (Terminal 1)
```bash
cd mlops-e2e-ml-pipeline
/Users/sairameshpv/Documents/Nebius/.venv/bin/uv tool run --python 3.12 mlflow server --host 127.0.0.1 --port 5001
```
> Note: port 5000 is occupied by macOS AirPlay Receiver. MLflow runs on 5001.

### Airflow (Terminal 2)
```bash
cd mlops-e2e-ml-pipeline
set -a && source .env && set +a
bash run-airflow-standalone.sh
```

- Airflow UI: http://localhost:8080 (admin / admin)
- MLflow UI: http://localhost:5001

---

## How to Trigger the DAG

### From the Airflow UI

1. Open http://localhost:8080
2. Find **`evaluate_agent`** in the DAG list
3. Click **▶ Trigger DAG w/ config**
4. Paste the parameters JSON:

```json
{
  "split": "test",
  "subset": "verified",
  "workers": 2,
  "model": "nebius/moonshotai/Kimi-K2.6",
  "task_slice": "0:3",
  "run_id": "",
  "cost_limit": "0"
}
```

5. Click **Trigger**

### From the CLI (REST API)

```bash
TOKEN=$(curl -s -X POST "http://localhost:8080/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -X POST "http://localhost:8080/api/v2/dags/evaluate_agent/dagRuns" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{
    \"logical_date\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"conf\": {
      \"split\": \"test\",
      \"subset\": \"verified\",
      \"workers\": 2,
      \"model\": \"nebius/moonshotai/Kimi-K2.6\",
      \"task_slice\": \"0:3\",
      \"run_id\": \"\",
      \"cost_limit\": \"0\"
    }
  }"
```

### DAG Parameters

| Parameter | Default | Description |
|---|---|---|
| `split` | `test` | SWE-bench split: `test` or `dev` |
| `subset` | `verified` | SWE-bench subset: `verified`, `lite`, or `full` |
| `workers` | `2` | Parallel workers for agent and evaluation |
| `model` | `nebius/moonshotai/Kimi-K2.6` | Inference model |
| `task_slice` | `0:3` | Python-style slice of tasks e.g. `0:10` |
| `run_id` | _(auto)_ | Custom run ID; auto-generated if left empty |
| `cost_limit` | `0` | Per-task cost limit in USD (0 = no limit) |

---

## Artifact Layout

Every run produces a fully self-contained directory:

```
runs/
  <run-id>/
    config.json          ← all params used for this run
    metrics.json         ← resolve_rate, resolved/submitted counts
    manifest.json        ← absolute paths to every artifact
    run-agent/
      preds.json         ← patches in SWE-bench format (keyed by instance_id)
      trajectories/
        <instance-id>/
          <instance-id>.traj.json   ← full agent reasoning + tool calls
        exit_statuses_<ts>.yaml     ← per-instance exit status
        minisweagent.log            ← agent run log
    run-eval/
      reports/
        <model>.<run-id>.json       ← SWE-bench summary report
        logs/
          run_evaluation/<run-id>/<model>/
            <instance-id>/
              eval.sh               ← evaluation script
              patch.diff            ← applied patch
              report.json           ← pass/fail per test
              test_output.txt       ← raw test runner output
              run_instance.log      ← container execution log
```

### How to Re-run by `run_id`

Pass the existing `run_id` in the trigger config to reuse the directory:

```json
{ "run_id": "run-20260701T035348-abba78", ... }
```

`prepare_run` uses `exist_ok=True` so the directory is not wiped. `run_agent` will re-run the agent and overwrite `preds.json`. To skip the agent and only re-run evaluation, that requires triggering `run_eval` directly from the Airflow UI (click the task → **Clear**).

---

## Completed Run: `run-20260701T035348-abba78`

| Field | Value |
|---|---|
| Run ID | `run-20260701T035348-abba78` |
| Timestamp | 2026-07-01T03:53:48 UTC |
| Model | `nebius/moonshotai/Kimi-K2.6` |
| Dataset | SWE-bench Verified, split=test |
| Task slice | `0:3` (astropy instances) |
| Workers | 2 |

### Results

| Metric | Value |
|---|---|
| Submitted instances | 3 |
| Completed instances | 3 |
| Resolved instances | **2** |
| Unresolved instances | 1 |
| Empty patches | 0 |
| **Resolve rate** | **66.67%** |

Instances attempted:
- `astropy__astropy-12907` — **RESOLVED**
- `astropy__astropy-13033` — **RESOLVED**
- `astropy__astropy-13236` — unresolved

---

## MLflow Tracking

All runs are logged to the **`swe-bench-evaluation`** experiment at http://localhost:5001.

Each run logs:
- **Params**: `run_id`, `model`, `split`, `subset`, `task_slice`, `workers`, `cost_limit`, `timestamp`
- **Metrics**: `resolve_rate`, `resolved_instances`, `submitted_instances`, `completed_instances`, `empty_patch_instances`, `error_instances`
- **Artifacts**: `config.json`, `metrics.json`, `manifest.json`

### Run Comparison

| Run ID | Model | Submitted | Resolved | Resolve Rate | Note |
|---|---|---|---|---|---|
| `run-20260701T035348-abba78` | Kimi-K2.6 | 3 | 2 | **0.6667** | With swebench config |
| `run-20260701T022650-fab9e4` | Kimi-K2.6 | 3 | 0 | 0.0 | Without config (empty patches) |
| `run-20260701T020900-57c900` | Kimi-K2.6 | 3 | 0 | 0.0 | Without config (empty patches) |

The config file (`mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml`) provides the agent with task instructions, Docker environment setup, and patch submission protocol. Without it the agent produces empty patches.

Screenshots:
- `screenshots/airflow_dag.png` — Airflow Graph view showing all 4 tasks completed
- `screenshots/mlflow_runs.png` — MLflow UI showing all 3 runs with metrics

---

## Prerequisites

- Nebius VM: 8 CPU, 32 GB RAM (or run locally on Mac)
- `NEBIUS_API_KEY` in `.env`
- Docker Desktop running (required by SWE-bench evaluation harness)
- Upstream repos cloned:
  ```bash
  git clone https://github.com/SWE-agent/mini-swe-agent.git
  git clone https://github.com/swe-bench/SWE-bench.git
  ```
- Dependencies installed: `uv sync`

---

## What Would Be Added for Production

- **`docker-compose.yaml`**: replace manual `run-airflow-standalone.sh` + `mlflow server` with `docker compose up -d`
- **DockerOperator**: replace `subprocess.run` in `run_agent` and `run_eval` with isolated Docker tasks
- **S3 upload**: add a fifth task to upload `runs/<run-id>/` to Nebius Object Storage and log the URI to MLflow