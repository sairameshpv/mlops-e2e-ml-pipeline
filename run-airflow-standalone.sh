set -euo pipefail

export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false

# Expose project venv packages (mlflow, swebench, mini-swe-agent, etc.) to Airflow workers
export PYTHONPATH="$(pwd)/.venv/lib/python3.14/site-packages${PYTHONPATH:+:$PYTHONPATH}"

mkdir -p $AIRFLOW_HOME

echo '{"admin": "admin"}' > $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated

uv tool run --with mlflow apache-airflow standalone
