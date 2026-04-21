"""
_project_root.py  –  shared helper imported by all DAGs

Resolves the project root reliably whether running:
  - locally (python airflow/dags/dag_xxx.py)
  - in Docker with CAPSTONE_ROOT env var set
  - in Airflow with DAGs folder mounted anywhere

Priority order:
  1. CAPSTONE_ROOT environment variable   ← set this in docker-compose / .env
  2. Walk up from this file looking for main.py   ← works locally
  3. /opt/airflow/capstone   ← Docker default fallback
"""

import os
from pathlib import Path


def find_project_root() -> Path:
    # 1. Explicit env var — most reliable in Docker
    env_root = os.environ.get("CAPSTONE_ROOT")
    if env_root:
        p = Path(env_root).resolve()
        if p.exists():
            return p

    # 2. Walk up from this file until we find main.py
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / "main.py").exists() and (parent / "data_lake").exists():
            return parent

    # 3. Docker default: project mounted at /opt/airflow/capstone
    docker_default = Path("/opt/airflow/capstone")
    if docker_default.exists():
        return docker_default

    # 4. Last resort: two levels up from dags/
    return here.parents[2]


PROJECT_ROOT = find_project_root()
