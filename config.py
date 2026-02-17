"""
COnfiguration utilities for GitLab Service Collector.
Утилиты конфигурации для GitLab Service Collector.
"""

import os
import json
from dotenv import load_dotenv
from typing import Dict, Any


def load_config() -> Dict[str, Any]:
    """Load config from .env, environment, and optional config.json.
       ЗАгрузить конфигурацию из .env, пееменных окружения и опционального config.json."""
    load_dotenv()
    config = {
        "gitlab_url": os.getenv("GITLAB_URL", "https://gitlab.com"),
        "gitlab_token": os.getenv("GITLAB_PRIVATE_TOKEN", ""),
        "group_path": os.getenv("GITLAB_GROUP", ""),
        "max_projects": int(os.getenv("MAX_PROJECTS", "100")),
        "timeout": int(os.getenv("REQUEST_TIMEOUT", "30")),
        "max_retries": int(os.getenv("MAX_RETRIES", "3")),
        "output_dir": os.getenv("OUTPUT_DIR", "results"),
        "ignore_files": os.getenv("IGNORE_FILES", ".gitlab-ci.yml,docker-compose.yml,docker-compose.yaml").split(","),
        "ignore_projects": os.getenv("IGNORE_PROJECTS", "").split(",") if os.getenv("IGNORE_PROJECTS") else []
    }

    # Override with config.json if it exists
    if os.path.exists("config.json"):
        try:
            with open("config.json") as f:
                config.update(json.load(f))
        except Exception:
            pass
    return config


def save_sample_config():
    """Create an example config file (config.example.json)."""
    sample = {
        "gitlab_url": "https://gitlab.com",
        "gitlab_token": "YOUR_PRIVATE_TOKEN_HERE",
        "group_path": "your-group-name",
        "max_projects": 50,
        "timeout": 30,
        "max_retries": 3,
        "output_dir": "results",
        "ignore_files": [".gitlab-ci.yml", "docker-compose.yml", "docker-compose.yaml"],
        "ignore_projects": ["test-", "demo-", "example-"]
    }
    with open("config.example.json", "w") as f:
        json.dump(sample, f, indent=2)
    print("📄 Example config created: config.example.json")


def ensure_output_dir(output_dir: str = "results") -> str:
    """Create output directory if missing."""
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def format_size(size_bytes: int) -> str:
    """Convert bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"