"""
ingestion/base.py
Abstract base class for all data extractors.
"""
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class BaseExtractor(ABC):
    """
    Abstract base for all ingestion extractors.
    Provides config loading, path resolution, and logging setup.
    """

    def __init__(self, config_path: Optional[str] = None, layer: str = "raw"):
        self.config_path = config_path
        self.layer = layer

        # Logger FIRST
        self.logger = self._setup_logger()

        # Load config ONLY if provided
        if config_path:
            self.config = self._load_config(config_path)
        else:
            self.config = {}

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        with open(config_path, "r") as f:
            raw = yaml.safe_load(f)
        return self._resolve_env_vars(raw)

    def _resolve_env_vars(self, obj: Any) -> Any:
        """Recursively replace ${VAR} placeholders with environment values."""
        if isinstance(obj, dict):
            return {k: self._resolve_env_vars(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._resolve_env_vars(i) for i in obj]
        if isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
            var_name = obj[2:-1]
            value = os.getenv(var_name)
            if value is None:
                self.logger.warning(f"Environment variable '{var_name}' not set.")
            return value
        return obj

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.__class__.__name__)

        if not logger.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(fmt)
            logger.addHandler(handler)

        logger.setLevel(logging.INFO)

        # ✅ ADD THIS LINE (fix duplication)
        logger.propagate = False

        return logger

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------
    def _build_output_path(
        self,
        entity: str,
        source_name: str,
        partition: Optional[Dict[str, str]] = None,
    ) -> Path:
        """
        Build a generic, standardised raw-layer path:
          data_lake/raw/<entity>/<source_name>/<partition_keys>/
        Example:
          data_lake/raw/weather/open_meteo/city=London/year=2023/month=01/
        """
        base = Path("data_lake") / self.layer / entity / source_name
        if partition:
            for k, v in partition.items():
                base = base / f"{k}={v}"
        base.mkdir(parents=True, exist_ok=True)
        return base

    def _timestamped_filename(self, prefix: str, extension: str) -> str:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        return f"{prefix}_{ts}.{extension}"

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------
    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """Run extraction and return path(s) of written files."""
        ...

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Basic validation of extracted data."""
        ...
