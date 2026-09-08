"""Runtime bootstrap.

Every execution notebook needs the same five things wired together. Doing that once
here keeps the notebooks to roughly a dozen lines, which is what makes them
maintainable as generic programs rather than per-table copies.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

from .audit import AuditLogger
from .config import FrameworkConfig
from .control import ControlRepository
from .logging_utils import FrameworkLogger
from .spark_utils import get_spark


@dataclass
class Runtime:
    """The wired-up framework, handed to a layer runner."""

    spark: SparkSession
    cfg: FrameworkConfig
    repo: ControlRepository
    audit: AuditLogger
    log: FrameworkLogger

    @property
    def batch_id(self) -> str:
        return self.audit.batch_id


def bootstrap(
    environment: Optional[str] = None,
    batch_id: Optional[str] = None,
    layer: Optional[str] = None,
    conf_dir: Optional[str] = None,
) -> Runtime:
    """Resolve config, open the control tables and start an audit session."""
    ensure_repo_on_path()
    spark = get_spark()
    cfg = FrameworkConfig.load(environment=environment, conf_dir=conf_dir)
    log = FrameworkLogger({"environment": cfg.environment, "layer": layer or "framework"}, cfg.log_level)
    audit = AuditLogger(spark, cfg, batch_id=batch_id, logger=log)
    log.bind(batch_id=audit.batch_id)
    repo = ControlRepository(spark, cfg)
    log.info(
        "framework runtime ready",
        framework_version=cfg.framework_version,
        control_schema=cfg.control_prefix,
        audit_schema=cfg.audit_prefix,
    )
    return Runtime(spark=spark, cfg=cfg, repo=repo, audit=audit, log=log)


def ensure_repo_on_path() -> Path:
    """Put <repo>/src and <repo>/notebooks on sys.path.

    src/ so `framework` imports, and notebooks/ so gold transformation modules named
    in the control table (e.g. transformations.gold.dim_customer) can be imported.
    """
    repo_root = Path(__file__).resolve().parents[2]
    for candidate in (repo_root / "src", repo_root / "notebooks"):
        path = str(candidate)
        if candidate.exists() and path not in sys.path:
            sys.path.insert(0, path)
    return repo_root
