"""Framework level configuration.

Two distinct kinds of settings exist in this framework, and keeping them apart is
what stops the control tables from becoming environment specific:

  * FrameworkConfig  - environment wiring (catalogs, checkpoint root, log level).
                       Lives in conf/framework.<env>.yml, deployed per environment.
  * Control tables   - per feed metadata (which file, which table, which rules).
                       Environment agnostic, promoted unchanged dev -> tst -> prd.

Catalog names in the control tables are written as logical tokens - `bronze`,
`silver`, `gold`, or `{env}`-prefixed literals - and resolved through
FrameworkConfig.resolve_catalog so the same metadata row works in every
environment. A literal catalog name in the YAML is still honoured as-is.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from .exceptions import ConfigurationError

_TOKEN_RE = re.compile(r"\$\{([a-zA-Z0-9_.]+)\}")


@dataclass
class FrameworkConfig:
    """Resolved framework settings for one environment."""

    environment: str

    # --- framework metadata location -------------------------------------------------
    framework_catalog: str
    control_schema: str = "control"
    audit_schema: str = "audit"

    # --- logical layer catalogs ------------------------------------------------------
    # Control table rows may say catalog_name: bronze and have it resolved here.
    catalogs: Dict[str, str] = field(default_factory=dict)

    # --- storage -------------------------------------------------------------------
    checkpoint_root: str = ""
    quarantine_schema_suffix: str = ""

    # --- defaults applied when the control row leaves a column NULL ------------------
    defaults: Dict[str, Any] = field(default_factory=dict)

    # --- Free Edition namespace setup ------------------------------------------------
    # Read only by notebooks/00_setup_framework.py, which creates the Volumes and layer
    # schemas. The framework's runtime path never touches it.
    free_edition: Dict[str, Any] = field(default_factory=dict)

    log_level: str = "INFO"
    framework_version: str = "1.0.0"

    # -------------------------------------------------------------------------------
    # factory
    # -------------------------------------------------------------------------------
    @classmethod
    def load(cls, environment: Optional[str] = None, conf_dir: Optional[str] = None) -> "FrameworkConfig":
        """Load conf/framework.<env>.yml.

        `environment` falls back to the ETL_ENVIRONMENT env var, then to "dev".
        `conf_dir` falls back to ETL_CONF_DIR, then to <repo>/conf.
        """
        env = environment or os.environ.get("ETL_ENVIRONMENT") or "dev"
        base = Path(conf_dir or os.environ.get("ETL_CONF_DIR") or _default_conf_dir())
        path = base / f"framework.{env}.yml"
        if not path.exists():
            raise ConfigurationError(
                f"Framework config not found: {path}. "
                f"Expected conf/framework.{env}.yml relative to {base}."
            )
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return cls.from_dict(raw, environment=env)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any], environment: Optional[str] = None) -> "FrameworkConfig":
        env = environment or raw.get("environment")
        if not env:
            raise ConfigurationError("environment must be set in the framework config or passed explicitly")

        # ${env} placeholders inside the yml are expanded against the resolved values.
        scope = {"env": env}
        resolved = _expand_tokens(raw, scope)

        try:
            framework_catalog = resolved["framework_catalog"]
        except KeyError as exc:
            raise ConfigurationError("framework_catalog is required in the framework config") from exc

        cfg = cls(
            environment=env,
            framework_catalog=framework_catalog,
            control_schema=resolved.get("control_schema", "control"),
            audit_schema=resolved.get("audit_schema", "audit"),
            catalogs=dict(resolved.get("catalogs", {})),
            checkpoint_root=resolved.get("checkpoint_root", ""),
            quarantine_schema_suffix=resolved.get("quarantine_schema_suffix", ""),
            defaults=dict(resolved.get("defaults", {})),
            free_edition=dict(resolved.get("free_edition", {})),
            log_level=resolved.get("log_level", "INFO"),
            framework_version=resolved.get("framework_version", "1.0.0"),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.checkpoint_root:
            raise ConfigurationError(
                "checkpoint_root is required - Auto Loader needs a durable location for its "
                "schema store and streaming checkpoints"
            )
        # Free Edition has no storage credential, so an external path can only fail at
        # the first write - hours into a run. Reject it at config load instead.
        if "://" in self.checkpoint_root:
            raise ConfigurationError(
                f"checkpoint_root is an external path ({self.checkpoint_root}). This build "
                f"targets Databricks Free Edition, which has no storage credential - use a "
                f"UC Volume path such as /Volumes/<catalog>/<schema>/<volume>/checkpoints"
            )
        for layer in ("bronze", "silver", "gold"):
            if layer not in self.catalogs:
                raise ConfigurationError(f"catalogs.{layer} is missing from the framework config")

    # -------------------------------------------------------------------------------
    # helpers
    # -------------------------------------------------------------------------------
    def resolve_catalog(self, catalog_name: str) -> str:
        """Map a logical catalog token to the physical catalog for this environment.

        `bronze` -> catalogs.bronze; anything not in the map is returned untouched so
        that a control row may name a physical catalog directly when it must.
        """
        if not catalog_name:
            raise ConfigurationError("catalog_name is empty - the control table row is incomplete")
        return self.catalogs.get(catalog_name.strip().lower(), catalog_name)

    def default_for(self, key: str, fallback: Any = None) -> Any:
        """Framework default used when a control table column is NULL."""
        return self.defaults.get(key, fallback)

    @property
    def control_prefix(self) -> str:
        return f"{self.framework_catalog}.{self.control_schema}"

    @property
    def audit_prefix(self) -> str:
        return f"{self.framework_catalog}.{self.audit_schema}"

    def control_table(self, name: str) -> str:
        return f"{self.control_prefix}.{name}"

    def audit_table(self, name: str) -> str:
        return f"{self.audit_prefix}.{name}"

    def checkpoint_path(self, layer: str, catalog: str, schema: str, table: str, kind: str) -> str:
        """Deterministic checkpoint/schema location.

        Deriving these keeps the control tables free of storage paths that would
        otherwise have to change per environment. `kind` is "checkpoint" or "schema".
        """
        root = self.checkpoint_root.rstrip("/")
        return f"{root}/{layer}/{catalog}/{schema}/{table}/_{kind}"


def _default_conf_dir() -> Path:
    """<repo root>/conf, derived from this file's location."""
    return Path(__file__).resolve().parents[2] / "conf"


def _expand_tokens(node: Any, scope: Dict[str, str]) -> Any:
    """Recursively expand ${key} placeholders using `scope`."""
    if isinstance(node, dict):
        return {k: _expand_tokens(v, scope) for k, v in node.items()}
    if isinstance(node, list):
        return [_expand_tokens(v, scope) for v in node]
    if isinstance(node, str):
        return _TOKEN_RE.sub(lambda m: scope.get(m.group(1), m.group(0)), node)
    return node
