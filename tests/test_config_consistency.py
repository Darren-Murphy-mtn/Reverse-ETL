"""The marker and sandbox repo are configured on both the Python and the dbt side; keep them in sync."""
import yaml

from pipeline_common.config import ROOT, load_config


def dbt_vars() -> dict:
    return yaml.safe_load((ROOT / "dbt_project" / "dbt_project.yml").read_text())["vars"]


def test_marker_matches_dbt_var():
    assert dbt_vars()["stale_marker"] in load_config().reverse_etl.marker


def test_stale_label_matches_dbt_var():
    assert dbt_vars()["stale_label"] == load_config().reverse_etl.stale_label


def test_sandbox_has_a_dbt_threshold_override():
    assert load_config().sandbox_repo in dbt_vars()["stale_days_by_repo"]
