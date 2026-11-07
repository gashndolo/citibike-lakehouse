import dagster as dg

from lakehouse import orchestration, pipeline


def _stub(monkeypatch, gate_status="pass"):
    def ok(*args, **kwargs):
        return {"status": "loaded", "rows_loaded": 1}

    monkeypatch.setattr(pipeline, "run_bronze", ok)
    monkeypatch.setattr(pipeline, "run_silver", ok)
    monkeypatch.setattr(pipeline, "run_gold", ok)
    monkeypatch.setattr(pipeline, "run_quality", lambda *a, **k: {
        "status": gate_status, "rows_checked": 1,
        "failed_expectations": None if gate_status == "pass" else "expect_x:ride_id"})


def _run(**kw):
    return dg.materialize(
        orchestration.assets, partition_key="2021-03-01", raise_on_error=False,
        resources={"spark": dg.ResourceDefinition.mock_resource()}, **kw)


def _built(result):
    return {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}


def test_month_key():
    assert pipeline.month_key("2021-03-01") == "202103"


def test_twelve_monthly_partitions():
    keys = orchestration.MONTHS.get_partition_keys()
    assert keys[0] == "2021-01-01" and keys[-1] == "2021-12-01" and len(keys) == 12


def test_definitions_contain_the_core_layers():
    names = {a.key.to_user_string() for a in orchestration.defs.resolve_all_asset_specs()}
    assert {"bronze_trips", "silver_trips", "quality_gate"} <= names


def test_healthy_month_builds_every_layer(monkeypatch):
    _stub(monkeypatch, "pass")
    result = _run()
    assert result.success
    assert {"bronze_trips", "silver_trips", "quality_gate"} <= _built(result)


def test_failed_gate_stops_the_run_before_gold(monkeypatch):
    _stub(monkeypatch, "fail")
    result = _run()
    assert not result.success
    built = _built(result)
    assert {"bronze_trips", "silver_trips"} <= built
    assert "quality_gate" not in built and "gold_tables" not in built