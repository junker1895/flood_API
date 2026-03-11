from datetime import date

from app.jobs.ingest_forecast_run import _parse_forecast_date, should_store_detail, should_store_risk




def test_sparse_risk_write_rules():
    assert should_store_risk(risk_class=1, uparea=1.0, region_id="", major_threshold=5000.0, priority_regions=set())
    assert should_store_risk(risk_class=0, uparea=6000.0, region_id="", major_threshold=5000.0, priority_regions=set())
    assert should_store_risk(risk_class=0, uparea=1.0, region_id="A", major_threshold=5000.0, priority_regions={"A"})
    assert not should_store_risk(risk_class=0, uparea=1.0, region_id="", major_threshold=5000.0, priority_regions=set())


def test_sparse_detail_write_rules():
    assert should_store_detail(risk_class=1, uparea=1.0, region_id="", detail_threshold=10000.0, detail_regions=set())
    assert should_store_detail(risk_class=0, uparea=10001.0, region_id="", detail_threshold=10000.0, detail_regions=set())
    assert should_store_detail(risk_class=0, uparea=1.0, region_id="B", detail_threshold=10000.0, detail_regions={"B"})
    assert not should_store_detail(risk_class=0, uparea=1.0, region_id="", detail_threshold=10000.0, detail_regions=set())


def test_parse_forecast_date_supports_iso_and_yyyymmdd():
    assert _parse_forecast_date("2026-03-11") == date(2026, 3, 11)
    assert _parse_forecast_date("20260311") == date(2026, 3, 11)
