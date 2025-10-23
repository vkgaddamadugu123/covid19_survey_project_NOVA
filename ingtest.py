import pandas as pd
import pytest
import Ingestion  #importing our main file as a module

def test_get_engine_env_missing(monkeypatch):     #test for missing env variable
    monkeypatch.delenv("PG_URL", raising=False)   #delete pgurl
    with pytest.raises(SystemExit):       #to fail and exit if no url is present
        Ingestion.get_engine()

def test_get_engine_valid(monkeypatch):    #to set pg url to inmemro without external db and to verify
    monkeypatch.setenv("PG_URL", "sqlite:///:memory:")
    eng = Ingestion.get_engine()
    assert "sqlite" in str(eng.url)

def test_fetch_page_success(monkeypatch):       #to test api fetch
    class FakeResp:
        status_code = 200
        def raise_for_status(self): return None
        def json(self): return [{"res_state": "NY"}]

    def fake_get(url, params=None, timeout=None):       #to test endpoint and paging
        assert url == Ingestion.API_URL
        assert "$limit" in params and "$offset" in params
        return FakeResp()

    monkeypatch.setattr(Ingestion.requests, "get", fake_get)     #replaces requests, without touching global state
    rows = Ingestion.fetch_page(limit=5, offset=0)          #calls the function and validate the parsed result
    assert isinstance(rows, list) and rows[0]["res_state"] == "NY"

def test_fetch_page_rate_limit(monkeypatch):       #tests retry logic
    class HTTPError(Exception):
        def __init__(self, status): self.response = type("R", (), {"status_code": status})

    calls = {"n": 0}      #to see how many times fake get is called
    class OkResp:
        status_code = 200                   #fake response for retry call
        def raise_for_status(self): return None
        def json(self): return [{"ok": True}]

    def fake_get(url, params=None, timeout=None):         #first raise a HTTPError 429 and second return ok
        calls["n"] += 1
        if calls["n"] == 1:
            err = Ingestion.requests.exceptions.HTTPError()
            err.response = type("R", (), {"status_code": 429})
            raise err
        return OkResp()

    monkeypatch.setattr(Ingestion.requests, "get", fake_get)         #injecting fake behavior
    monkeypatch.setattr(Ingestion.time, "sleep", lambda s: None)       #avoid delay
    out = Ingestion.fetch_page(limit=5, offset=0)
    assert out == [{"ok": True}] and calls["n"] == 2    #retired once and returned second

def test_transform_rows_to_df_and_empty():       #both empty and non empty paths
    df_empty = Ingestion.transform_rows_to_df([])       #to ensure correct schema even when there is no data
    assert list(df_empty.columns) == Ingestion.KEEP_COLS and len(df_empty) == 0

    rows = [{"res_state": "CA", "sex": "Female"}]     
    df = Ingestion.transform_rows_to_df(rows)        #non empty gets into keep cols with values
    assert list(df.columns) == Ingestion.KEEP_COLS
    assert df.loc[0, "res_state"] == "CA"

def test_clean_df_basic():      #deduplicating
    df_in = pd.DataFrame([
        {
            "cdc_case_earliest_dt": "2020-07-01",
            "case_month": "2020-07",
            "res_state": " ny ",
            "age_group": "",
            "sex": None,
            "race": "nan",
            "ethnicity": None,
            "death_yn": " y ",
            "hosp_yn": " N ",
            "icu_yn": "n",
            "medcond_yn": "Unknown",
        },
        {
            "cdc_case_earliest_dt": "2020-07-01",
            "case_month": "2020-07",
            "res_state": " ny ",
            "age_group": "",
            "sex": None,
            "race": "nan",
            "ethnicity": None,
            "death_yn": " y ",
            "hosp_yn": " N ",
            "icu_yn": "n",
            "medcond_yn": "Unknown",
        },
    ])
    cleaned = Ingestion.clean_df(df_in)      #running clean logic
    assert len(cleaned) == 1        #duplicate row dropped
    assert cleaned.loc[cleaned.index[0], "res_state"] == "NY"    #trimmed and uppercased
    assert pd.api.types.is_datetime64_any_dtype(cleaned["cdc_case_earliest_dt"])     #verify earliest date is coerced to pandas
    cm = cleaned.loc[cleaned.index[0], "case_month"]    #accept both string or datetime
    parsed = pd.to_datetime(pd.Series([cm]), errors="coerce").iloc[0]
    assert pd.notna(parsed)

def test_load_df_to_postgres(monkeypatch):       
    df = pd.DataFrame([{"a": 1}])
    called = {}
    def fake_to_sql(self, name, con, if_exists, index, chunksize, method):      #records all fake sql arguments
        called.update(dict(name=name, if_exists=if_exists, index=index,
                           chunksize=chunksize, method=method, con=con))
    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=True)  #no real db involved

    Ingestion.load_df_to_postgres("fake_engine", df)     #curated loader, checks all names and flags
    assert called["name"] == "covid_case_surveillance"
    assert called["if_exists"] == "append"
    assert called["index"] is False
    assert called["chunksize"] == 10000
    assert called["method"] == "multi"

def test_load_df_to_postgres_raw(monkeypatch):    #for raw table
    df = pd.DataFrame([{"a": 1}])
    called = {}
    def fake_to_sql(self, name, con, if_exists, index, chunksize, method):
        called["name"] = name
    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=True)   #validates writes to raw table
    Ingestion.load_df_to_postgres_raw("fake_engine", df)
    assert called["name"] == "covid_case_surveillance_raw"


if __name__ == "__main__":        #main function torun tests
    import sys, pytest
    sys.exit(pytest.main(["-q", __file__]))
