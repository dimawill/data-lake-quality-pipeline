import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location("ingest", Path(__file__).resolve().parents[1] / "scripts/ingest.py")
ingest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ingest)


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self, cap):
        return self.data[:cap]


VALID = b"trip_id,trip_start_timestamp,trip_miles,fare\nt1,2023-01-01T00:00:00,1,4\n"


def test_ingest_bounded_sample_and_upload(monkeypatch, capsys):
    seen = {}

    def open_url(url, timeout):
        seen["url"] = url
        assert timeout == 30
        return FakeResponse(VALID)

    def run(command, input, check):
        seen["command"] = command
        assert input == VALID and check
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(ingest.urllib.request, "urlopen", open_url)
    monkeypatch.setattr(ingest.subprocess, "run", run)
    monkeypatch.setattr(sys, "argv", ["ingest.py", "--bucket", "my-raw", "--limit", "5"])
    ingest.main()
    assert "%24limit=5" in seen["url"]
    assert seen["command"] == ["aws", "s3", "cp", "-", "s3://my-raw/trips/sample.csv", "--sse", "AES256"]
    assert "Uploaded bounded sample" in capsys.readouterr().out


@pytest.mark.parametrize("data", [b"", b"wrong,header\na,b\n", VALID[:VALID.index(b"\nt1")] + b"\n"])
def test_refuses_empty_or_wrong_schema(monkeypatch, data):
    monkeypatch.setattr(ingest.urllib.request, "urlopen", lambda *_args, **_kwargs: FakeResponse(data))
    monkeypatch.setattr(ingest.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("Should not upload"))
    monkeypatch.setattr(sys, "argv", ["ingest.py", "--bucket", "my-raw"])
    with pytest.raises(ValueError, match="schema"):
        ingest.main()


def test_refuses_more_than_2mb(monkeypatch):
    monkeypatch.setattr(ingest.urllib.request, "urlopen", lambda *_args, **_kwargs: FakeResponse(b"x" * (ingest.MAX_BYTES + 1)))
    monkeypatch.setattr(sys, "argv", ["ingest.py", "--bucket", "my-raw"])
    with pytest.raises(ValueError, match="2 MB"):
        ingest.main()


def test_limit_validation_before_request(monkeypatch):
    monkeypatch.setattr(ingest.urllib.request, "urlopen", lambda *_args, **_kwargs: pytest.fail("Should not fetch"))
    monkeypatch.setattr(sys, "argv", ["ingest.py", "--bucket", "my-raw", "--limit", "1001"])
    with pytest.raises(SystemExit):
        ingest.main()
