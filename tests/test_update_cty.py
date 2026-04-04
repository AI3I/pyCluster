from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_update_cty_module():
    script = Path(__file__).resolve().parents[1] / "scripts" / "update_cty.py"
    spec = importlib.util.spec_from_file_location("update_cty_script", script)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_update_cty_main_refreshes_cty_and_wpxloc(tmp_path, monkeypatch, capsys) -> None:
    mod = _load_update_cty_module()
    config = tmp_path / "config" / "pycluster.toml"
    config.parent.mkdir(parents=True)
    config.write_text(
        "[node]\nnode_call='N0NODE-1'\nnode_alias='N0NODE'\nowner_name='Sysop'\nqth='Somewhere'\nnode_locator=''\nmotd='hi'\nbranding_name='pyCluster'\nwelcome_title='Welcome'\nwelcome_body=''\nlogin_tip=''\nshow_status_after_login=true\nrequire_password=true\nsupport_contact=''\nwebsite_url=''\nprompt_template='[{timestamp}] {node}{suffix}'\n"
        "[telnet]\nhost='127.0.0.1'\nport=7300\nmax_clients=10\nidle_timeout_seconds=0\nmax_line_length=512\n"
        "[web]\nhost='127.0.0.1'\nport=8080\nadmin_token=''\n"
        "[public_web]\nenabled=true\nhost='127.0.0.1'\nport=8081\nstatic_dir=''\ncty_dat_path='./data/cty.dat'\nwpxloc_raw_path='./data/wpxloc.raw'\n"
        "[store]\nsqlite_path='./data/pycluster.db'\n",
        encoding="utf-8",
    )
    calls: list[str] = []

    def fake_download(url: str, label: str, min_size: int = 65536) -> bytes:
        calls.append(label)
        return (label + " sample data\n").encode("ascii") * 5000

    monkeypatch.setattr(mod, "_download", fake_download)
    monkeypatch.setattr(mod, "_validate_cty", lambda path: (1234, 56))
    monkeypatch.setattr(mod, "_validate_wpxloc", lambda path: (2345, 67))
    monkeypatch.setattr("sys.argv", ["update_cty.py", "--config", str(config)])

    rc = mod.main()
    assert rc == 0
    out = capsys.readouterr().out
    assert "CTY.DAT updated" in out
    assert "WPXLOC.RAW updated" in out
    assert calls == ["CTY.DAT", "WPXLOC.RAW"]
    assert (tmp_path / "data" / "cty.dat").exists()
    assert (tmp_path / "data" / "wpxloc.raw").exists()


def test_update_cty_main_honors_cty_only(tmp_path, monkeypatch, capsys) -> None:
    mod = _load_update_cty_module()
    config = tmp_path / "config" / "pycluster.toml"
    config.parent.mkdir(parents=True)
    config.write_text(
        "[node]\nnode_call='N0NODE-1'\nnode_alias='N0NODE'\nowner_name='Sysop'\nqth='Somewhere'\nnode_locator=''\nmotd='hi'\nbranding_name='pyCluster'\nwelcome_title='Welcome'\nwelcome_body=''\nlogin_tip=''\nshow_status_after_login=true\nrequire_password=true\nsupport_contact=''\nwebsite_url=''\nprompt_template='[{timestamp}] {node}{suffix}'\n"
        "[telnet]\nhost='127.0.0.1'\nport=7300\nmax_clients=10\nidle_timeout_seconds=0\nmax_line_length=512\n"
        "[web]\nhost='127.0.0.1'\nport=8080\nadmin_token=''\n"
        "[public_web]\nenabled=true\nhost='127.0.0.1'\nport=8081\nstatic_dir=''\ncty_dat_path='./data/cty.dat'\nwpxloc_raw_path='./data/wpxloc.raw'\n"
        "[store]\nsqlite_path='./data/pycluster.db'\n",
        encoding="utf-8",
    )
    calls: list[str] = []

    def fake_download(url: str, label: str, min_size: int = 65536) -> bytes:
        calls.append(label)
        return (label + " sample data\n").encode("ascii") * 5000

    monkeypatch.setattr(mod, "_download", fake_download)
    monkeypatch.setattr(mod, "_validate_cty", lambda path: (1234, 56))
    monkeypatch.setattr(mod, "_validate_wpxloc", lambda path: (2345, 67))
    monkeypatch.setattr("sys.argv", ["update_cty.py", "--config", str(config), "--cty-only"])

    rc = mod.main()
    assert rc == 0
    out = capsys.readouterr().out
    assert "CTY.DAT updated" in out
    assert "WPXLOC.RAW" not in out
    assert calls == ["CTY.DAT"]
    assert (tmp_path / "data" / "cty.dat").exists()
    assert not (tmp_path / "data" / "wpxloc.raw").exists()
