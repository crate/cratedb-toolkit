from cratedb_toolkit.util.platform import PlatformInfo


def test_platforminfo_application():
    pi = PlatformInfo()
    outcome = pi.application()
    assert "name" in outcome
    assert "version" in outcome
    assert "platform" in outcome


def test_platforminfo_libraries():
    pi = PlatformInfo()
    outcome = pi.libraries()
    assert isinstance(outcome, dict)
