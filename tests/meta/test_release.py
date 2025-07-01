from verlib2 import Version

from cratedb_toolkit.meta.release import CrateDBRelease


def test_release_stable():
    cr = CrateDBRelease()
    assert cr.stable.category == "stable"
    assert Version(cr.stable.version) >= Version("5.10.5")
    assert cr.stable.date >= "2025-04-30"
    assert cr.stable.type == "tarball"
    assert cr.stable.cloud_version.startswith("stable-")


def test_release_testing():
    cr = CrateDBRelease()
    assert cr.testing.category == "testing"
    assert Version(cr.testing.version) >= Version("5.10.5")
    assert cr.testing.date >= "2025-04-28"
    assert cr.testing.type == "tarball"
    assert cr.testing.cloud_version.startswith("testing-")


def test_release_nightly():
    cr = CrateDBRelease()
    assert cr.nightly.category == "nightly"
    assert Version(cr.nightly.version) >= Version("6.0.0")
    assert cr.nightly.date >= "2025-05-01"
    assert cr.nightly.type == "tarball"
    assert cr.nightly.cloud_version.startswith("nightly-")
