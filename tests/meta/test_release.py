from cratedb_toolkit.meta.release import CrateDBRelease


def test_release_stable():
    cr = CrateDBRelease()
    assert cr.stable.category == "stable"
    assert cr.stable.version >= "5.10.5"
    assert cr.stable.date >= "2025-04-30"
    assert cr.stable.type == "tarball"
    assert cr.stable.url >= "https://cdn.crate.io/downloads/releases/crate-5.10.5.tar.gz"
    assert cr.stable.cloud_version.startswith("stable-")
    assert cr.stable.cloud_version >= "stable-5.10.5"


def test_release_testing():
    cr = CrateDBRelease()
    assert cr.testing.category == "testing"
    assert cr.testing.version >= "5.10.5"
    assert cr.testing.date >= "2025-04-28"
    assert cr.testing.type == "tarball"
    assert cr.testing.url >= "https://cdn.crate.io/downloads/releases/crate-5.10.5.tar.gz"
    assert cr.testing.cloud_version.startswith("testing-")
    assert cr.testing.cloud_version >= "testing-5.10.5"


def test_release_nightly():
    cr = CrateDBRelease()
    assert cr.nightly.category == "nightly"
    assert cr.nightly.version >= "6.0.0"
    assert cr.nightly.date >= "2025-05-01"
    assert cr.nightly.type == "tarball"
    assert cr.nightly.url >= "https://cdn.crate.io/downloads/releases/crate-5.10.5.tar.gz"
    assert cr.nightly.cloud_version.startswith("nightly-")
    assert cr.nightly.cloud_version >= "nightly-6.0.0-2025-05-01-00-02"
