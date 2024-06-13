# Releasing


## About

When running the release procedure, several GitHub Action workflows will be
triggered, building and publishing different kinds of artefacts.

- Python source distribution and wheel packages, published to the Python Package Index (PyPI).

  https://pypi.org/project/cratedb-toolkit/

- OCI container images, published to the GitHub Container Registry (GHCR).

  https://github.com/crate-workbench/cratedb-toolkit/pkgs/container/cratedb-toolkit

The signal to start the release pipeline is by tagging the Git repository,
and pushing that tag to remote.


## Procedure

On branch `main`:

- Add a section for the new version in the `CHANGES.md` file.
- Commit your changes with a message like `Release x.y.z`.
- Create a tag, and push to remote.
  This will trigger a GitHub action which releases the new version to PyPi.
  ```shell
  git tag v0.0.14
  git git push && push --tags
  ```
- On GitHub, designate a new release, copying in the relevant section
  from the CHANGELOG.
  https://github.com/crate-workbench/cratedb-toolkit/releases

Optionally, build the package and upload to PyPI manually.
```shell
poe release
```
