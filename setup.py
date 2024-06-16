# This is a shim to allow GitHub to decode the package in order to provide
# the "Used by" section on the project homepage.
import setuptools

if __name__ == "__main__":
    setuptools.setup(name="cratedb-toolkit")
