from setuptools import setup

VERSION = "0.0.1"

setup(
    name = "tntqueue",
    packages = ["tntqueue"],
    install_requires = ["msgpack-python", "tarantool"],
    test_suite = "tests.test_queue",
    version = VERSION,
    platforms = ["all"],
    author = "Eugine Blikh",
    author_email = "bigbes@gmail.com",
    url = "https://github.com/bigbes92/tnt-pyq",
    license = "MIT",
    description = "Python bindings for tarantool Queue",
    long_description = open("README.md").read(),
)
