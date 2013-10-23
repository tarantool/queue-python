from setuptools import setup

VERSION = "0.0.1"

cmdclass = {}

try:
    from tests.setup_commands import test
    cmdclass["tests"] = tests
except ImportError:
    pass

setup(
    name = "tntqueue",
    packages = ["tntqueue"],
    version = VERSION,
    platforms = ["all"],
    author = "Eugine Blikh",
    author_email = "bigbes@gmail.com",
    url = "https://github.com/bigbes92/tnt-pyq",
    license = "MIT",
    description = "Python bindings for tarantool Queue",
    long_description = open("README.md").read(),
    cmdclass = cmdclass
)
