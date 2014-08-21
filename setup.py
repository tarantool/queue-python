import codecs
import os
import re
from setuptools import setup
import sys

cmdclass = {}

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

try:
    from sphinx.setup_command import BuildDoc
    cmdclass["build_sphinx"] = BuildDoc
except ImportError:
    pass

def read(*parts):
    filename = os.path.join(os.path.dirname(__file__), *parts)
    with codecs.open(filename, encoding='utf-8') as fp:
        return fp.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

setup(name='tarantool-queue',
      version=find_version('tarantool_queue', '__init__.py'),
      description='Python bindings for Tarantool queue script (http://github.com/tarantool/queue)',
      long_description=read('README.rst'),
      author='Eugine Blikh',
      author_email='bigbes@gmail.com',
      maintainer='Eugine Blikh',
      maintainer_email='bigbes@gmail.com',
      license='MIT',
      packages=['tarantool_queue'],
      platforms = ["all"],
      install_requires=[
            'msgpack-python',
            'tarantool<0.4'
          ],
      url='http://github.com/tarantool/tarantool-queue-python',
      test_suite='tests.test_queue',
      tests_require=[
            'msgpack-python',
            'tarantool'
          ],
      classifiers=[
            'Development Status :: 4 - Beta',
            'Operating System :: OS Independent',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Topic :: Database :: Front-Ends',
            'Environment :: Console'
          ],
      cmdclass = cmdclass
)
