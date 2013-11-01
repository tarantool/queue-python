from setuptools import setup
import sys

cmdclass = {}

from tarantool_queue import __version__

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

try:
    from sphinx.setup_command import BuildDoc
    cmdclass["build_sphinx"] = BuildDoc
except ImportError:
    pass

setup(name='tarantool-queue',
      version=__version__,
      description='Python bindings for Tarantool queue script (http://github.com/tarantool/queue)',
      long_description=open('README.rst').read(),
      author='Eugine Blikh',
      author_email='bigbes@gmail.com',
      maintainer='Eugine Blikh',
      maintainer_email='bigbes@gmail.com',
      license='MIT',
      packages=['tarantool_queue'],
      platforms = ["all"],
      install_requires=[
            'msgpack-python',
            'tarantool'
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
