from setuptools import setup
import sys

VERSION = '0.0.2'
cmdclass = {}

if 'upload' in sys.argv or 'register' in sys.argv or 'tarball' in sys.argv:
    import tntqueue
    assert tntqueue.VERSION == VERSION

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
      version=VERSION,
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
      url='http://github.com/bigbes92/tnt-pyq',
      test_suite='tests.test_queue',
      tests_require=[
            'msgpack-python',
            'tarantool'
          ],
      classifiers=[
            'Development Status :: 3 - Alpha',
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
