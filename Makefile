html:
	python setup.py build_sphinx
test:
	python setup.py test
upload:
	python setup.py register sdist --format=gztar,zip upload
