__author__ = "andrew.kelleher@buzzfeed.com (Andrew Kelleher)"

try:
    from setuptools import setup, find_packages
except ImportError:
    import distribute_setup
    distribute_setup.use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='phonon',
    version='0.7',
    packages=find_packages(),
    author='Andrew Kelleher, Matthew Semanyshyn',
    author_email='andrew.kelleher@buzzfeed.com, matthew.semanyshyn@buzzfeed.com',
    description='Provides easy, fault tolerant, distributed references with redis as a backend.',
    test_suite='test',
    tests_require=[
        'autopep8==1.1.1',
        'mock==1.0.1',
        'nose==1.2.1',
        'coverage==3.7.1',
    ],
    install_requires=[
        'pytz==2014.10',
        'python-dateutil==2.4.1',
        'redis==2.10.3',
    ],
    url='http://www.github.com/buzzfeed/phonon',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Development Status :: 3 - Alpha',
    ],
    keywords="distributed reference references aggregation pipeline big data online algorithm"
)
