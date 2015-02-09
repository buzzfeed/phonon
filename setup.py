__author__ = "andrew.kelleher@buzzfeed.com (Andrew Kelleher)"

try:
  from setuptools import setup, find_packages
except ImportError:
  import distribute_setup
  distribute_setup.use_setuptools()
  from setuptools import setup, find_packages

setup(
    name='phonon',
    version='0.5',
    packages=find_packages(),
    author='Andrew Kelleher, Matthew Semanyshyn',
    author_email='andrew.kelleher@buzzfeed.com, matthew.semanyshyn@buzzfeed.com',
    description='Provides easy, fault tolerant, distributed references with redis as a backend.',
    test_suite='test',
    install_requires=[
        'pytz==2014.10',
    ],
    url='http://www.github.com/buzzfeed/phonon',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Development Status :: 3 - Alpha', 
    ],
    keywords="distributed reference references aggregation pipeline big data online algorithm"
)
