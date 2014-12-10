__author__ = "andrew.kelleher@buzzfeed.com (Andrew Kelleher)"

try:
  from setuptools import setup, find_packages
except ImportError:
  import distribute_setup
  distribute_setup.use_setuptools()
  from setuptools import setup, find_packages

setup(
    name='disref',
    version='0.1',
    packages=find_packages(),
    author='Andrew Kelleher', 
    author_email='andrew.kelleher@buzzfeed.com', 
    description='Provides easy, fault tolerant, distributed references with redis as a backend.',
    test_suite='test',
    install_requires=[
        'sherlock==0.3.0',
        'pytz==2014.10',
    ],
    url='http://www.github.com/buzzfeed/disref',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
    ],
    keywords="distributed reference references aggregation pipeline big data online algorithm"
)
