#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="bigdata_dpy",
    version="0.1",

    packages=find_packages(),
    scripts=["scripts/hdpy", "scripts/spark_dpy", "scripts/mahout_dpy",
             "scripts/hadoop_engine",
             "scripts/cassandra_dpy", "scripts/mongo_dpy"],

    install_requires=["execo", "networkx"],

    # PyPI
    author='Miguel Liroz Gistau',
    author_email='miguel.liroz_gistau@inria.fr',
    description="A collection of scripts and packages that help in the "
                "deployment and experimental evaluation of Hadoop in Grid5000.",
    url="https://github.com/mliroz/hadoop",
    license="BSD",
    keywords="hadoop spark mongodb cassandra hive g5k grid5000 execo",
  
)