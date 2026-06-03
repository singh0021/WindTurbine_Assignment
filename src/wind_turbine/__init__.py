"""Reusable, framework-agnostic transformation logic for the wind turbine pipeline.

Everything in this package is plain PySpark that operates on DataFrames and has no
dependency on the Declarative Pipelines (``dlt``) runtime. That separation is
deliberate: the pipeline modules in ``src/pipelines`` are thin ``@dlt.table``
wrappers around these functions, while the test suite exercises the functions
directly on a local SparkSession. Pure transformation logic -> easy unit tests.
"""
