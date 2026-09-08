"""Metadata driven ETL framework for Databricks (Auto Loader based, no DLT).

Layout
------
config          framework level settings resolved from conf/framework.<env>.yml
control         control table repository + YAML -> control table metadata loader
audit           job / DQ / file audit writers
ingestion       Auto Loader landing -> bronze ingestion
dq              rule engine and the python rule function registry
transform       SCD1 / SCD2 / append / overwrite Delta writers
gold            transformation dispatcher for the gold layer
"""

__version__ = "1.0.0"
