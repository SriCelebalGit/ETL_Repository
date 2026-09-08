"""Gold layer transformation modules.

Each module exposes:

    def transform(spark, ctx) -> DataFrame

`ctx` is a framework.gold.TransformContext carrying the target identity, the control
row's `parameters` map, and helpers to read silver and already-built gold tables. The
function returns a DataFrame; the framework performs the write according to the control
row's load_type, so a transformation never issues a write of its own.

That separation is what makes these unit testable - see tests/test_transformations.py.
"""
