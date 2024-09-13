from files_pandas.Agg_Departments import agg_dep_pd
from files_pyspark.Agg_Departments_PySpark import agg_dep_PS

from pyspark.testing.utils import assertDataFrameEqual

def test_agg():
    assertDataFrameEqual(agg_dep_pd(), agg_dep_PS().toPandas())
