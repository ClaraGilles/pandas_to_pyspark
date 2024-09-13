from files_pandas.Missing_Values_Handling import missing_values_pd
from files_pyspark.Missing_Values_Handling_PySpark import missing_values_ps

from pyspark.testing.utils import assertDataFrameEqual

def test_nan():
    assertDataFrameEqual(missing_values_pd(), missing_values_ps().toPandas())
