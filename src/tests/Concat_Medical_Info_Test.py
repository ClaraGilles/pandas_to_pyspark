from files_pandas.Concat_Medical_Info import conc_lower_pd
from files_pyspark.Concat_Medical_Info_PySpark import conc_lower_ps

from pyspark.testing.utils import assertDataFrameEqual

def test_agg():
    assertDataFrameEqual(conc_lower_pd(), conc_lower_ps().toPandas())
