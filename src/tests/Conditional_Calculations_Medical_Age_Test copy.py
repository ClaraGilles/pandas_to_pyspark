from files_pandas.Conditional_Calculations_Medical_Age import age_condi_pd
from files_pyspark.Conditional_Calculations_Medical_Age_PySpark import age_condi_ps

from pyspark.testing.utils import assertDataFrameEqual

def test_age():
    assertDataFrameEqual(age_condi_pd(), age_condi_ps().toPandas())
