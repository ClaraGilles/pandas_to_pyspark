from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Données
matrix = [

[1, 34, 'Cardiology', 10],
[2, 45, 'Neurology', 12],
[3, 23, 'Cardiology', 5],
[4, 64, 'Orthopedics', 8],
[5, 52, 'Cardiology', 9],

]

columns = ['patient_id', 'age', 'department', 'visit_count']

df = spark.createDataFrame(matrix, columns)

df.show()

# GroupBy et calculs statistiques
