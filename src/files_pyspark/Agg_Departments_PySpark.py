from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def agg_dep_PS():

    # Crée une session Spark
    spark = SparkSession.builder.master("local[*]").appName("MigrationExample").getOrCreate()

    # Données
    matrix = [

    [1, 34, 'Cardiology', 10],
    [2, 45, 'Neurology', 12],
    [3, 23, 'Cardiology', 5],
    [4, 64, 'Orthopedics', 8],
    [5, 52, 'Cardiology', 9],

    ]

    # Colonnes
    columns = ['patient_id', 'age', 'department', 'visit_count']

    df = spark.createDataFrame(matrix, columns)

    agg_df = df.groupBy(
        "department").agg(
        F.sum("visit_count").alias("total_visit_count"),  # Somme de la colonne 'visit_count'
        F.mean("age").alias("mean_age"),                 # Moyenne de la colonne 'age'
        F.max("age").alias("max_age")                    # Valeur maximale de la colonne 'age'
    )

    return agg_df
