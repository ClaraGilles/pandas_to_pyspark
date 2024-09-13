from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def missing_values_ps():
    # Crée une session Spark
    spark = SparkSession.builder.master("local[*]").appName("MigrationExample").getOrCreate()

    # Données dans les colonnes
    data = [
        (1, 34.0, 'Cardiology'),
        (2, float('nan'), 'Neurology'),
        (3, 50.0, 'Orthopedics'),
        (4, float('nan'), float('nan')),
        (5, 15.0, 'Neurology')
    ]

    # Noms des colonnes
    columns = ['patient_id', 'age', 'department']

    # Création du DataFrame PySpark
    df = spark.createDataFrame(data, columns)

    # Filtrer les lignes où 'age' n'est pas NaN avant de calculer la moyenne
    df_filtered = df.filter(~F.isnan(F.col('age')))

    # Calcul de la moyenne de la colonne 'age', en ignorant les NaN
    mean_age = df_filtered.select(F.mean('age')).first()[0]

    df = df.withColumn('age', F.when(F.isnan('age'), mean_age).otherwise(F.col('age')))

    df = df.withColumn('department',
                    F.when(F.isnan('department'), 'Unknown').otherwise(F.col('department')))

    return df
