from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def age_condi_ps():

    # Crée une session Spark
    spark = SparkSession.builder.master("local[*]").appName("MigrationExample").getOrCreate()

    # Données sous forme de liste de tuples
    data = [
        (1, 34, 'Cardiology'),
        (2, 70, 'Neurology'),
        (3, 50, 'Orthopedics'),
        (4, 20, 'Cardiology'),
        (5, 15, 'Neurology')
    ]

    # Création du DataFrame PySpark
    columns = ['patient_id', 'age', 'department']
    df = spark.createDataFrame(
        data, schema=columns)

    df.show()

    # Ajout d'une colonne conditionnelle (catégorie d'âge)
    df = df.withColumn(
        'age_category',
        F.when(df.age > 60, 'senior')
        .when(df.age > 18, 'adult')
        .otherwise('minor'))

    return df
