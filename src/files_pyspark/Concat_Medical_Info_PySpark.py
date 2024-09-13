from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Crée une session Spark
spark = SparkSession.builder.master("local[*]").appName("MigrationExample").getOrCreate()



def conc_lower_ps():
    data = [
    ('John Doe', 'Diabetes'),
    ('Jane Smith', 'Heart Disease'),
    ('Alice Brown', 'Hypertension')
    ]

# Création du DataFrame PySpark
    columns = ['patient_name', 'diagnosis']
    df = spark.createDataFrame(
            data, schema=columns)

    # Conversion en minuscules et ajout d'un champ
    df = df.withColumn(
        "diagnosis_lower", F.lower("diagnosis"))
    df = df.withColumn(
        "full_info", F.concat_ws(' - ',
                        df.patient_name,
                        df.diagnosis_lower))

    return df
