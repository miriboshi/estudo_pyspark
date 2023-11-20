from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Curso de Pyspark').getOrCreate()

print ("Deu certo! Parabains")


df = spark.read.csv('./DATASETS/LOGINS.csv', sep=';', header = True)
df.show()

