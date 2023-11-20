from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName('Curso de Pyspark')
		.config('spark.sql.repl.eagerEval.enabled', True)
        .getOrCreate()
         )

print ("Deu certo! Parabains")

#formata os campos e lê .csv, mas não retorna em formato de tabela
#campos muito longos quebrados
df = spark.read.csv('./DATASETS/LOGINS.csv', sep=';', header=True)
df.show()


#transformação em tabela
#spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
df

