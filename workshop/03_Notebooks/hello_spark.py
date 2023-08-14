#perezoso, apila las operaciones hasta que salen instrucciones con datos ejecuta el proceso
import pyspark

#4 nucleos y memoria de 4 gb
conf = pyspark.SparkConf()
conf.set('spark.executor.cores', '4')
conf.set('spark.cores.max', '4')
conf.set('spark.executor.memory', '4g')

#SparkContext Class
#local
sc = pyspark.SparkContext(master="local",appName="MyApp",conf=conf)

# remoto
#sc = pyspark.SparkContext(master="spark://10.10.22.162:7077",appName="MyApp",conf=conf)

'''
class pyspark.SparkContext (
master = None, es la url del cluster
appName = None, nombre de la app que se identifica con el cluster
sparkHome = None,
pyFiles = None,
environment = None,
batchSize = 0,
serializer = PickleSerializer(),
conf = None, lista de opciones de spark
gateway = None,
jsc = None,
profiler_cls = <class 'pyspark.profiler.BasicProfiler'> )
'''

sqlContext = pyspark.sql.SparkSession(sc)
input_data = sqlContext.read.csv("/home/nacho/Documents/Escuela_Met_Num_CIMAT/ZM_2015_pob.csv", header=True, inferSchema=True) #pandas
#Detalles del dataframe cargado
input_data.show(5, False)
type(input_data)
input_data.printSchema()

#Crear una vista para manejar los datos:
input_data.createOrReplaceTempView("zonas")
# seleccionar las columnas CVE_ZM, NOM_ZM y POB_2015, en caso de que el nombre de la columna
# Tenga caracteres conflictivos (como espacios o caracteres especiales), se pueden usar
# comillas invertidas `
result = sqlContext.sql("SELECT CVE_ZM, NOM_ZM, POB_2015 from zonas")
result = sqlContext.sql("SELECT first(CVE_ZM), NOM_ZM, sum(POB_2015) as POB_SUM from zonas group by NOM_ZM")
result = sqlContext.sql("SELECT CVE_ZM, NOM_ZM, POB_2015 from zonas")
result = sqlContext.sql("SELECT CVE_ZM, NOM_ZM, POB_2015 from zonas where CVE_ZM > 2")
result.show()


#result.show()result.select("*").withColumn("CV_int")
result.collect() #todo lo que tiene el df
#result.show(30)
r2 = result.limit(10)

r2.collect()
