// Databricks notebook source
// MAGIC %md
// MAGIC Où sont sockées les données

// COMMAND ----------

val root = "/FileStore/tables"

// COMMAND ----------

// MAGIC %md
// MAGIC Créer SparkSession et SparkContext

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.getOrCreate()
val sc = spark.sparkContext

// COMMAND ----------

// MAGIC %md
// MAGIC Inspectez le Spark Context

// COMMAND ----------

val config=sc.getConf.getAll

for (conf <- config)
  println(conf._1 +", "+ conf._2)

// COMMAND ----------

// MAGIC %md
// MAGIC * Internally, each RDD is characterized by five main properties:
// MAGIC  *  A list of partitions
// MAGIC  *  A function for computing each split
// MAGIC  *  A list of dependencies on other RDDs
// MAGIC  *  Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
// MAGIC  *  Optionally, a list of preferred locations to compute each split on (e.g. block locations for
// MAGIC  *    an HDFS file)
// MAGIC  '''

// COMMAND ----------

// MAGIC %md
// MAGIC * Créez un RDD avec valeurs 'Ana' et 'Bob'
// MAGIC * Visualisez-le
// MAGIC * Recréez plusieurs fois en changeant le nombre des partiotions
// MAGIC * Affichez le nombre de partitions

// COMMAND ----------

val namelist = List("Ana","Bob")

// COMMAND ----------

val nameRDD = sc.parallelize(namelist)
nameRDD.collect()

// COMMAND ----------

nameRDD.getNumPartitions

// COMMAND ----------

val nameRDD = sc.parallelize(namelist, 5)
nameRDD.collect()

// COMMAND ----------

nameRDD.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC * Créer un RDD à partir du RDD avec valeurs, 'Ana', 'Bob'
// MAGIC * Les valeurs du nouveau RDD sont 'Ana2nd', 'Bob2nd'

// COMMAND ----------

val anotherRDD = nameRDD.map(y => y + "2nd")
anotherRDD.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Créer RDD à partir du fichier 'sample.txt' et affichier les premières 5 lignes

// COMMAND ----------

// Create RDD from external Data source
import java.io.File

val path = new File(root,"sample.txt").getPath

val fileRDD = spark.sparkContext.textFile(path)

fileRDD.take(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Créer a un RDD de tuples à partir du RDD avec les noms  
// MAGIC Il devrait contenir ('Ana', 20), ('Bob', 20)

// COMMAND ----------

val pairRDD = nameRDD.map(x => (x,20))
pairRDD.collect

// COMMAND ----------

// MAGIC %md
// MAGIC ### map V.S. flatmap  
// MAGIC à partir d'un RDD avec valeurs  
// MAGIC `Array(Array('Ana','Bob'),Array('Caren'))`  
// MAGIC use map or flatMap to return:  
// MAGIC 1: `Array('Ana', 'Bob', 'plus', 'Caren', 'plus')`  
// MAGIC 2: `Array(Array('Ana', 'Bob', 'plus'), Array('Caren', 'plus'))`  
// MAGIC Utilisez `++` pour les concatenations  
// MAGIC 
// MAGIC Faites le même chose avec `List` et `Seq` 

// COMMAND ----------

val namelist = Array(Array("Ana","Bob"),Array("Caren"))
val nameRDD = sc.parallelize(namelist)

// COMMAND ----------

nameRDD.flatMap(x => x ++ Array("plus")).collect()

// COMMAND ----------

nameRDD.map(x => x ++ Array("plus")).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Avec `List()`

// COMMAND ----------

val namelist = List(List("Ana","Bob"),List("Caren"))
val nameRDD = sc.parallelize(namelist)

// COMMAND ----------

nameRDD.flatMap(x => x ++ List("plus")).collect()

// COMMAND ----------

nameRDD.map(x => x ++ List("plus")).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Avec `Seq`

// COMMAND ----------

val namelist = Seq(Seq("Ana","Bob"),Seq("Caren"))
val nameRDD = sc.parallelize(namelist)

// COMMAND ----------

nameRDD.flatMap(x => x ++ Seq("plus")).collect()

// COMMAND ----------

nameRDD.map(x => x ++ Seq("plus")).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Agrégation
// MAGIC à partir d'une liste de nombres, partitionnez-la en 3 partitions,
// MAGIC * Utiliser map et reduce et calculez la moyenne
// MAGIC * aggregate to calculate its average

// COMMAND ----------

val numRDD = sc.parallelize(List(1,2,3,4,5),3)

// COMMAND ----------

val Array(acc, cnt) = numRDD.map(x => Array(x, 1)).reduce((x, y) => Array(x(0) + y(0), x(1) + y(1)))

// COMMAND ----------

println(acc, cnt, acc/cnt)
