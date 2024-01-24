// Databricks notebook source
// MAGIC %md
// MAGIC Où sont sockées les données

// COMMAND ----------

val root = "/FileStore/tables"

// COMMAND ----------

// MAGIC %md
// MAGIC Créer SparkSession et SparkContext

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Inspectez le Spark Context

// COMMAND ----------



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



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC * Créer un RDD à partir du RDD avec valeurs, 'Ana', 'Bob'
// MAGIC * Les valeurs du nouveau RDD sont 'Ana2nd', 'Bob2nd'

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Créer RDD à partir du fichier 'sample.txt' et affichier les premières 5 lignes

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Créer a un RDD de tuples à partir du RDD avec les noms  
// MAGIC Il devrait contenir ('Ana', 20), ('Bob', 20)

// COMMAND ----------



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



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Agrégation
// MAGIC à partir d'une liste de nombres, partitionnez-la en 3 partitions,
// MAGIC * Utiliser map et reduce et calculez la moyenne
// MAGIC * aggregate to calculate its average

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------


