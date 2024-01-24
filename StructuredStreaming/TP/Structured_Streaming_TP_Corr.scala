// Databricks notebook source
// MAGIC %md
// MAGIC # TP final: utlisation the Spark StructuredStreaming et Spark DataFrames en général
// MAGIC Ce TP va etre moins detaillé et vous laisse plus de liberté de jouer avec les donneés

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// COMMAND ----------

val spark = SparkSession.builder.getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC Chargez les fichiers sur '/databricks-datasets/iot-stream/data-device' comme DataFrame et inspectez-les  
// MAGIC 
// MAGIC Faites les opérations habituelles

// COMMAND ----------

val inputPath = "/databricks-datasets/iot-stream/data-device"

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/iot-stream/data-device

// COMMAND ----------


val df = spark.read.format("json").load(inputPath)

// COMMAND ----------

df.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez le schéma et créer une structure avec ce schéma  
// MAGIC Ceci est nécessaire pour créer un readStream ou le schéma doit être fourni

// COMMAND ----------

df.printSchema()

// COMMAND ----------

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import scala.jdk.CollectionConverters

val jsonSchema = StructType(
  Array( 
    StructField("calories_burnt", DoubleType, true),
    StructField("device_id", LongType, true),
    StructField("id", LongType, true),
    StructField("miles_walked", DoubleType, true),
    StructField("num_steps", LongType, true),
    StructField("timestamp", TimestampType, true),
    StructField("user_id", LongType, true),
    StructField("value", StringType, true)
  )
)

// COMMAND ----------

// MAGIC %md
// MAGIC Créez un objet readStream et relisez les fichiers ; utiliser le schéma créé avant

// COMMAND ----------

val streamingInputDF = (
  spark
    .readStream
    .format("file")
    .schema(jsonSchema)
    .option("maxFilesPerTrigger", 1)  // Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)

// COMMAND ----------

// MAGIC %md
// MAGIC Créer la requête liée au readStream  
// MAGIC 
// MAGIC Utiliser une fenêtre (choisir la durée et la séparation) et grouper par cette fenêtre et l'id de l'appareil  
// MAGIC 
// MAGIC Agréger ensemble min, max, moyenne, somme de calories_burnt, miles_walked, num_steps 

// COMMAND ----------

val streamingAggDF = (                 
  streamingInputDF
  .withWatermark("timestamp", "1 minutes")
  .groupBy(
    window($"timestamp", "10 hour", "10 hour"),
    streamingInputDF("device_id")
  ).agg(
    max(streamingInputDF("calories_burnt").alias("max_cal")),
    max(streamingInputDF("miles_walked").alias("max_miles")),
    max(streamingInputDF("num_steps").alias("max_steps")),
    min(streamingInputDF("calories_burnt").alias("min_cal")),
    min(streamingInputDF("miles_walked").alias("min_miles")),
    min(streamingInputDF("num_steps").alias("min_steps")),
    avg(streamingInputDF("calories_burnt").alias("avg_cal")),
    avg(streamingInputDF("miles_walked").alias("avg_miles")),
    avg(streamingInputDF("num_steps").alias("avg_strps")),
    sum(streamingInputDF("calories_burnt").alias("sum_cal")),
    sum(streamingInputDF("miles_walked").alias("sum_miles")),
    sum(streamingInputDF("num_steps").alias("sum_steps"))
  )
)

// COMMAND ----------

// MAGIC %md
// MAGIC Assurez-vous que la source est en streaming

// COMMAND ----------

streamingAggDF.isStreaming

// COMMAND ----------

// MAGIC %md
// MAGIC Créer l'objet writeStream avec format "memory"  
// MAGIC Ceci sera le data sink

// COMMAND ----------

val query = (
  streamingAggDF
    .writeStream
    .format("memory")        // memory = store in-memory table 
    .queryName("aggs")       // counts = name of the in-memory table
    .outputMode("complete")  // complete = all the counts should be in the table
    .start()
)

// COMMAND ----------

// MAGIC %md
// MAGIC Faites des requêtes répétées au data sink pour vous assurer qu'il reçoit des données  
// MAGIC Choisissez n'importe quelle requête; compter le nombre d'éléments suffit

// COMMAND ----------

spark.sql("select count(*) from aggs").show()

// COMMAND ----------

spark.sql("select count(*) from aggs").show()

// COMMAND ----------

spark.sql("select count(*) from aggs").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Arrêtez la requête soit avec une instruction, soit en cliquant sur la cellule

// COMMAND ----------

query.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC Créer un autre objet writeStream avec la même requête  
// MAGIC Cette fois on va écrire sur des fichiers  
// MAGIC Le mode n'est pas "complete" mais "append"  
// MAGIC Donc n'oubliez pas le waterMark  
// MAGIC Attention: withWatermark(df.time...) ne marche pas, il faut utliser withWatermark("time"...)

// COMMAND ----------

val streamingAggDF = (                 
  streamingInputDF
  .withWatermark("timestamp", "1 hours")
  .groupBy(
    window($"timestamp", "1 hours", "1 hours"),
    streamingInputDF("device_id")
  ).count()
)

// COMMAND ----------

// MAGIC %md
// MAGIC Sauvegardez le datasink sur un répertoire an utilisant un format adapté ; essayez différents formats.
// MAGIC Est-ce que csv marche ? json ? parquet ?

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/test_streaming_cp

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/test_streaming_csv

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/test_streaming_aggs

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/aggs_parquet

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/aggs

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/aggs_c

// COMMAND ----------

// Better set path and checkpointLocation at the same value

val outPath = "/FileStore/tables/aggs"

val query = (
  streamingAggDF
  .writeStream
  .format("json")
  .option("checkpointLocation", outPath)
  .option("path", outPath)
  .start()
)

// COMMAND ----------

// MAGIC %md
// MAGIC Lisez les données enregistrées dans une base de données et explorez-les ; essayez différentes requêtes sur le DataFrame

// COMMAND ----------

val df = spark.read.format("json").load(outPath)

// COMMAND ----------

df.show(truncate=false)

// COMMAND ----------

val outPath = "/FileStore/tables/aggs_csv"

val query = (
  streamingAggDF
  .writeStream
  .format("csv")
  .option("checkpointLocation", outPath)
  .option("path", outPath)
  .start()
)

// COMMAND ----------

val outPath = "/FileStore/tables/aggs_parquet"

val query = (
  streamingAggDF
  .writeStream
  .format("parquet")
  .option("checkpointLocation", outPath)
  .option("path", outPath)
  .start()
)

// COMMAND ----------

val df1 = spark.read.format("parquet").load(outPath)

// COMMAND ----------

df1.show(truncate=false)
