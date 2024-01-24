// Databricks notebook source
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql

import java.io.File

val root = "/FileStore/tables/"

val spark = SparkSession
  .builder
  .config("spark.sql.warehouse.dir", root)
  .enableHiveSupport()
  .getOrCreate()

val path = new File(root,"employees.csv").getPath

val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

// COMMAND ----------

df.write.saveAsTable("my_table")

// COMMAND ----------

dbutils.fs.ls("/user/hive/warehouse/my_table")

dbutils.fs.rm("/user/hive/warehouse/my_table", recurse=true)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from my_table;

// COMMAND ----------

display(df)

// COMMAND ----------

df.createOrReplaceTempView("df")

spark.sql("select * from df").show(5)

// COMMAND ----------

spark.sql("update df set salary=0")

// COMMAND ----------

// MAGIC %sql
// MAGIC update my_table set SALARY = 0;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from my_table;

// COMMAND ----------

val sqlc = spark.sqlContext

val df = sqlc.table("my_table")

df.show()

// COMMAND ----------

val df = spark.read.table("my_table")

df.show()

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS my_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS my_table;

// COMMAND ----------

val df = spark.read.table("my_table")

df.show()

// COMMAND ----------

val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

df.write.saveAsTable("my_table2")

// COMMAND ----------

val df2 = spark.read.table("my_table2")

// COMMAND ----------

display(df)

// COMMAND ----------

display(df2)

// COMMAND ----------

spark.sql("desc formatted my_table").show()

// COMMAND ----------

spark.catalog.cacheTable("my_table")

// COMMAND ----------

spark.sql("select * from my_table").show()
