// Databricks notebook source
// MAGIC %md
// MAGIC Répertoire où les fichiers seront stockés en Databricks

// COMMAND ----------

val root = "/FileStore/tables/"

// COMMAND ----------

// MAGIC %md
// MAGIC # Installez une bibliothèque pour lire et écrire fichier XML

// COMMAND ----------

// MAGIC %md
// MAGIC # Créer SparkSession et SparkContext  
// MAGIC Donnez un titre à la spark session

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("tp").getOrCreate()
val sc = spark.sparkContext

// COMMAND ----------

// MAGIC %md
// MAGIC # Exminer la configuration à l'aide de Spark Context

// COMMAND ----------

val config=sc.getConf.getAll

for (conf <- config)
  println(conf._1 +", "+ conf._2)

// COMMAND ----------

// MAGIC %md
// MAGIC # Lire un RDD en Dataframe
// MAGIC
// MAGIC * Créez une liste des valeurs à inclure dans le RDD : [(Nom1, Age1),(Nom2,Age2)...]  
// MAGIC * Utilisez Seq
// MAGIC * Puis utilisez List
// MAGIC * Puis utilisez Array
// MAGIC * Donnez des noms aux colonnes du DataFrame
// MAGIC * Essayez differents syntaxes

// COMMAND ----------

val data = Seq(("Michel",25),("Amandine",22),("Stéphane",20),("Laure",26))
val columns = Seq("Nom", "Age")

val rdd = sc.parallelize(data)
rdd.collect()

// COMMAND ----------

val df = spark.createDataFrame(rdd).toDF(columns:_*)
df.printSchema()
df.show(false)

// COMMAND ----------

val data = List(("Michel",25),("Amandine",22),("Stéphane",20),("Laure",26))
val columns = List("Nom", "Age")

val rdd = sc.parallelize(data)
rdd.collect()

// COMMAND ----------

val df = spark.createDataFrame(rdd).toDF(columns:_*)
df.printSchema()
df.show(false)

// COMMAND ----------

val data = Array(("Michel",25),("Amandine",22),("Stéphane",20),("Laure",26))
val columns = Array("Nom", "Age")

val rdd = sc.parallelize(data)
rdd.collect()

// COMMAND ----------

val df = spark.createDataFrame(rdd).toDF(columns:_*)
df.printSchema()
df.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC # Faites la même chose sans utiliser un RDD

// COMMAND ----------

val data = Seq(("Michel",25),("Amandine",22),("Stéphane",20),("Laure",26))
val columns = Seq("Nom", "Age")

val df = spark.createDataFrame(data).toDF(columns:_*)
df.printSchema()
df.show(false)

// COMMAND ----------

val data = List(("Michel",25),("Amandine",22),("Stéphane",20),("Laure",26))
val columns = List("Nom", "Age")

val df = spark.createDataFrame(data).toDF(columns:_*)
df.printSchema()
df.show(false)

// COMMAND ----------

val data = Array(("Michel",25),("Amandine",22),("Stéphane",20),("Laure",26))
val columns = Array("Nom", "Age")

val df = spark.createDataFrame(data).toDF(columns:_*)
df.printSchema()
df.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Transformez le RDD en un DataFrame avec deux colonnes `name` et `people`  
// MAGIC
// MAGIC Utilisez la transformation `map()` pour créer un objet de type `Row` et créez le DataFrame à partir de celui ci

// COMMAND ----------

// MAGIC %md
// MAGIC # Lire une Dataframe à partir d'un fichier csv

// COMMAND ----------

// MAGIC %md
// MAGIC  Lire le fichier csv 'walmart_stock.csv' en deux façons  
// MAGIC  Visualiser-le avec show et display

// COMMAND ----------

import java.io.File

val path = new File(root,"walmart_stock.csv").getPath

val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

df.show(5)

// COMMAND ----------

display(df)

// COMMAND ----------

val df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(path)

df.show(5)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez les noms des colonnes

// COMMAND ----------

df.columns

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez le schema du fichier 

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Faites un résumé statistique

// COMMAND ----------

df.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC Comptage de nombre de lignes

// COMMAND ----------

df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez les 5 premieres lignes

// COMMAND ----------

df.head(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez les dernières 10 lignes

// COMMAND ----------

df.tail(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Selectionnez quelques colonnes de la Dataframe  
// MAGIC Utilisez plus d'une syntaxe 

// COMMAND ----------

df.select("Date", "Volume").show(10)

// COMMAND ----------

import org.apache.spark.sql.functions.col

df.select(col("Date"), col("Volume")).show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Comptage de nombre des dates unique dans la Dataframe ( Hint: utilisation de .distinct())

// COMMAND ----------

df.select("Date").distinct().count()

// COMMAND ----------

df.describe().show(8)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Créez une nouvelle colonne à partir de colonne existante (utilisez withColumn)

// COMMAND ----------

df.withColumn("Marge", df("High")-df("Low")).show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Calcul manuel du moyenne ('mean') de la colonne 'Close' en aggrégant suivant la 'Date'

// COMMAND ----------

df.groupBy("Date").avg("Close").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Calculez la moyenne, les max et min et le count de Open, High, Low  et renommez les résultats

// COMMAND ----------

import org.apache.spark.sql.functions.{sum,avg,max,min,mean,count}

df.groupBy("Date").agg(
    avg("Open").as("avg_open"),
    avg("High").as("avg_high"),
    avg("Low").as("avg_low"),
    min("Open").as("min_open"),
    min("High").as("min_high"),
    min("Low").as("min_low"),
    max("Open").as("max_open"),
    max("High").as("max_high"),
    max("Low").as("max_low"),
    count("Open").as("num_open"),
    count("High").as("num_high"),
    count("Low").as("num_low"), 
).show(6)

// COMMAND ----------

// MAGIC %md
// MAGIC Créez une SQL View à partir du DataFrame et selectionnez tout à partir de cette view

// COMMAND ----------

df.createOrReplaceTempView("table1")

// COMMAND ----------

spark.sql("SELECT * from table1").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Selectionnez la date et le volume maximum de chaque data en utilisant la SQL view

// COMMAND ----------

val df3 = spark.sql("select Date, max(Volume) from table1 group by Date")

// COMMAND ----------

df3.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Supprimez la colonne Close du DataFrame  
// MAGIC Affichez les colonne du résultat

// COMMAND ----------

df.drop("Close").columns

// COMMAND ----------

// MAGIC %md
// MAGIC Créez un nouveau DataFrame en supprimant la colonne 'Adj Close' 

// COMMAND ----------

val df2=df.drop("Adj Close")
df2.columns

// COMMAND ----------

// MAGIC %md
// MAGIC  Vérifiez si des champs sont dans ce dataframe nulls  
// MAGIC  Essayez différents façons  
// MAGIC  Utilisez Spark SQL aussi

// COMMAND ----------


df.filter("Date is null or Open is null or High is null or Low is null or Close is null or Volume is null").show()

// COMMAND ----------

df.where("Date is null or Open is null or High is null or Low is null or Close is null or Volume is null").show()

// COMMAND ----------

df.filter(
    df("Date").isNull || 
    df("Open").isNull ||
    df("High").isNull ||
    df("Low").isNull ||
    df("Close").isNull ||
    df("Volume").isNull
).show()

// COMMAND ----------

df.where(
    df("Date").isNull || 
    df("Open").isNull ||
    df("High").isNull ||
    df("Low").isNull ||
    df("Close").isNull ||
    df("Volume").isNull
).show()

// COMMAND ----------

import org.apache.spark.sql.functions.isnull

df.filter(
    isnull(df("Date")) ||
    isnull(df("Open")) ||
    isnull(df("High")) ||
    isnull(df("Low")) ||
    isnull(df("Close")) ||
    isnull(df("Volume"))
).show()

// COMMAND ----------

df.where(
    isnull(df("Date")) ||
    isnull(df("Open")) ||
    isnull(df("High")) ||
    isnull(df("Low")) ||
    isnull(df("Close")) ||
    isnull(df("Volume"))
).show()

// COMMAND ----------

df.createOrReplaceTempView("DATA")

spark.sql("SELECT * FROM DATA where Date is null or Open is null or High is null or Low is null or Close is null or Volume is null" ).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Sauvegardez le DataFrame dans les formats
// MAGIC * ORC
// MAGIC * Patrquet
// MAGIC * Avro
// MAGIC * JSON
// MAGIC * XML
// MAGIC Lisez le fichier sauvegardé un plus d'une façon si possible  
// MAGIC Pouvez-vous utliser tous les formats ? Il y a quelque un qui pose problème ?

// COMMAND ----------

import java.io.File

// COMMAND ----------

// MAGIC %md
// MAGIC ORC

// COMMAND ----------

val path = new File(root, "walmart.orc").getPath

df.write.mode("overwrite").orc(path)

val df2=spark.read.orc(path)

df2.show()

// COMMAND ----------

df.write.mode("overwrite").option("header", true).format("orc").save(path)

val df2=spark.read.format("orc").load(path)

df2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Parquet

// COMMAND ----------

val path = new File(root, "walmart.parquet").getPath

df.write.mode("overwrite").parquet(path)

val df2=spark.read.parquet(path)

df2.show()

// COMMAND ----------

df.write.mode("overwrite").option("header", true).format("parquet").save(path)

val df2=spark.read.format("parquet").load(path)

df2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC JSON

// COMMAND ----------

val path = new File(root, "walmart.json").getPath

df.write.mode("overwrite").json(path)

val df2=spark.read.json(path)

df2.show()

// COMMAND ----------

df.write.mode("overwrite").option("header", true).format("json").save(path)

val df2=spark.read.format("json").load(path)

df2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC XML

// COMMAND ----------

import com.databricks.spark.xml

val path = new File(root, "walmart.xml").getPath

df.write.format("com.databricks.spark.xml").mode("overwrite").save(path)

val df2=spark.read.format("com.databricks.spark.xml").load(path)
df2.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC Avro

// COMMAND ----------

val path = new File(root, "walmart.avro").getPath

df.withColumnRenamed("Adj Close", "AdjClose").write.format("avro").mode("overwrite").save(path)

val df2=spark.read.format("avro").load(path)
df2.show()
