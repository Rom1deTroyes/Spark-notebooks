// Databricks notebook source
// MAGIC %md
// MAGIC Location des fichiers

// COMMAND ----------

val root = "/FileStore/tables/"

// COMMAND ----------

// MAGIC %md
// MAGIC # Dans ce TP vous allez éxaminer le dataset Titanic

// COMMAND ----------

// MAGIC %md
// MAGIC Faites le choses préliminaires

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.getOrCreate()
val sc = spark.sparkContext

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

// MAGIC %md
// MAGIC Lisez le fichier '/FileStore/tables/titanic.csv' en deux façons différentes et inspectez-le  

// COMMAND ----------

import java.io.File

val path = new File(root,"titanic.csv").getPath

// COMMAND ----------

val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

// COMMAND ----------

val df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)

// COMMAND ----------

df.head(10)

// COMMAND ----------

df.tail(10)

// COMMAND ----------

df.describe().show()

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Combinen des passagers il y a dans le DataFrame ?

// COMMAND ----------

df.count()

// COMMAND ----------

df.head(10)

// COMMAND ----------

df.tail(10)

// COMMAND ----------

df.columns

// COMMAND ----------

df.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez et comptez le nombre de passagers de sex feminin; essayes plusieurs syntaxes

// COMMAND ----------

df.filter(df("Sex") === "female").show(5, truncate = false)

// COMMAND ----------

df.where(df("Sex") === "female").show(5, truncate = false)

// COMMAND ----------

df.filter(df("Sex") === "female").count()

// COMMAND ----------

df.filter(df("Sex").equalTo("female")).count()

// COMMAND ----------

df.filter(col("Sex") === "female").count()

// COMMAND ----------

df.filter(col("Sex").equalTo("female")).count()

// COMMAND ----------

// MAGIC %md
// MAGIC Careful with the quotes

// COMMAND ----------

df.filter("Sex == 'female'").count()

// COMMAND ----------

df.filter("Sex == 'female'").count()

// COMMAND ----------

// MAGIC %md
// MAGIC Afficher âge et sexe des passagers de sexe féminin et don l'âge est > 20 ans

// COMMAND ----------

df.filter((df("Sex") === "female") && (df("Age") > 20)).select(df("Sex"), df("Age")).show(5, truncate=false)

// COMMAND ----------

df.where((df("Sex") === "female") && (df("Age") > 20)).select(df("Sex"), df("Age")).show(5, truncate=false)

// COMMAND ----------

df.filter((df("Sex").equalTo("female")) && (df("Age") > 20)).select(df("Sex"), df("Age")).show(5, truncate=false)

// COMMAND ----------

df.where((df("Sex").equalTo("female")) && (df("Age") > 20)).select(df("Sex"), df("Age")).show(5, truncate=false)

// COMMAND ----------

df.filter((col("Sex") === "female") && (col("Age") > 20)).select(col("Age"), col("Sex")).show(5, truncate=false)

// COMMAND ----------

df.filter((col("Sex").equalTo("female")) && (col("Age") > 20)).select(col("Age"), col("Sex")).show(5, truncate=false)

// COMMAND ----------

df.where((col("Sex") === "female") && (col("Age") > 20)).select(col("Age"), col("Sex")).show(5, truncate=false)

// COMMAND ----------

df.where((col("Sex").equalTo("female")) && (col("Age") > 20)).select(col("Age"), col("Sex")).show(5, truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC Créee un SQL temporary view et sélectionnez Age et Sex ou le Sex est féminin et l'Age > 20

// COMMAND ----------

df.createOrReplaceTempView("Titanic")

// COMMAND ----------

spark.sql("select Age, Sex from Titanic where Age > 20 and Sex = 'female'").show(5, truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC Compter les nombre de passgers de sexe féminin et don l'âge est > 20 ans

// COMMAND ----------

df.filter((df("Sex") === "female") && (df("Age") > 20)).count()

// COMMAND ----------

// MAGIC %md
// MAGIC Compter le nombre de passagers dont l'âge n'est pas null

// COMMAND ----------

df.filter(df("Age").isNotNull).count()

// COMMAND ----------

// MAGIC %md
// MAGIC Calculez l'âge moyen, mimimum, maximum et rénommez les colonne

// COMMAND ----------

import org.apache.spark.sql.functions.{mean, min, max}

df.agg(
    mean("Age").as("mean_age"), 
    min("Age").as("min_age"),
    max("Age").as("max_age")
).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Ajoutez une colonne donnant l'age en mois et donnez un nom significatif

// COMMAND ----------

df.withColumn("Age en mois",col("Age") * 12).show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Transformez la colonne Age de façon que il donne l'âge en mois

// COMMAND ----------

df.withColumn("Age", col("Age") * 12).show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez l'âge moyen des passagers groupés par classe et port de départ et triez la sortie par classe et port de départ  
// MAGIC Trouvez les bonnes colonnes en examinant le schéma

// COMMAND ----------

df.groupBy("Pclass", "Embarked").agg(
    mean(col("Age"))
).orderBy("Pclass", "Embarked").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Crées un nouveau DataFrame ou pour les passagers dont l'âge est null dans le DataFrame d'origine la moyenne est imputée  
// MAGIC Comptez le nombre des nulls dans le deux DataFrames

// COMMAND ----------

// Voici comment extraire l'âge moyen comme valeur double
// asInstanceOf[Double] est nécessaire pout transfromer la valeur en Double 
df.agg(mean("Age")).head()(0).asInstanceOf[Double]

// COMMAND ----------

val df2=df.na.fill(value=df.agg(mean("Age")).head()(0).asInstanceOf[Double], Array("Age"))

// COMMAND ----------

df.filter(df("Age").isNull).count()

// COMMAND ----------

df2.filter(df2("Age").isNull).count()

// COMMAND ----------

// MAGIC %md
// MAGIC L'âge moyen doit être le même dans les deux DataFrames

// COMMAND ----------

df.agg(mean("Age")).head()(0)

// COMMAND ----------

df2.agg(mean("Age")).head()(0)

// COMMAND ----------

// MAGIC %md
// MAGIC Combien des passagers de sexe masculin et féminin il y a dans le DataFrame ?

// COMMAND ----------

df.groupBy("Sex").count().show()

// COMMAND ----------

// MAGIC %md
// MAGIC Calculez la probabilité de survie des passagers de sexe masculin et féminin  
// MAGIC Hint: la fonction count() utilisée pour visualiser les DataFrame revoie un entier dont la valeur peut être utilisée pour calculer la probabilité

// COMMAND ----------

df.filter((df("Sex") === "male") && (df("Survived") === 1)).count().asInstanceOf[Double] / df.filter(df("Sex") === "male").count().asInstanceOf[Double]

// COMMAND ----------

// MAGIC %md
// MAGIC Sauvegardez le dataFrame en format csv et parquet

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/titanic_csv

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/titanic_parquet

// COMMAND ----------

import java.io.File

val path1 = new File(root,"titanic_csv").getPath

val path2 = new File(root,"titanic_parquet").getPath

// COMMAND ----------

df.write.mode("overwrite").csv(path1)

// COMMAND ----------

df.write.mode("overwrite").parquet(path2)
