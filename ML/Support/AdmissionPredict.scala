// Databricks notebook source
// MAGIC %md
// MAGIC # Exemple de régression linéaire
// MAGIC
// MAGIC
// MAGIC Nous voulons estimer la probabilité d'admission à l'université connaissant les valeurs de certaines 'features' (GRE Score,TOEFL Score,University Rating, etc)
// MAGIC
// MAGIC Les données sont stockées sur un fichier csv appelé Admission_Predict.csv qui se trouve librement sur Internet; ici, il a déjà été téléchargé.
// MAGIC
// MAGIC Voici les importations nécessaires

// COMMAND ----------

import org.apache.spark.sql.SparkSession

// Remarquez: spark.ml, pas spark.mllib qui est l'ancienne API
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.LinearRegression

// Ici il est nécessaire de l'utliser
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

// COMMAND ----------

// MAGIC %md
// MAGIC Comme toujours créons ls SparkSession

// COMMAND ----------

val spark = SparkSession.builder.appName("Regression with Spark").getOrCreate();

// COMMAND ----------

// MAGIC %md
// MAGIC Répertoire où les fichiers seront stockés en Databricks

// COMMAND ----------

val root = "/FileStore/tables/"

// COMMAND ----------

// MAGIC %md
// MAGIC Lisons la donnée et explorons-la
// MAGIC
// MAGIC Remarquez `option("inferSchema", true)`: nous laissons à Spark le soins de déduire le schéma

// COMMAND ----------

import java.io.File

val path = new File(root, "admissions.csv").getPath

val dataset = spark.read.option("sep", ",").option("inferSchema", true).option("header", true).csv(path)

// COMMAND ----------

dataset.show(10, truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC Valable aussi

// COMMAND ----------

import org.apache.spark.sql.functions.col

val columnsAll=dataset.columns.map(m=>col(m))
dataset.select(columnsAll:_*).show()

// COMMAND ----------

dataset.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Affichons les colonnes

// COMMAND ----------

dataset.columns.foreach(column => println(column))

// COMMAND ----------

// MAGIC %md
// MAGIC Contrôlons s'il y a des valeurs nulles ; ça est toujours une bonne pratique  
// MAGIC Heureusement, il n'y a aucune

// COMMAND ----------

dataset.createOrReplaceTempView("admissions")

dataset.columns.foreach((column: String) => spark.sql("SELECT count(*) FROM admissions where '" + column + "'is null").show())

// COMMAND ----------

// MAGIC %md
// MAGIC Plus simple

// COMMAND ----------

val filterCond = dataset.columns.map(x=>col(x).isNull).reduce(_ and _)

val filteredDf = dataset.filter(filterCond).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Aussi valable

// COMMAND ----------

val filterCond = dataset.columns.map(x=>col(x).isNull).reduce(_ && _)

val filteredDf = dataset.filter(filterCond).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Ici nous utilisons `VectorAssembler` pour mettre la donnée dans une forme adaptée aux outils de Spark ML
// MAGIC
// MAGIC *  Tout d'abord, nous supprimons la colonne des résultats du dataset
// MAGIC *  Puis, `VectorAssembler` met le reste du dataset dans la forme requise.

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer

val cols = dataset.columns.to[ArrayBuffer]

println(cols)

cols -= "Chance of Admit"

println(cols)

// COMMAND ----------

// MAGIC %md
// MAGIC Autre possibilité pour supprimer la colonne des résultats du dataset

// COMMAND ----------

val cols = dataset.columns
println(cols)
val colsMinusChanceOfAdmit = dataset.columns.filter(! _.contains("Chance of Admit"))
println(colsMinusChanceOfAdmit)

// COMMAND ----------

val assembler = new VectorAssembler().setInputCols(colsMinusChanceOfAdmit).setOutputCol("features")

// Now let us use the transform method to transform our dataset
val data = assembler.transform(dataset)

// COMMAND ----------

// MAGIC %md
// MAGIC Voici le résultat

// COMMAND ----------

data.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC C'est généralement une bonne idée de normaliser les données  
// MAGIC
// MAGIC * En soustrayant leur moyenne
// MAGIC
// MAGIC * Puis en divisant par leur écart-type
// MAGIC
// MAGIC `StandardScaler()` nous permet de faire ça

// COMMAND ----------

import org.apache.spark.sql.DataFrame

val standardscaler = new StandardScaler().setInputCol("features").setOutputCol("Scaled_features")

val scaledData = standardscaler.fit(data).transform(data)

// COMMAND ----------

// MAGIC %md
// MAGIC Voici le résultat

// COMMAND ----------

scaledData.select("features", "Chance of Admit", "Scaled_features").show(truncate=false)

// COMMAND ----------

val assembled_data = scaledData.select("Scaled_features", "Chance of Admit")
assembled_data.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC On divise la donnée en train et test dataset, dans le ratio 0.7, 0.3

// COMMAND ----------

val Array(train, test) = assembled_data.randomSplit(Array(0.7, 0.3), seed = 12345)

// COMMAND ----------

train.show(5, truncate=false)

// COMMAND ----------

test.show(5, truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Linear Regression

// COMMAND ----------

// MAGIC %md
// MAGIC Créons le modele de regression linéaire

// COMMAND ----------

val lin_reg = new LinearRegression()

lin_reg.setLabelCol("Chance of Admit").setFeaturesCol("Scaled_features").setMaxIter(20)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fit

// COMMAND ----------

val model=lin_reg.fit(train)

// COMMAND ----------

// MAGIC %md
// MAGIC Voici le prédictions

// COMMAND ----------

val prediction_test=model.transform(test)

// COMMAND ----------

prediction_test.show(10, truncate=false)

// COMMAND ----------

prediction_test.select("Chance of Admit", "prediction").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Ici il faut utliser des outils de `pyspark.mllib` donc il faut transformer le prédictions en RDD

// COMMAND ----------

// Compute raw scores on the test set
val predictionAndLabels = prediction_test.select("Chance of Admit","prediction").rdd

// COMMAND ----------

// MAGIC %md
// MAGIC Quelque metrique

// COMMAND ----------

import org.apache.spark.mllib.evaluation.RegressionMetrics

/*
predictionAndLabels.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])) is necessary
predictionAndLabels only gives error
See https://www.programcreek.com/scala/org.apache.spark.mllib.evaluation.RegressionMetrics
*/
val metrics = new RegressionMetrics(predictionAndLabels.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

// COMMAND ----------

// Squared Error
println("MSE = %s".format(metrics.meanSquaredError))
println("RMSE = %s".format(metrics.rootMeanSquaredError))

// R-squared
println("R-squared = %s".format(metrics.r2))

// Mean absolute error
println("MAE = %s".format(metrics.meanAbsoluteError))

// Explained variance
println("Explained variance = %s".format(metrics.explainedVariance))
