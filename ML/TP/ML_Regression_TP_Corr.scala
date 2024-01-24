// Databricks notebook source
// MAGIC %md
// MAGIC Où les données sont stockées

// COMMAND ----------

val root = "/FileStore/tables"

// COMMAND ----------

// MAGIC %md
// MAGIC # TP régression linéaire
// MAGIC 
// MAGIC Voici les importations nécessaires

// COMMAND ----------

import org.apache.spark.sql.SparkSession

// Remarquez: spark.ml, pas spark.mllib qui est l'ancienne API
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.LinearRegression

// Ici il est nécessaire de l'utliser
import org.apache.spark.mllib.evaluation.RegressionMetrics

// COMMAND ----------

// MAGIC %md
// MAGIC Comme toujours créons ls SparkSession

// COMMAND ----------

val spark = SparkSession.builder.appName("Regression with Spark").getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC Lisons la donnée et explorons-la
// MAGIC 
// MAGIC Remarquez `option("inferSchema", true)`: nous laissons à PySpark le soins de déduire le schéma

// COMMAND ----------

import java.io.File

val path = new File(root, "housing.csv").getPath

val dataset = spark.read.option("sep", ",").option("inferSchema", true).option("header", true).csv(path)

// COMMAND ----------

dataset.show()

// COMMAND ----------

import org.apache.spark.sql.functions.col

val columnsAll=dataset.columns.map(m=>col(m))
dataset.select(columnsAll:_*).show()

// COMMAND ----------

dataset.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Fix the dataset, do this only once

// COMMAND ----------

dataset.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Contrôlons s'il y a des valeurs nulles ; ça est toujours une bonne pratique  
// MAGIC Heureusement, il n'y a aucune

// COMMAND ----------

dataset.createOrReplaceTempView("housing")

dataset.columns.foreach((column: String) => spark.sql("SELECT count(*) FROM housing where '" + column + "'is null").show())

// COMMAND ----------

// MAGIC %md
// MAGIC Ici nous utilisons `VectorAssembler` pour mettre la donnée dans une forme adaptée aux outils de Spark ML
// MAGIC 
// MAGIC *  Tout d'abord, nous supprimons la colonne des résultats du dataset
// MAGIC *  Puis, `VectorAssembler` met le reste du dataset dans la forme requise.

// COMMAND ----------

val cols = dataset.columns
println(cols)

val cols_median_house_value = dataset.columns.filter(! _.contains("median_house_value"))
println(cols_median_house_value)

val assembler = new VectorAssembler().setInputCols(cols_median_house_value).setOutputCol("features")

// Now let us use the transform method to transform our dataset
val transformed_dataset=assembler.transform(dataset)

// COMMAND ----------

// MAGIC %md
// MAGIC Voici le résultat

// COMMAND ----------

transformed_dataset.select("features","median_house_value").show(truncate=false)

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

val standardscaler = new StandardScaler().setInputCol("features").setOutputCol("Scaled_features")

val data = standardscaler.fit(transformed_dataset).transform(transformed_dataset)

// COMMAND ----------

// MAGIC %md
// MAGIC Voici le résultat

// COMMAND ----------

data.select("features","median_house_value","Scaled_features").show(truncate=false)

// COMMAND ----------

val assembled_data = data.select("Scaled_features","median_house_value")
assembled_data.show()

// COMMAND ----------

// MAGIC %md
// MAGIC On divise la donnée en train et test dataset, dans le ratio 0.7, 0.3

// COMMAND ----------

val Array(train, test) = assembled_data.randomSplit(Array(0.7, 0.3), seed=12345)

// COMMAND ----------

train.show()

// COMMAND ----------

test.show()

// COMMAND ----------

train.count()

// COMMAND ----------

test.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Linear Regression

// COMMAND ----------

// MAGIC %md
// MAGIC Créons le modele de regression linéaire

// COMMAND ----------

val lin_reg = new LinearRegression()

lin_reg.setLabelCol("median_house_value").setFeaturesCol("Scaled_features").setMaxIter(20)

lin_reg.getClass()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fit

// COMMAND ----------

val model=lin_reg.fit(train)
model.getClass()

// COMMAND ----------

// MAGIC %md
// MAGIC Voici le prédictions

// COMMAND ----------

val prediction_test=model.transform(test)
prediction_test.getClass()

// COMMAND ----------

prediction_test.show()

// COMMAND ----------

prediction_test.select("median_house_value","prediction").show(10)

// COMMAND ----------

prediction_test.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Ici il faut utliser des outils de `pyspark.mllib` donc il faut transformer le prédictions en RDD

// COMMAND ----------

// Compute raw scores on the test set
val predictionAndLabels = prediction_test.select("median_house_value","prediction").rdd
predictionAndLabels.getClass()

// COMMAND ----------

// MAGIC %md
// MAGIC Quelque metrique

// COMMAND ----------

import org.apache.spark.mllib.evaluation.RegressionMetrics

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
