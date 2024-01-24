// Databricks notebook source
// MAGIC %md
// MAGIC # Exemple de régression logistique
// MAGIC
// MAGIC Nous voulons estimer la probabilité de tomber malade de diabéte connaissant les valeurs de certaines 'features' (Glucose,BloodPressure,SkinThickness,Insulin,BMI etc)
// MAGIC
// MAGIC Les données sont stockées sur un fichier csv appelé Diabetes_Predict.csv qui se trouve librement sur Internet; ici, il a déjà été téléchargé.
// MAGIC
// MAGIC Voici les importations nécessaires  
// MAGIC
// MAGIC Il est mieux de les faire au début  

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.classification.RandomForestClassifier

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.sql.DataFrame

import java.io.File

import org.apache.spark.sql.functions.col

// COMMAND ----------

// MAGIC %md
// MAGIC Comme toujours créons ls SparkSession

// COMMAND ----------

val spark = SparkSession.builder.appName("Classification with Spark").getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC Répertoire où les fichiers seront stockés en Databricks

// COMMAND ----------

val root = "/FileStore/tables/"

// COMMAND ----------

// MAGIC %md
// MAGIC Lisons la donnée et explorons-la
// MAGIC
// MAGIC Remarquez `option("inferSchema", true)`: ici nous ne laissons pas à PySpark le soins de déduire le schéma mais on le fera à la main

// COMMAND ----------

// import java.io.File

val path = new File(root, "diabetes.csv").getPath

val dataset = spark.read.option("sep", ",").option("inferSchema", true).option("header", true).csv(path)

// COMMAND ----------

dataset.show(10)

// COMMAND ----------

dataset.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC On va transformer toutes les colonnes en `float` à la main  
// MAGIC Importons les paquets nécessaires

// COMMAND ----------

// import org.apache.spark.sql.functions.col

/*
This works
Taken from https://stackoverflow.com/questions/42080730/how-to-cast-all-columns-of-dataframe-to-string
Look for the Scala solution
*/

val new_data = dataset.select(dataset.columns.map(c => col(c).cast("Float")) : _*)

// COMMAND ----------

new_data.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Désormais les colonnes du nouveau dataset sont des `float` 

// COMMAND ----------

new_data.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Contrôlons s'il y a des valeurs nulles ; ça est toujours une bonne pratique  
// MAGIC Heureusement, il n'y a aucune

// COMMAND ----------

new_data.createOrReplaceTempView("diabetes")

new_data.columns.foreach((column: String) => spark.sql("SELECT count(*) FROM diabetes where '" + column + "'is null").show())

// COMMAND ----------

// MAGIC %md
// MAGIC Plus simple

// COMMAND ----------

val filterCond = new_data.columns.map(x=>col(x).isNull).reduce(_ and _)

val filteredDf = new_data.filter(filterCond).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Ici nous utilisons `VectorAssembler` pour mettre la donnée dans une forme adaptée aux outils de Spark ML
// MAGIC
// MAGIC *  Tout d'abord, nous supprimons la colonne des résultats du dataset
// MAGIC *  Puis, `VectorAssembler` met le reste du dataset dans la forme requise.

// COMMAND ----------

val colsMinusOutcome = dataset.columns.filter(! _.contains("Outcome"))
println(colsMinusOutcome)

// COMMAND ----------

val assembler = new VectorAssembler().setInputCols(colsMinusOutcome).setOutputCol("features")

// Now let us use the transform method to transform our dataset
val data = assembler.transform(new_data)

// COMMAND ----------

// MAGIC %md
// MAGIC Voici le résultat

// COMMAND ----------

data.select("features", "Outcome").show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC C'est généralement une bonne idée de normaliser les données
// MAGIC
// MAGIC     En soustrayant leur moyenne
// MAGIC
// MAGIC     Puis en divisant par leur écart-type
// MAGIC
// MAGIC StandardScaler() nous permet de faire ça

// COMMAND ----------

// import org.apache.spark.sql.DataFrame

val standardscaler = new StandardScaler().setInputCol("features").setOutputCol("Scaled_features")

val scaledData = standardscaler.fit(data).transform(data)

// COMMAND ----------

// MAGIC %md
// MAGIC Voici le résultat

// COMMAND ----------

scaledData.select("features", "Outcome", "Scaled_features").show(5, truncate=false)

// COMMAND ----------

val assembled_data = scaledData.select("Scaled_features","Outcome")

assembled_data.show(10, truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC On divise la donnée en train et test dataset, dans le ratio 0.7, 0.3

// COMMAND ----------

val Array(train, test) = assembled_data.randomSplit(Array(0.7, 0.3), seed = 12345)

// COMMAND ----------

train.show(10, truncate=false)

// COMMAND ----------

test.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Logistic Regression

// COMMAND ----------

val log_reg = new LogisticRegression()

log_reg.setLabelCol("Outcome").setFeaturesCol("Scaled_features").setMaxIter(20)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fit

// COMMAND ----------

val model=log_reg.fit(train)

// COMMAND ----------

val prediction_test=model.transform(test)

// COMMAND ----------

prediction_test.show(10, truncate=false)

// COMMAND ----------

prediction_test.select("Outcome","prediction").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Ici il faut utliser des outils de `pyspark.mllib` donc il faut transformer le prédictions en RDD

// COMMAND ----------

// Compute raw scores on the test set

val predictionAndLabels = prediction_test.select("Outcome", "Prediction").rdd.map(r => (r(0).toString.toDouble,r(1).toString.toDouble))

// COMMAND ----------

predictionAndLabels.collect()

// COMMAND ----------

// import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

val metrics = new BinaryClassificationMetrics(predictionAndLabels, 0)

predictionAndLabels.getClass

val auc = metrics.areaUnderROC

// Area under ROC curveBinaryClassificationMetrics
println(s"Area under ROC = $auc")

// COMMAND ----------

// MAGIC %md
// MAGIC Calculons la précision (accuracy)

// COMMAND ----------

val evaluator = new MulticlassClassificationEvaluator().setLabelCol("Outcome").setPredictionCol("prediction").setMetricName("accuracy")

val accuracy_LR = evaluator.evaluate(prediction_test)
println("Accuracy = " ,accuracy_LR)

// COMMAND ----------

// MAGIC %md
// MAGIC # Matériaux supplémentaires
// MAGIC Faire si possible

// COMMAND ----------

// MAGIC %md
// MAGIC ## NaiveBayes

// COMMAND ----------

// Nous avons besoin du type de test et train
test.getClass()

// COMMAND ----------

// import org.apache.spark.ml.classification.NaiveBayes

def naive_bayes_classification(
  train: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
  test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]): Unit = {

  val naive_bayes = new NaiveBayes().setSmoothing(1.0).setFeaturesCol("Scaled_features").setLabelCol("Outcome")

  val model = naive_bayes.fit(train)

  val prediction_test=model.transform(test)

  val predictionAndLabels = prediction_test.select("Outcome", "Prediction").rdd.map(r => (r(0).toString.toDouble,r(1).toString.toDouble))

  val metrics = new BinaryClassificationMetrics(predictionAndLabels, 0)

  val auc = metrics.areaUnderROC

  // Area under ROC curveBinaryClassificationMetrics
  println(s"Area under ROC = $auc")

  val evaluator = new MulticlassClassificationEvaluator().setLabelCol("Outcome").setPredictionCol("prediction").setMetricName("accuracy")

  val accuracy_NB= evaluator.evaluate(prediction_test)
  println("Accuracy = " , accuracy_NB)
}

naive_bayes_classification(train, test)


// COMMAND ----------

// MAGIC %md
// MAGIC ## GBTClassifier

// COMMAND ----------

// import org.apache.spark.ml.classification.GBTClassifier

def gbt_classification(
  train: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
  test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]): Unit = {

  val gbt_classifier = new GBTClassifier().setFeaturesCol("Scaled_features").setLabelCol("Outcome")

  val model = gbt_classifier.fit(train)

  val prediction_test=model.transform(test)

  val predictionAndLabels = prediction_test.select("Outcome", "Prediction").rdd.map(r => (r(0).toString.toDouble,r(1).toString.toDouble))

  val metrics = new BinaryClassificationMetrics(predictionAndLabels, 0)

  val auc = metrics.areaUnderROC

  // Area under ROC curveBinaryClassificationMetrics
  println(s"Area under ROC = $auc")

  val evaluator = new MulticlassClassificationEvaluator().setLabelCol("Outcome").setPredictionCol("prediction").setMetricName("accuracy")

  val accuracy_GBT = evaluator.evaluate(prediction_test)
  println("Accuracy = " ,accuracy_GBT)
}

gbt_classification(train, test)

// COMMAND ----------

// MAGIC %md
// MAGIC ## RandomForestClassifier

// COMMAND ----------

// import org.apache.spark.ml.classification.RandomForestClassifier

def rf_classification(
  train: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
  test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]): Unit = {

  val rf_classifier = new RandomForestClassifier().setNumTrees(100).setFeaturesCol("Scaled_features").setLabelCol("Outcome")

  val model = rf_classifier.fit(train)

  val prediction_test=model.transform(test)

  val predictionAndLabels = prediction_test.select("Outcome", "Prediction").rdd.map(r => (r(0).toString.toDouble,r(1).toString.toDouble))

  val metrics = new BinaryClassificationMetrics(predictionAndLabels, 0)

  val auc = metrics.areaUnderROC

  // Area under ROC curveBinaryClassificationMetrics
  println(s"Area under ROC = $auc")

  val accuracy_RF= evaluator.evaluate(prediction_test)
  println("Accuracy = " , accuracy_RF)
}

rf_classification(train, test)
