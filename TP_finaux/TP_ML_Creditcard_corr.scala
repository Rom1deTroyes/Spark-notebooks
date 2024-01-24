// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # TP final
// MAGIC
// MAGIC Dans ce TP, vous écrirez une classe pour effectuer une classification sur un ensemble de données spécifié en entrée.  
// MAGIC
// MAGIC La classe doit avoir une méthode main qui prend en entrée le nom du fichier ou du répertoire contenant les données et le nom de la colonne à prédire  
// MAGIC
// MAGIC Effectuez la prédiction pour plusieurs classificateurs (régression logistique, GBT ou autres de votre choix (Attention: naive bayes ne marche pas)  
// MAGIC
// MAGIC Rassemblez les résultats de tous les classificateurs dans un dataframe, affichez-le et enregistrez-le sur le disque  
// MAGIC
// MAGIC Essayez de diviser le code de la classe afin de minimiser les duplications  
// MAGIC
// MAGIC Exécuter le code avec quelques jeux de données : le diabète et les cartes de crédit par exemple  

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.classification.GBTClassifier

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.sql.functions.col

import java.io.File

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}

// NECESSARY TO CRE  ATE DATAFRAMES SPECIFYING A SCHEMA 
import scala.collection.JavaConversions._

// COMMAND ----------

val spark = SparkSession.builder.appName("TP 2 credit card").getOrCreate()

// COMMAND ----------

object aClassifier {
  val root = "/FileStore/tables/"
  val ret_columns = List("Algorithm","ROC-AUC","Accuracy")
  val features = "features"
  val scaled_features = "Scaled_features"
  val prediction = "prediction"

  def get_df(dir_name: String, filename: String): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = { 
    val root2 = "/FileStore/tables/" + dir_name
    val path = new File(root2, filename).getPath
    val df = spark.read.option("sep", ",").option("inferSchema", true).option("header", true).csv(path)
    return df
  }

  def preprocessing(data: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    features: String, scaled_features: String, outcome: String
    ): Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] = {
    val colsMinusOutcome = data.columns.filter(! _.contains(outcome))
    val assembler = new VectorAssembler().setInputCols(colsMinusOutcome).setOutputCol(features)
    val new_data = assembler.transform(data)
    val standardscaler = new StandardScaler().setInputCol(features).setOutputCol(scaled_features)
    val scaledData = standardscaler.fit(new_data).transform(new_data)
    val assembled_data = scaledData.select(scaled_features,outcome)
  
    assembled_data.randomSplit(Array(0.9, 0.1), seed = 12345)
  }

  def logistic_classification(
    train: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    features: String, outcome: String
    ): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = {
    
    val lr = new LogisticRegression().setFeaturesCol(scaled_features).setLabelCol(outcome)
    
    val model = lr.fit(train)
    
    val prediction_test=model.transform(test)

    prediction_test
  }

  def naive_bayes_classification(
    train: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    features: String, outcome: String
    ): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = {
    
    val nb = new NaiveBayes().setSmoothing(1.0).setFeaturesCol(scaled_features).setLabelCol(outcome)
    
    val model = nb.fit(train)
    
    val prediction_test=model.transform(test)

    prediction_test
  }

  def gbt_classification(
    train: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    features: String, outcome: String
    ): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = {
    
    val gbt = new GBTClassifier().setFeaturesCol(scaled_features).setLabelCol(outcome)
    
    val model = gbt.fit(train)
    
    val prediction_test=model.transform(test)

    prediction_test
  }

  def print_metrics(prediction_test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    algo: String, outcome: String, prediction: String): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = {
   
    val predictionAndLabels = prediction_test.select(outcome, prediction).rdd.map(r => (r(0).toString.toDouble,r(1).toString.toDouble))
    
    val metrics = new BinaryClassificationMetrics(predictionAndLabels, 0)
    
    val auc = metrics.areaUnderROC
  
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(outcome).setPredictionCol(prediction).setMetricName("accuracy")
    
    val accuracy_RF = evaluator.evaluate(prediction_test)

    val ret_data = List((algo, auc, accuracy_RF))

    val ret_df = spark.createDataFrame(ret_data).toDF(ret_columns:_*)

    ret_df
  }

  def main(args: Array[String]): Unit = {
    val df = get_df(args(0), args(1))
    val outcome = args(2)

    val arr_train_test = preprocessing(df, features, scaled_features, outcome)
    
    val pred_df1 = logistic_classification(arr_train_test(0), arr_train_test(1), scaled_features, outcome)
    val ret_df1 = print_metrics(pred_df1, "Logistic Regression", outcome, prediction)

    val pred_df2 = gbt_classification(arr_train_test(0), arr_train_test(1), scaled_features, outcome)
    val ret_df2 = print_metrics(pred_df2, "GBT", outcome, prediction)
    
    val ret_df = ret_df1.union(ret_df2)

    display(ret_df)

    val out_path =  root + "/results_" +  args(0) + "csv"
    ret_df.write.mode("overwrite").option("header", true).csv(out_path)
  }
}

aClassifier.main(Array("creditcard", "creditcard.csv", "Class"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Dirty code

// COMMAND ----------

object aClassifier {
  /*
  val schema = StructType(
    List(
      StructField("Algorithm",StringType,true),
      StructField("ROC-AUC",DoubleType,true),
      StructField("accuracy",DoubleType,true)
    )
  )
  */

  val ret_columns = List("Algorithm","ROC-AUC","Accuracy")
  val features = "features"
  val scaled_features = "Scaled_features"
  val prediction = "prediction"

  def get_df(filename: String): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = { 
    val root = "/FileStore/tables/creditcard"
    val path = new File(root, filename).getPath
    val df = spark.read.option("sep", ",").option("inferSchema", true).option("header", true).csv(path)
    return df
  }

  def preprocessing(data: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    features: String, scaled_features: String, outcome: String
    ): Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] = {
    val colsMinusOutcome = data.columns.filter(! _.contains(outcome))
    val assembler = new VectorAssembler().setInputCols(colsMinusOutcome).setOutputCol(features)
    val new_data = assembler.transform(data)
    val standardscaler = new StandardScaler().setInputCol(features).setOutputCol(scaled_features)
    val scaledData = standardscaler.fit(new_data).transform(new_data)
    val assembled_data = scaledData.select(scaled_features,outcome)
    // Array(train, test) = assembled_data.randomSplit(Array(0.9, 0.1), seed = 12345)
    assembled_data.randomSplit(Array(0.9, 0.1), seed = 12345)
  }

  def logistic_classification(
    train: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    features: String, outcome: String
    ): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = {
    /*
    val colsMinusOutcome = data.columns.filter(! _.contains("Class"))
    val assembler = new VectorAssembler().setInputCols(colsMinusOutcome).setOutputCol("features")
    val new_data = assembler.transform(data)
    val standardscaler = new StandardScaler().setInputCol("features").setOutputCol("Scaled_features")
    val scaledData = standardscaler.fit(new_data).transform(new_data)
    val assembled_data = scaledData.select("Scaled_features","Class")
    val Array(train, test) = assembled_data.randomSplit(Array(0.9, 0.1), seed = 12345)
    */
    // val lr = new LogisticRegression().setFeaturesCol("Scaled_features").setLabelCol("Class")
    val lr = new LogisticRegression().setFeaturesCol(features).setLabelCol(outcome)
    val model = lr.fit(train)
    val prediction_test=model.transform(test)

    prediction_test
  }

  def print_metrics(prediction_test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row],
    algo: String, outcome: String, prediction: String): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = {
    // val predictionAndLabels = prediction_test.select("Class", "Prediction").rdd.map(r => (r(0).toString.toDouble,r(1).toString.toDouble))
    val predictionAndLabels = prediction_test.select(outcome, prediction).rdd.map(r => (r(0).toString.toDouble,r(1).toString.toDouble))
    val metrics = new BinaryClassificationMetrics(predictionAndLabels, 0)
    val auc = metrics.areaUnderROC
    // Area under ROC curveBinaryClassificationMetrics
    // println(s"Area under ROC = $auc")

    // Precision by threshold
    //val precision = metrics.precisionByThreshold
    //precision.collect.foreach {
    //  case (t, p) => println(s"Threshold: $t, Precision: $p")
    //}

    // Recall by threshold
    //val recall = metrics.recallByThreshold
    //recall.collect.foreach { 
    //  case (t, r) => println(s"Threshold: $t, Recall: $r")
    //}

    // Precision-Recall Curve
    // val PRC = metrics.pr

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(outcome).setPredictionCol(prediction).setMetricName("accuracy")
    
    val accuracy_RF = evaluator.evaluate(prediction_test)
    //println(accuracy_RF)

    val ret_data = List((algo, auc, accuracy_RF))

    val ret_df = spark.createDataFrame(ret_data).toDF(ret_columns:_*)

    ret_df
  }

  def main(args: Array[String]): Unit = {
    val df = get_df(args(0))
    val outcome = args(1)
    val arr_train_test = preprocessing(df, features, scaled_features, outcome)
    val pred_df = logistic_classification(arr_train_test(0), arr_train_test(1), scaled_features, outcome)
    val ret_df = print_metrics(pred_df, "Logistic Regression", outcome, prediction)
    display(ret_df)
  }
}

aClassifier.main(Array("creditcard.csv", "Class"))

// COMMAND ----------

var x = Array("creditcard.csv", "Class", List("logistic"))

var algos = x(2)

for (algo <- algos) {
      algo
    }
