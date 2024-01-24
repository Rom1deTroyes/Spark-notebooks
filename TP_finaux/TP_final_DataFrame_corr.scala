// Databricks notebook source
// MAGIC %md
// MAGIC Aperçu de l'exercice  
// MAGIC
// MAGIC Dans cet exercice, nous allons jouer avec Spark Datasets & Dataframes, du Spark SQL  
// MAGIC
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC Obtenir des données  
// MAGIC
// MAGIC Ici, nous allons extraire quelques exemples de données déjà préchargées sur tous les clusters de databricks.

// COMMAND ----------

// display datasets already in databricks
display(dbutils.fs.ls("/databricks-datasets"))

// COMMAND ----------

// MAGIC %md 
// MAGIC
// MAGIC Jetons un coup d'œil à l'ensemble de données « adult » sur le système de fichiers.  
// MAGIC
// MAGIC Il s'agit des données typiques du recensement américain que vous voyez souvent en ligne dans les didacticiels.  
// MAGIC
// MAGIC Voici les mêmes données dans le référentiel UCI.
// MAGIC
// MAGIC En passant : ici, ce même ensemble de données est utilisé comme exemple de démarrage rapide pour l'API Google CLoud ML  
// MAGIC
// MAGIC Vous allez créer une table SQL de ce fichier

// COMMAND ----------

// MAGIC %fs ls databricks-datasets/adult/adult.data

// COMMAND ----------

// MAGIC %md 
// MAGIC Spark SQL  
// MAGIC
// MAGIC Ci-dessous, nous utiliserons Spark SQL pour charger les données, puis les enregistrerons également en tant que Dataframe.  
// MAGIC
// MAGIC Le résultat final sera donc une table Spark SQL appelée adult et un Spark Dataframe appelé df_adult  
// MAGIC
// MAGIC Ceci est un exemple de la flexibilité de Spark dans la mesure où vous pouvez effectuer de nombreuses tâches ETL et gestion des données en utilisant soit Spark SQL, soit Dataframes et spark. La plupart du temps, il s’agit d’utiliser ce avec quoi vous êtes le plus à l’aise.  
// MAGIC
// MAGIC Créez une table SQL "adult"

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- drop the table if it already exists
// MAGIC DROP TABLE IF EXISTS adult

// COMMAND ----------

// MAGIC %sql
// MAGIC -- create a new table in Spark SQL from the datasets already loaded in the underlying filesystem.
// MAGIC -- In the real world you might be pointing at a file on HDFS or a hive table etc. 
// MAGIC CREATE TABLE adult (
// MAGIC   age DOUBLE,
// MAGIC   workclass STRING,
// MAGIC   fnlwgt DOUBLE,
// MAGIC   education STRING,
// MAGIC   education_num DOUBLE,
// MAGIC   marital_status STRING,
// MAGIC   occupation STRING,
// MAGIC   relationship STRING,
// MAGIC   race STRING,
// MAGIC   sex STRING,
// MAGIC   capital_gain DOUBLE,
// MAGIC   capital_loss DOUBLE,
// MAGIC   hours_per_week DOUBLE,
// MAGIC   native_country STRING,
// MAGIC   income STRING)
// MAGIC USING com.databricks.spark.csv
// MAGIC OPTIONS (path "/databricks-datasets/adult/adult.data", header "true")

// COMMAND ----------

// MAGIC %md
// MAGIC Inspectez la table  

// COMMAND ----------

// MAGIC %scala
// MAGIC // look at the data
// MAGIC // spark.sql("SELECT * FROM adult LIMIT 5").show() 
// MAGIC // this will look prettier in Databricks if you use display() instead
// MAGIC display(spark.sql("SELECT * FROM adult LIMIT 5"))

// COMMAND ----------

// MAGIC %md
// MAGIC Ecrivez une requete SQL pour quelques taux récapitulatifs de l’état civil par profession  
// MAGIC
// MAGIC Faites la même chose avec une table temporaire  

// COMMAND ----------

// MAGIC %scala
// MAGIC // Lets get some summary marital status rates by occupation
// MAGIC val result = spark.sql(
// MAGIC   """
// MAGIC   SELECT 
// MAGIC     occupation,
// MAGIC     SUM(1) as n,
// MAGIC     ROUND(AVG(if(LTRIM(marital_status) LIKE 'Married-%',1,0)),2) as married_rate,
// MAGIC     ROUND(AVG(if(lower(marital_status) LIKE '%widow%',1,0)),2) as widow_rate,
// MAGIC     ROUND(AVG(if(LTRIM(marital_status) = 'Divorced',1,0)),2) as divorce_rate,
// MAGIC     ROUND(AVG(if(LTRIM(marital_status) = 'Separated',1,0)),2) as separated_rate,
// MAGIC     ROUND(AVG(if(LTRIM(marital_status) = 'Never-married',1,0)),2) as bachelor_rate
// MAGIC   FROM 
// MAGIC     adult 
// MAGIC   GROUP BY 1
// MAGIC   ORDER BY n DESC
// MAGIC   """)
// MAGIC display(result)

// COMMAND ----------

// MAGIC %scala
// MAGIC // register the df we just made as a table for spark sql
// MAGIC result.createOrReplaceTempView ("result")
// MAGIC spark.sql("SELECT * FROM result").show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez du SQL Spark pour obtenir le meilleur « bachelor_rate » par groupe « éducation » ?

// COMMAND ----------


val result = spark.sql(
  """
  SELECT
    education,
    MAX(bachelor_rate) as max_bachelor_rate
  FROM ( 
    SELECT 
      education,
      SUM(1) as n,
      ROUND(AVG(if(LTRIM(marital_status) LIKE 'Married-%',1,0)),2) as married_rate,
      ROUND(AVG(if(lower(marital_status) LIKE '%widow%',1,0)),2) as widow_rate,
      ROUND(AVG(if(LTRIM(marital_status) = 'Divorced',1,0)),2) as divorce_rate,
      ROUND(AVG(if(LTRIM(marital_status) = 'Separated',1,0)),2) as separated_rate,
      ROUND(AVG(if(LTRIM(marital_status) = 'Never-married',1,0)),2) as bachelor_rate
    FROM 
      adult 
    GROUP BY 1
    ORDER BY n DESC
  ) as tmp
  GROUP BY education
  ORDER BY max_bachelor_rate DESC
  """)
result.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Calculez la fraction de personnes avec un revenu > 50k $ par profession et triez par ordre décroissant

// COMMAND ----------

val result = spark.sql(
  """
  SELECT 
    occupation,
    AVG(IF(income = ' >50K',1,0)) as plus_50k
  FROM 
    adult 
  GROUP BY occupation
  ORDER BY plus_50k DESC
  """)
  display(result)

// COMMAND ----------

// MAGIC %md
// MAGIC Ci-dessous, nous allons créer notre DataFrame à partir de la table SQL et effectuer une analyse similaire à celle que nous avons faite avec Spark SQL mais en utilisant l'API DataFrames.  

// COMMAND ----------

// register a df from the sql df
val df_adult = spark.table("adult")
val cols = df_adult.columns

// COMMAND ----------

// look at df schema
df_adult.printSchema()

// COMMAND ----------

display(df_adult)

// COMMAND ----------

// MAGIC %md 
// MAGIC Calculez le taux de divorce pour chaque occupation et triez par ordre décroissant avec DataFrames

// COMMAND ----------

import org.apache.spark.sql.functions.{when, col, mean, desc, round}

// wrangle the data a bit
val df_result = df_adult.select(
  df_adult("occupation"),
  // create a 1/0 type col on the fly
  when( col("marital_status").like("%Divorced%") , 1.0 ).otherwise(0.0).as("is_divorced")
).groupBy("occupation").agg(round(mean("is_divorced"),4).as("divorced_rate")).orderBy(desc("divorced_rate"))

df_result.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Faites la même chose en Spark SQL

// COMMAND ----------

val df = spark.sql("""
  SELECT 
    occupation,
    MEAN(is_divorced) as divorce_rate FROM (
    SELECT
      occupation,
      if (LTRIM(marital_status) = 'Divorced',1,0) as is_divorced
    FROM 
      adult
    ) as tmp 
  GROUP BY occupation
  ORDER BY divorce_rate desc
""")

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Transformez le DataFrame en Dataset

// COMMAND ----------

case class Adult(
  age: Double,
  workclass: String,
  fnlwgt: Double,
  education: String,
  education_num: Double,
  marital_status: String,
  occupation: String
)

// COMMAND ----------

val df_adult = spark.table("adult")
val ds_adult = df_adult.as[Adult]

df_adult.getClass

ds_adult.getClass
