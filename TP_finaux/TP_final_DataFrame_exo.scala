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



// COMMAND ----------




// COMMAND ----------

// MAGIC %md
// MAGIC Inspectez la table  

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Ecrivez une requete SQL pour quelques taux récapitulatifs de l’état civil par profession  
// MAGIC
// MAGIC Faites la même chose avec une table temporaire  

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez du SQL Spark pour obtenir le meilleur « bachelor_rate » par groupe « éducation » ?

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Calculez la fraction de personnes avec un revenu > 50k $ par profession et triez par ordre décroissant

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Ci-dessous, nous allons créer notre DataFrame à partir de la table SQL et effectuer une analyse similaire à celle que nous avons faite avec Spark SQL mais en utilisant l'API DataFrames.  

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md 
// MAGIC Calculez le taux de divorce pour chaque occupation et triez par ordre décroissant avec DataFrames

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Faites la même chose en Spark SQL

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Transformez le DataFrame en Dataset
