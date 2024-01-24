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



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Lisez le fichier '/FileStore/tables/titanic.csv' en deux façons différentes et inspectez-le  

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Combinen des passagers il y a dans le DataFrame ?

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Affichez et comptez le nombre de passagers de sex feminin; essayes plusieurs syntaxes

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Careful with the quotes

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Afficher âge et sexe des passagers de sexe féminin et don l'âge est > 20 ans

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Créee un SQL temporary view et sélectionnez Age et Sex ou le Sex est féminin et l'Age > 20

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Compter les nombre de passgers de sexe féminin et don l'âge est > 20 ans

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Compter le nombre de passagers dont l'âge n'est pas null

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Calculez l'âge moyen, mimimum, maximum et rénommez les colonne

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Ajoutez une colonne donnant l'age en mois et donnez un nom significatif

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Transformez la colonne Age de façon que il donne l'âge en mois

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Affichez l'âge moyen des passagers groupés par classe et port de départ et triez la sortie par classe et port de départ  
// MAGIC Trouvez les bonnes colonnes en examinant le schéma

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Crées un nouveau DataFrame ou pour les passagers dont l'âge est null dans le DataFrame d'origine la moyenne est imputée  
// MAGIC Comptez le nombre des nulls dans le deux DataFrames

// COMMAND ----------

// Voici comment extraire l'âge moyen comme valeur double
// asInstanceOf[Double] est nécessaire pout transfromer la valeur en Double 
// df.agg(mean("Age")).head()(0).asInstanceOf[Double]

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC L'âge moyen doit être le même dans les deux DataFrames

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Combien des passagers de sexe masculin et féminin il y a dans le DataFrame ?

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Calculez la probabilité de survie des passagers de sexe masculin et féminin  
// MAGIC Hint: la fonction count() utilisée pour visualiser les DataFrame revoie un entier dont la valeur peut être utilisée pour calculer la probabilité

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Sauvegardez le dataFrame en format csv et parquet

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------


