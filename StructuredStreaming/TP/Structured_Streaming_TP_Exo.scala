// Databricks notebook source
// MAGIC %md
// MAGIC # TP final: utlisation the Spark StructuredStreaming et Spark DataFrames en général
// MAGIC Ce TP va etre moins detaillé et vous laisse plus de liberté de jouer avec les donneés

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Chargez les fichiers sur '/databricks-datasets/iot-stream/data-device' comme DataFrame et inspectez-les  
// MAGIC 
// MAGIC Faites les opérations habituelles

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Affichez le schéma et créez une structure avec ce schéma  
// MAGIC Ceci est nécessaire pour créer un readStream ou le schéma doit être fourni

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Créez un objet readStream et relisez les fichiers ; utiliser le schéma créé avant

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Créer la requête liée au readStream  
// MAGIC 
// MAGIC Utiliser une fenêtre (choisir la durée et la séparation) et grouper par cette fenêtre et l'id de l'appareil  
// MAGIC 
// MAGIC Agréger ensemble min, max, moyenne, somme de calories_burnt, miles_walked, num_steps 

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Assurez-vous que la source est en streaming

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Créer l'objet writeStream avec format "memory"  
// MAGIC Ceci sera le data sink

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Faites des requêtes répétées au data sink pour vous assurer qu'il reçoit des données  
// MAGIC Choisissez n'importe quelle requête; compter le nombre d'éléments suffit

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Arrêtez la requête soit avec une instruction, soit en cliquant sur la cellule

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Créer un autre objet writeStream avec la même requête  
// MAGIC Cette fois on va écrire sur des fichiers  
// MAGIC Le mode n'est pas "complete" mais "append"  
// MAGIC Donc n'oubliez pas le waterMark  
// MAGIC Attention: withWatermark(df.time...) ne marche pas, il faut utliser withWatermark("time"...)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Sauvegardez le datasink sur un répertoire an utilisant un format adapté ; essayez différents formats.
// MAGIC Est-ce que csv marche ? json ? parquet ?

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Lisez les données enregistrées dans une base de données et explorez-les ; essayez différentes requêtes sur le DataFrame

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------


