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



// COMMAND ----------

// MAGIC %md
// MAGIC # Exminer la configuration à l'aide de Spark Context

// COMMAND ----------



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



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC # Faites la même chose sans utiliser un RDD

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



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



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Affichez les noms des colonnes

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Affichez le schema du fichier 

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Faites un résumé statistique

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Comptage de nombre de lignes

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Affichez les 5 premieres lignes

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Affichez les dernières 10 lignes

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Selectionnez quelques colonnes de la Dataframe  
// MAGIC Utilisez plus d'une syntaxe 

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Comptage de nombre des dates unique dans la Dataframe ( Hint: utilisation de .distinct())

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Créez une nouvelle colonne à partir de colonne existante (utilisez withColumn)

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Calcul manuel du moyenne ('mean') de la colonne 'Close' en aggrégant suivant la 'Date'

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Calculez la moyenne, les max et min et le count de Open, High, Low  et renommez les résultats

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Créez une SQL View à partir du DataFrame et selectionnez tout à partir de cette view

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Selectionnez la date et le volume maximum de chaque data en utilisant la SQL view

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Supprimez la colonne Close du DataFrame  
// MAGIC Affichez les colonne du résultat

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Créez un nouveau DataFrame en supprimant la colonne 'Adj Close' 

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC  Vérifiez si des champs sont dans ce dataframe nulls  
// MAGIC  Essayez différents façons  
// MAGIC  Utilisez Spark SQL aussi

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



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



// COMMAND ----------

// MAGIC %md
// MAGIC ORC

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Parquet

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC JSON

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC XML

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Avro

// COMMAND ----------


