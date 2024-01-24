// Databricks notebook source
// MAGIC %md
// MAGIC # Structured Streaming

// COMMAND ----------

// MAGIC %md
// MAGIC Structured Streaming est un moteur de traitement de flux évolutif et tolérant aux pannes basé sur le moteur Spark SQL.  
// MAGIC Vous pouvez exprimer votre calcul de flux de la même manière que vous exprimeriez un calcul par lots sur des données statiques.  
// MAGIC Le moteur Spark SQL se chargera de l'exécuter de manière incrémentielle et continue et de mettre à jour le résultat final au fur et à mesure que les données de streaming continueront d'arriver.  
// MAGIC Vous pouvez utiliser l'API Dataset/DataFrame dans Scala, Java, Python ou R pour exprimer des agrégations de flux, des fenêtres événementielles, des jointures flux à lot, etc.  
// MAGIC Le calcul est exécuté sur le même moteur Spark SQL optimisé.  
// MAGIC Enfin, le système assure des garanties de tolérance aux pannes exactement une fois de bout en bout grâce à des points de contrôle et à des journaux à écriture anticipée.  
// MAGIC En bref, le streaming structuré fournit un traitement de flux rapide, évolutif, tolérant aux pannes et de bout en bout, exactement une fois, sans que l'utilisateur ait à raisonner sur le streaming.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Modèle de programmation
// MAGIC 
// MAGIC L'idée clé du streaming structuré est de traiter un flux de données en direct comme une table ajoutée en continu.  
// MAGIC Cela conduit à un nouveau modèle de traitement de flux qui est très similaire à un modèle de traitement batch.  
// MAGIC 
// MAGIC Vous allez exprimer votre calcul de flux sous forme de requête standard de type batch comme sur une table statique, et Spark l'exécute comme une requête incrémentielle sur la table d'entrée illimitée.  
// MAGIC 
// MAGIC Comprenons ce modèle plus en détail.
// MAGIC Concepts de base
// MAGIC 
// MAGIC Considérez le flux de données d'entrée comme la "table d'entrée". Chaque élément de données qui arrive sur le flux est comme une nouvelle ligne ajoutée à la table d'entrée.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Comprenons ce modèle plus en détail.  
// MAGIC Concepts de base
// MAGIC 
// MAGIC * Considérez le flux de données d'entrée comme la "table d'entrée". Chaque élément de données qui arrive sur le flux est comme une nouvelle ligne ajoutée à la table d'entrée.
// MAGIC 
// MAGIC 
// MAGIC * Une requête sur l'entrée générera le "Tableau de résultats".  
// MAGIC * À chaque intervalle de déclenchement (par exemple, toutes les 1 seconde), de nouvelles lignes sont ajoutées à la table d'entrée, qui met éventuellement à jour la table de résultats.  
// MAGIC * Chaque fois que la table de résultats est mise à jour, nous voudrions écrire les lignes de résultats modifiées dans un récepteur externe.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Output
// MAGIC 
// MAGIC 
// MAGIC L' Output est définie comme ce qui est écrit sur le stockage externe.  
// MAGIC Il peut être défini dans un mode différent :
// MAGIC 
// MAGIC * Complete mode: L'intégralité du tableau de résultats mis à jour sera écrite sur le stockage externe. C'est au connecteur de stockage de décider comment gérer l'écriture de toute la table.
// MAGIC 
// MAGIC * Append mode: Seules les nouvelles lignes ajoutées dans le tableau de résultats depuis le dernier trigger seront écrites sur le stockage externe. Cela s'applique uniquement aux requêtes où les lignes existantes dans la table de résultats ne sont pas censées changer.
// MAGIC 
// MAGIC * Update mode: Seules les lignes qui ont été mises à jour dans la table de résultats depuis le dernier trigger seront écrites sur le stockage externe (disponible depuis Spark 2.1.1). Notez que ceci est différent du mode complet en ce que ce mode ne produit que les lignes qui ont changé depuis le dernier trigger. Si la requête ne contient pas d'agrégations, elle sera équivalente au mode Append.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Sources d'entrée
// MAGIC 
// MAGIC Il existe quelques sources intégrées.
// MAGIC 
// MAGIC * Source de fichier: Lit les fichiers écrits dans un répertoire sous forme de flux de données. Les fichiers seront traités dans l'ordre d'heure de modification des fichiers. Si latestFirst est défini, l'ordre sera inversé. Les formats de fichiers pris en charge sont texte, CSV, JSON, ORC, Parquet.
// MAGIC 
// MAGIC * Source Kafka: Lit les données de Kafka.
// MAGIC 
// MAGIC * Socket source (pour les tests): Lit les données texte UTF8 à partir d'une connexion socket. Notez que cela ne doit être utilisé qu'à des fins de test car cela ne fournit pas de garanties de tolérance aux pannes de bout en bout.
// MAGIC 
// MAGIC * Source rate (pour les tests) - Génère des données au nombre spécifié de lignes par seconde, chaque ligne de sortie contient un horodatage et une valeur. timestamp est un type Timestamp contenant l'heure d'envoi du message et value est de type Long contenant le nombre de messages, en commençant par 0 comme première ligne. Cette source est destinée aux tests et à l'analyse comparative.
// MAGIC 
// MAGIC * Source micro-batch (pour les tests) - Génère des données au nombre spécifié de lignes par micro-lot, chaque ligne de sortie contient un horodatage et une valeur: timestamp est un type Timestamp contenant l'heure d'envoi du message et value est de type Long contenant le nombre de messages, en commençant par 0 comme première ligne. Contrairement à la source de données de taux, cette source de données fournit un ensemble cohérent de lignes d'entrée par micro-lot, quelle que soit l'exécution de la requête (configuration du déclencheur, requête en retard, etc.), par exemple, le lot 0 produira 0 ~ 999 et le lot 1 produira 1000 ~ 1999, et ainsi de suite. Il en va de même pour le temps généré. Cette source est destinée aux tests et à l'analyse comparative.
// MAGIC 
// MAGIC Certaines sources ne sont pas tolérantes aux pannes car elles ne garantissent pas que les données puissent être relues; il ne faut pas les utliser en production

// COMMAND ----------

// MAGIC %md
// MAGIC # Flux de travail
// MAGIC 
// MAGIC * Créez un objet readStream ; c'est un DataFrame, il est donc accessible comme n'importe quel DataFrame
// MAGIC * L'objet peut avoir plusieurs types de sources : fichiers, sources Kafka, etc
// MAGIC * Créez une requête à partir de cet objet readStream ; cette requête est la source de données réelle
// MAGIC * Créer un objet writeStream qui sert de sortie de données
// MAGIC * l'objet writeStream peut avoir plusieurs formats : une table SQL, des fichiers, la console etc
// MAGIC * le writeStream est lié à la requête créée avant
// MAGIC * La chaîne readStream, query, writeStream constitue un flux de données temporel  
// MAGIC * Les données arrivent à la source, elles sont traitées par la requête et le résultat est stocké dans le récepteur
// MAGIC * Tous les objets impliqués sont DataFrame donc les techniques vues pour le DataFrame peuvent être utilisées

// COMMAND ----------

// MAGIC %md
// MAGIC # Un exemple
// MAGIC 
// MAGIC On utilisera les fichiers dans /databricks-datasets/structured-streaming/events/ mis à disposition par Databricks  
// MAGIC 
// MAGIC Il s'agit de 50 fichiers JSON avec schéma time and action
// MAGIC * time est un timestamp
// MAGIC * action est un string
// MAGIC 
// MAGIC ### option("maxFilesPerTrigger", 1) Traite une séquence de fichiers comme un flux en sélectionnant un fichier à la fois

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// COMMAND ----------

val spark = SparkSession.builder.getOrCreate()

// COMMAND ----------

val inputPath = "/databricks-datasets/structured-streaming/events/"

// Since we know the data format already, let's define the schema to speed up processing (no need for Spark to infer schema)

val jsonSchema = StructType(
  Array(
    StructField("time", TimestampType, true),
    StructField("action", StringType, true)
  )
)

val streamingInputDF = (
  spark
    .readStream
    .format("file")
    .schema(jsonSchema)               // Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  // Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)

// COMMAND ----------

// MAGIC %md
// MAGIC Il s'agit d'un DataFrame

// COMMAND ----------

streamingInputDF.getClass()

// COMMAND ----------

dbutils.fs.ls("/databricks-datasets/structured-streaming/events")

// COMMAND ----------

// MAGIC %fs head /databricks-datasets/structured-streaming/events/file-1.json

// COMMAND ----------

// MAGIC %md
// MAGIC Nous pouvons maintenant calculer le nombre d'actions "open" et "close" avec des fenêtres d'une heure.  
// MAGIC Pour ce faire, nous allons faire groupBy sur la colonne action et les fenêtres de 1 heure sur la colonne de temps.  
// MAGIC La fonction window nous permet de prendre de fenêtres de durée spécifiée, séparées d'intervalles spécifiées   
// MAGIC Remarquez que la requête est exactement comme pour les DataFrame  
// MAGIC La longueur de la fenêtre doit être >= à la séparation entre fenêtres, sinon quelques données ne sera pas inclus dans aucune fenêtre et ça donne une erreur

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingCountsDF = (                 
  streamingInputDF
    .groupBy(
      streamingInputDF("action"),
      window(streamingInputDF("time"), "1 hour", "1 hour") // Longueur de la fenêtre >= longueur de l'intervalle 
    )
    .count()
)

// COMMAND ----------

//  Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

// COMMAND ----------

streamingCountsDF.getClass()

// COMMAND ----------

// MAGIC %md
// MAGIC Créons l'objet writeStream
// MAGIC 
// MAGIC * format (memory) signifie que nous le stockons dans une table en mémoire, que nous pouvons analyser avec Sql
// MAGIC * outputMode("complete") # complete = tous les comptages doivent être dans le tableau
// MAGIC * .queryName("counts") Choisissons un nom pour la sortie ; ce nom doit être utilisé pour les requêtes que nous ferons

// COMMAND ----------

val query = (
  streamingCountsDF
    .writeStream
    .format("memory")        // memory = store in-memory table 
    .queryName("counts")     // counts = name of the in-memory table
    .outputMode("complete")  // complete = all the counts should be in the table
    .start()
)

// COMMAND ----------

// MAGIC %md
// MAGIC Faisons des requêtes répétées  
// MAGIC Étant donné que le datasink est stocké dans mamory en tant que table SQL, nous pouvons utiliser SparkSQL  
// MAGIC Faisons de nombreuses requêtes répétées et voyons que la réponse change en temps réel

// COMMAND ----------

spark.sql("select action, sum(count) as total_count from counts group by action").show()

// COMMAND ----------

spark.sql("select action, sum(count) as total_count from counts group by action").show()

// COMMAND ----------

spark.sql("select action, sum(count) as total_count from counts group by action").show()

// COMMAND ----------

spark.sql("select action, sum(count) as total_count from counts group by action").show()

// COMMAND ----------

// MAGIC %md
// MAGIC On va arreter l'objet writeStream

// COMMAND ----------

query.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC Créons maintenant un autre datasink, cette fois en utilisant des fichiers  
// MAGIC Le mode de sortie, pour le datasink de type file est "append", pas "complete"  
// MAGIC Dans ce cas, il est nécessaire de spécifier au datasink ce qu'il faut faire avec les données qui arrivent en retard  
// MAGIC L'instruction withWatermark spécifie le délai maximal pendant lequel les données peuvent être incluses dans la fenêtre actuelle et non dans une fenêtre ultérieure  
// MAGIC Il doit être utilisé lorsque le mode est "append" ou vous obtenez une erreur

// COMMAND ----------

// $"time", not "time"
val streamingCountsDF = (                 
  streamingInputDF
    .withWatermark("time", "1 hour")
    .groupBy(
      streamingInputDF("action"),
      window($"time", "1 hour", "1 hour") // Longueur de la fenêtre >= longueur de l'intervalle 
    )
    .count()
)

// COMMAND ----------

// MAGIC %md
// MAGIC Créons le datasink  
// MAGIC L'option checkpointLocation est nécessaire lorsque le mode est "append" car Spark a besoin de savoir où trouver les données pour synchroniser la mise à jour afin de ne pas répéter ni exclure des données  
// MAGIC Ici, nous spécifions le même emplacement que celui où nous sauvegardons les données

// COMMAND ----------

// Better put checkpoint and data in the same location

val path = "/FileStore/tables/test_streaming"

val query = (
  streamingCountsDF
    .writeStream
    .format("json")
    .option("checkpointLocation", path)
    .option("path", path)
    .start()
)

// COMMAND ----------

// MAGIC %md
// MAGIC Lisons les données enregistrées et explorons-les

// COMMAND ----------

val df = spark.read.format("json").load(path)

// COMMAND ----------

df.show(truncate=false)

// COMMAND ----------

query.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC Attention, pas tous le formats sont bons; le csv ne marche pas

// COMMAND ----------

val path = "/FileStore/tables/test_streaming_csv"

// COMMAND ----------

val query = (
  streamingCountsDF
    .writeStream
    .format("csv")
    .option("checkpointLocation", path)
    .option("path", path)
    .start()
)

// COMMAND ----------

// MAGIC %md
// MAGIC Tout dépend de la structure des données  
// MAGIC Essayons parquet

// COMMAND ----------

val path = "/FileStore/tables/test_streaming_parquet"

// COMMAND ----------

val query = (
  streamingCountsDF
    .writeStream
    .format("parquet")
    .option("checkpointLocation", path)
    .option("path", path)
    .start()
)

// COMMAND ----------

val df = spark.read.format("parquet").load(path)

// COMMAND ----------

df.show(truncate=false)

// COMMAND ----------

val df = spark.read.format("json").load(cpath)

// COMMAND ----------

df.show(truncate=false)

// COMMAND ----------

dbutils.fs.ls(cpath)

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/test_streaming_cp

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/test_streaming_csv

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/test_streaming

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/test_streaming_parquet

// COMMAND ----------

// MAGIC %md
// MAGIC Voici des exemples de création de Kafka data source
// MAGIC Ce code ne peut être exécuté sur Databricks parce que Kafka n'est pas installé, mais il donne une idée.
// MAGIC Remarquez que l'idée est toujours la même : créer une source de données de type readStream qui est simplement un DataFrame
// MAGIC Les détails changent en fonction du type de la source : pour Kafka par exemple, il faut spécifier les topics

// COMMAND ----------

// Subscribe to 1 topic
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to 1 topic, with headers
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .option("includeHeaders", "true")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
  .as[(String, String, Array[(String, Array[Byte])])]

// Subscribe to multiple topics
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1,topic2")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// Subscribe to a pattern
val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]

// COMMAND ----------

// MAGIC %md
// MAGIC Voici des exemples de data sink

// COMMAND ----------

// Write key-value data from a DataFrame to a specific Kafka topic specified in an option
val ds = df
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "topic1")
  .start()

// Write key-value data from a DataFrame to Kafka using a topic specified in the data
val ds = df
  .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .start()
