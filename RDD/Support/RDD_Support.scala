// Databricks notebook source
val root = "/FileStore/tables/"

// COMMAND ----------

// MAGIC %md
// MAGIC # Resilient Distributed Datasets (RDD)

// COMMAND ----------

// MAGIC %md
// MAGIC #### RDD ( Resilient Distributed Dataset ) est le concept central du framework Spark  
// MAGIC * Dataset: jeu de données qui se parcourt comme une collection  
// MAGIC * Distributed: jeu de données partitionné et chacune des partitions traitée sur un noeud
// MAGIC du cluster  
// MAGIC * Resilient: En cas de perte d’un noeud, le sous-traitement sera automatiquement relancé sur un autre noeud  
// MAGIC
// MAGIC #### RDD supporte 3 types d’operations  
// MAGIC * La Création
// MAGIC * Les transformations
// MAGIC * Les actions

// COMMAND ----------

// MAGIC %md
// MAGIC #### Création d'un RDD
// MAGIC
// MAGIC Pour créer un RDD, on peut charger les données à partir:
// MAGIC * Une collection (List), transformée en RDD avec l’opérateur ‘parallelize’
// MAGIC * Un fichier local ou distribué (HDFS) dont le format est configurable: texte brut, SequenceFile Hadoop,
// MAGIC JSON, etc.
// MAGIC * Une base de données: JDBC, Cassandra, Hbase, etc.
// MAGIC * Un autre RDD auquel on aura appliqué une transformation  
// MAGIC Le chemin inverse, exporter une RDD dans un fichier, dans une base de données ou une collection est aussi possible

// COMMAND ----------

// MAGIC %md
// MAGIC ### Déclencher Spark
// MAGIC
// MAGIC À partir de Spark 2.0 `SparkSession` est le point d'entrée unifié d'une application Spark de Spark  
// MAGIC
// MAGIC Il fournit un moyen d'interagir avec diverses fonctionnalités de Spark avec un nombre moindre de constructions.  
// MAGIC
// MAGIC Anciennement on utilisait `SparkContext`, mais pas
// MAGIC aujourd'hui  
// MAGIC Néanmois un `SparkContext`, est maintenant encapsulé dans `SparkSession`
// MAGIC  
// MAGIC ### Donc la premiére étape de toute application Spark est la crétion de `SparkSession`

// COMMAND ----------

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("RDD tutorial").getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC ### SparkContext
// MAGIC
// MAGIC Étant donné que RDD est l'ancienne abstraction de données Spark, ils doivent être créés avec SparkContext, pas SparkSession
// MAGIC
// MAGIC De nos jours, nous créerions toujours la SparkSession et utiliserions le SparkContext que SparkSession met à notre disposition

// COMMAND ----------

val sc = spark.sparkContext

// COMMAND ----------

// MAGIC %md
// MAGIC Examinons la configuration

// COMMAND ----------

val config=sc.getConf.getAll

for (conf <- config)
  println(conf._1 +", "+ conf._2)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Créer RDD en utilisant sparkContext.parallelize()
// MAGIC
// MAGIC En utilisant la fonction parallelize() de SparkContext (sparkContext.parallelize()), vous pouvez créer un RDD  
// MAGIC Cette fonction charge la collection existante de votre programme pilote dans RDD de parallélisation

// COMMAND ----------

// Create RDD from parallelize    
val data = List(1,2,3,4,5,6,7,8,9,10,11,12)
val rdd = sc.parallelize(data, 1)
rdd.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Assurons-nous que il s'agit d'un RDD

// COMMAND ----------

rdd.getClass()

// COMMAND ----------

// MAGIC %md
// MAGIC Valable aussi

// COMMAND ----------

rdd.getClass

// COMMAND ----------

// MAGIC %md
// MAGIC ### Partitionnement des données
// MAGIC
// MAGIC Remarquez  
// MAGIC `val rdd = sc.parallelize(data, 1)`  
// MAGIC
// MAGIC les `1` paramètre indiquet à Spark de diviser les données en 1 partitions
// MAGIC
// MAGIC #### La clé de l'efficacité de Spark est que les données sont divisées en plusieurs partitions, chacune stockée sur un nœud de cluster différent et traitée en parallèle.  
// MAGIC
// MAGIC Vous pouvez spécifier le nombre de partitions, ou laisser Spark le soin de choisir un nombre adéquat en fonction des caractéristiques de votre machine

// COMMAND ----------

// MAGIC %md
// MAGIC La fonction `glom()` vous permet de voire le partitionnement

// COMMAND ----------

rdd.glom.collect

// COMMAND ----------

// MAGIC %md
// MAGIC Valable aussi

// COMMAND ----------

rdd.glom.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Le partitionnement peut être changé à l'aide de deux
// MAGIC fontions, `repartition()` et `coalesce()`
// MAGIC
// MAGIC * `repartition()` peut augmenter ou diminuer le partitionnement
// MAGIC
// MAGIC * `coalesce()` peut seulement le diminuer, mais il est plus rapide  
// MAGIC
// MAGIC Exemples

// COMMAND ----------

val rdd1 = rdd.repartition(2)

// COMMAND ----------

rdd1.glom.collect

// COMMAND ----------

// MAGIC %md
// MAGIC Valable aussi

// COMMAND ----------

rdd1.glom.collect()

// COMMAND ----------

val rdd2 = rdd.coalesce(2)

// COMMAND ----------

rdd2.glom().collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Ceci ne fait rien: `coalesce()` peut seulement diminuer le partionnement des données

// COMMAND ----------

// MAGIC %md
// MAGIC ## Attention `repartition()` et `coalesce()` sont des opérations coûteuses: ne les déclenchez pas si ça n'est pas vraiment nécessaire

// COMMAND ----------

// MAGIC %md
// MAGIC ## Create RDD using sparkContext.textFile()
// MAGIC
// MAGIC Using textFile() method we can read a text (.txt) file into RDD.

// COMMAND ----------

// Create RDD from external Data source
import java.io.File

val path = new File(root,"sample.txt").getPath

val rdd2 = spark.sparkContext.textFile(path)

rdd2.collect

// COMMAND ----------

// MAGIC %md
// MAGIC ## Créer un RDD vide à l'aide de sparkContext.emptyRDD
// MAGIC
// MAGIC En utilisant la méthode emptyRDD() sur sparkContext, nous pouvons créer un RDD sans données  
// MAGIC Cette méthode crée un RDD vide sans partition.

// COMMAND ----------

// Creates empty RDD with no partition    
val rdd = spark.sparkContext.emptyRDD
// Not emptyRDD()
rdd.collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Créer un RDD vide avec partitions
// MAGIC
// MAGIC Parfois, nous pouvons avoir besoin d'écrire un RDD vide dans des fichiers par partition  
// MAGIC Dans ce cas, vous devez créer un RDD vide avec des partitions

// COMMAND ----------

// Create empty RDD with partition
val rdd2 = spark.sparkContext.parallelize(Seq.empty[String], 10)
// This creates 10 partitions

// COMMAND ----------

println("Num of Partitions: "+rdd2.getNumPartitions)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Paralléliser les RDDs
// MAGIC
// MAGIC Lorsque nous utilisons les méthodes parallelize() ou textFile() ou wholeTextFiles() de SparkContxt pour lancer RDD, il divise automatiquement les données en partitions en fonction de la disponibilité des ressources  
// MAGIC
// MAGIC Lorsque vous l'exécutez, il crée des partitions correspondant au même nombre de cœurs disponibles sur votre système  

// COMMAND ----------

// MAGIC %md
// MAGIC #### getNumPartitions()  
// MAGIC Il s'agit d'une fonction RDD qui renvoie le nombre de partitions dans lesquelles notre ensemble de données est divisé
// MAGIC
// MAGIC Attention: getNumPartitions sans () donne une erreur

// COMMAND ----------

val data = List(1,2,3,4,5,6,7,8,9,10,11,12)

val rdd = spark.sparkContext.parallelize(data, 3)
println("initial partition count:", rdd.getNumPartitions)

// COMMAND ----------

val rdd = spark.sparkContext.parallelize(data, 6)
println("initial partition count:", rdd.getNumPartitions)

// COMMAND ----------

val rdd=spark.sparkContext.parallelize(data)
println("initial partition count:", rdd.getNumPartitions)

// COMMAND ----------

// MAGIC %md
// MAGIC Visualisons le RDD
// MAGIC
// MAGIC # Attention: collect est une opération coûteuse qui peut étouffer votre mémoire si votre jeux de données est volumineux
// MAGIC # Ne l'utilisez pas systématiquement pour inspecter vos données

// COMMAND ----------

rdd.collect

// COMMAND ----------

// MAGIC %md
// MAGIC Assurons nous que il s'agit d'un RDD

// COMMAND ----------

rdd.getClass

// COMMAND ----------

// MAGIC %md
// MAGIC Si vous avez des gros jeux de données, vous pouvez les inspecter en sélectionnant des éléments de façon aléatoire.

// COMMAND ----------

val data = sc.parallelize(Range(1,100))

// COMMAND ----------

data.takeSample(false, 1, seed = 10L)

// COMMAND ----------

data.takeSample(false, 10, seed = 8L)

// COMMAND ----------

// MAGIC %md
// MAGIC # repartition et coalesce
// MAGIC
// MAGIC Souvent il est nécéssaire de changer le nombre de partitions d'un RDD  
// MAGIC
// MAGIC PySpark a deux fonctions, repartition() et coalsece()
// MAGIC
// MAGIC * repartition() est utilisé pour augmenter ou diminuer les partitions  
// MAGIC * coalesce() est utilisé uniquement pour diminuer uniquement le nombre de partitions mais il est plus rapide
// MAGIC
// MAGIC **Néanmoins, il s'agit d'opérations coûteuses, à exécuter avec prudence**

// COMMAND ----------

val rdd = spark.sparkContext.parallelize(List(1,2,3,4,56,7,8,9,12,3), 10)
rdd.getNumPartitions

// COMMAND ----------

rdd.coalesce(4).getNumPartitions

// COMMAND ----------

rdd.repartition(12).getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC ## Transformations vs Actions
// MAGIC
// MAGIC Les RDD prennent en charge deux types d'opérations :  
// MAGIC * les transformations, créent un nouvel ensemble de données à partir d'un ensemble existant
// MAGIC
// MAGIC * les actions, renvoient une valeur au programme pilote après avoir exécuté un calcul sur l'ensemble de données
// MAGIC
// MAGIC Par exemple, `map` est une transformation qui transmet chaque élément de l'ensemble de données via une fonction et renvoie un nouveau RDD représentant les résultats  
// MAGIC
// MAGIC D'autre part, `reduce` est une action qui agrège tous les éléments du RDD à l'aide d'une fonction et renvoie le résultat final au programme pilote
// MAGIC
// MAGIC Toutes les transformations dans Spark sont **paresseuses**, en ce sens qu'elles ne calculent pas leurs résultats immédiatement  
// MAGIC
// MAGIC Au lieu de cela, ils se souviennent simplement des transformations appliquées à un ensemble de données de base (par exemple, un fichier)  
// MAGIC
// MAGIC Les transformations ne sont calculées que lorsqu'une action nécessite qu'un résultat soit renvoyé au programme pilote  
// MAGIC
// MAGIC Cette conception permet à Spark de fonctionner plus efficacement
// MAGIC
// MAGIC Par exemple, nous pouvons réaliser qu'un ensemble de données créé via `map` sera utilisé dans une réduction et ne renverra que le résultat de la réduction au pilote, plutôt que l'ensemble de données mappé plus grand
// MAGIC
// MAGIC Voici un exemple

// COMMAND ----------

// MAGIC %md
// MAGIC # Transformations RDD
// MAGIC
// MAGIC #### Le transformations produisent toujours un autre RDD
// MAGIC
// MAGIC Tout d'abord créons un RDD à partir d'un fichier texte

// COMMAND ----------

import java.io.File

val path = new File(root,"test.txt").getPath

val rdd = spark.sparkContext.textFile(path)
val c = rdd.collect()

// COMMAND ----------

println(c.length)
println
c

// COMMAND ----------

// MAGIC %md
// MAGIC La transformation flatMap() renvoie un RDD avec potentiellement un nombre des éléments diffèrent du RDD d'origine  
// MAGIC Dans l'exemple ci-dessous  
// MAGIC * Le RDD d'origine contient un élément pour chaque ligne du fichier
// MAGIC * Le nouveau RDD contient un élément pour chaque mot du fichier

// COMMAND ----------

val rdd = spark.sparkContext.textFile(path)
val rdd2 = rdd.flatMap(x => x.split(" "))
val c = rdd2.collect()

// COMMAND ----------

println(c.length)
println
c

// COMMAND ----------

// MAGIC %md
// MAGIC La transformation map() renvoie un RDD avec le même nombre des éléments du RDD d'origine  

// COMMAND ----------

val rdd = spark.sparkContext.textFile(path)
val rdd2 = rdd.map(x => x.split(" "))
val c = rdd2.collect()

// COMMAND ----------

println(c.length)
println
c

// COMMAND ----------

// MAGIC %md
// MAGIC La transformation map() renvoie un RDD avec le même nombre des éléments du RDD d'origine  
// MAGIC Ici nous avons tranformé chaque élément du RDD d'origine en un nouveau élément consistant d'un tuple de la forme ('mots d'origine', 'valeur 1')  
// MAGIC Cela nous donne un RDD dont les éléménts sont dans une forme **(clé, valeur)**

// COMMAND ----------

val rdd = spark.sparkContext.textFile(path)
val rdd2 = rdd.flatMap(x => x.split(" "))
val rdd3 = rdd2.map(x => (x, 1))
val c = rdd3.collect()

// COMMAND ----------

println(c.length)
println
c

// COMMAND ----------

// MAGIC %md
// MAGIC La transformation reduceByKey applique la fonction spécifiée à chaque valeur de la clé des paires (clé, valeurs) du RDD d'origine  
// MAGIC Cela nous donnes le nombre des fois où chaque clé apparaît dans le RDD d'origine

// COMMAND ----------

val rdd = spark.sparkContext.textFile(path)
val rdd2 = rdd.flatMap(x => x.split(" "))
val rdd3 = rdd2.map(x => (x, 1))
val rdd4 = rdd3.reduceByKey(_ + _)
val c = rdd4.collect()

// COMMAND ----------

println(c.length)
println
c

// COMMAND ----------

// MAGIC %md
// MAGIC La transformation map() renvoie un nouveau RDD ou les éléments de chaque tuple ont changé de place  
// MAGIC Car le nombre d'éléments des 2 RDD est le même l'utilisation de map() est approprié

// COMMAND ----------

val rdd = spark.sparkContext.textFile(path)
val rdd2 = rdd.flatMap(x => x.split(" "))
val rdd3 = rdd2.map(x => (x, 1))
val rdd4 = rdd3.reduceByKey(_ + _)
val rdd5 = rdd4.map(x => (x._2, x._1))
val c = rdd5.collect()

// COMMAND ----------

println(c.length)
println
c

// COMMAND ----------

// MAGIC %md
// MAGIC La transformation sortByKey() trie chaque paire clé-valeur par la valeur de la clé

// COMMAND ----------

val rdd = spark.sparkContext.textFile(path)
val rdd2 = rdd.flatMap(x => x.split(" "))
val rdd3 = rdd2.map(x => (x,1))
val rdd4 = rdd3.reduceByKey(_ + _)
val rdd5 = rdd4.map(x => (x._2, x._1))
val rdd6 = rdd5.sortByKey()
val c = rdd6.collect()

// COMMAND ----------

println(c.length)
println
c

// COMMAND ----------

// MAGIC %md
// MAGIC ## Actions RDD
// MAGIC
// MAGIC #### Les actions produisent toujours un résultat

// COMMAND ----------

val r = spark.sparkContext.textFile(path)
  .flatMap(x => x.split(" "))
  .map(x => (x,1))
  .reduceByKey(_ + _)
  .map(x => (x._2, x._1))
  .count()

println(s"Count : $r")

// COMMAND ----------

// Action - first
val firstRec = rdd6.first()
println(firstRec.getClass())
println(s"First Record : $firstRec")

// COMMAND ----------

// Action - max
val datMax = rdd6.max()
println(s"Max Record : $datMax")

// COMMAND ----------

// MAGIC %md
// MAGIC L'action reduce applique une fonction aux lignes du RDD  
// MAGIC Ici nous comptons les nombre d'éléments du RDD

// COMMAND ----------

// Action - reduce
val totalWordCount = rdd6.reduce((a, b) => (a._1 + b._1, a._2))
println(s"dataReduce Record : $totalWordCount")
println(totalWordCount.getClass())

// COMMAND ----------

// MAGIC %md
// MAGIC L'action take prends les premiers éléments

// COMMAND ----------

// # Action - take
val data3 = rdd6.take(3)
for (d <- data3)
    println(d)

// COMMAND ----------

// MAGIC %md
// MAGIC collect() Renvoie toutes les données de RDD sous forme de tableau  
// MAGIC Soyez prudent lorsque vous utilisez cette action lorsque vous travaillez avec un gros RDD car vous risquez d'étouffer la mémoire

// COMMAND ----------

// Action - collect
val data = rdd6.collect()
for (f <- data)
    print(f)

// COMMAND ----------

// MAGIC %md
// MAGIC Sauvetage sur fichier
// MAGIC Remarquez
// MAGIC * Vous spécifiez le nom d'un répertoire, pas d'un fichier; donc on l'appelle wordCount
// MAGIC * Le RDD est sauvegardé dans le répertoire sur plusieurs fichiers

// COMMAND ----------

// MAGIC %fs rm -r /FileStore/tables/wordCount

// COMMAND ----------

import java.io.File

val path = new File(root,"wordCount").getPath

rdd6.saveAsTextFile(path)

// COMMAND ----------

// MAGIC %md
// MAGIC Si on essaie d'exécuter le sauvetage une deuxième fois, il donne une erreur, car le répertoire existe déjà  
// MAGIC
// MAGIC Il n'y a pas de façons de changer ça pour les RDD  
// MAGIC
// MAGIC Pour le DataFrame, il est possible

// COMMAND ----------

// MAGIC %md
// MAGIC ## Créer un DataFrame à partir d'un RDD

// COMMAND ----------

val columns = Seq("language","users_count")
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

val rdd = spark.sparkContext.parallelize(data)

// COMMAND ----------

// MAGIC %md
// MAGIC ### utiliser toDF() function

// COMMAND ----------

val dfFromRDD1 = rdd.toDF
dfFromRDD1.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Donner des noms de colonnes

// COMMAND ----------

val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

val dfFromRDD1 = rdd.toDF("language", "users_count")
dfFromRDD1.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Valable aussi

// COMMAND ----------

val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
val columns = Seq("language","users_count")

val dfFromRDD1 = rdd.toDF(columns:_*)
dfFromRDD1.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Utiliser createDataFrame() from SparkSession

// COMMAND ----------

val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
dfFromRDD2.printSchema
