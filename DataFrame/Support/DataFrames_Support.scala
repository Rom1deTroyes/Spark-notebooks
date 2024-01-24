// Databricks notebook source
// MAGIC %md
// MAGIC Répertoire où les fichiers seront stockés en Databricks  
// MAGIC Changez-lz si vous utilisez des fichiers locaux

// COMMAND ----------

val root = "/FileStore/tables/"

// COMMAND ----------

// MAGIC %md
// MAGIC # Spark `DataFrame`
// MAGIC
// MAGIC * Spark a commencé avec les `RDD` dont la syntaxe est un peu difficile !  
// MAGIC
// MAGIC * Spark 2 est allé vers le syntaxe `DataFrame` qui est beaucoup plus facile pour la manipulation de données
// MAGIC
// MAGIC * Spark dataFrame DF sont capables d’importer et exporter la donnée provenant d'une grande variété de sources.
// MAGIC
// MAGIC * Nous pouvons ensuite utiliser ces DataFrames pour appliquer diverses transformations sur les données.
// MAGIC
// MAGIC Spark DF contiennent des données dans un format de colonne et de ligne  
// MAGIC * Chaque colonne représente une caractéristique ou une variable ou un feature  
// MAGIC
// MAGIC * Chaque ligne représente un point de données individuel ou une observation 

// COMMAND ----------

// MAGIC %md
// MAGIC # Ce noterbook va être un peu long
// MAGIC * Beaucoup de choses à voir
// MAGIC * Mais c'est la partie la plus importante
// MAGIC * Si vous connaissez SQL ça va être utile
// MAGIC
// MAGIC ## Résumé
// MAGIC
// MAGIC * Crer un point d'entrée à Spark
// MAGIC * Créer un DataFrame
// MAGIC * Visualiser un DataFrame (instruction `show()`)
// MAGIC * Sélectionner les colonnes de DataFrame (instruction `select()`)
// MAGIC * Modifier les colonnes d'un DataFrame (instructions `withColumn(), withColumnRenamed()`)
// MAGIC * Sélectionner les lignes d'un DataFrame (instruction `filter(), where()`)
// MAGIC * Examine un DataFrame à l'aide d'expressions SQL (`Spark SQL`)
// MAGIC * Trier un DataFrame (insturctions `sort(), orderBy()`)
// MAGIC * Agrégations (instructions `groupBy(), agg()`)
// MAGIC * Jointure de DataFrame (instruction `join()`)
// MAGIC * Substitution de valeurs nulles (instructions `fillna(), fill()`)
// MAGIC * Imputations de valeurs manquantes
// MAGIC * Sauvegarde de DataFrame en csv et parquet

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC # Créer le point d'entrée à Spark
// MAGIC
// MAGIC * Aujoud'hui, à partir de lintroduction des DataFrame, on utilise SparkSession
// MAGIC * Avant Spark 2.0 on utlisait SparkContext
// MAGIC * SparkSession vous permet de créer un SparkContex si besoin
// MAGIC
// MAGIC Vous pouvez créer autant de SparkSession que vous le souhaitez dans une application ScalaSpark en utilisant SparkSession.builder() ou SparkSession.newSession().

// COMMAND ----------

/*
Importation nécessaire  
Toujours le faire au début
Un objet SparkSession appellé `spark` est créé  
Mais on peut en créer nous-même si on a besoin de définir des valeurs de configuration particulières.
*/

import org.apache.spark.sql.SparkSession

// COMMAND ----------

// Create SparkSession from builder

val spark1 = SparkSession.builder.master("local[2]")
    .appName("Un nom significatif")
    .getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC master() - Si vous l'exécutez sur le cluster, vous devez utiliser votre nom de maître comme argument de master()  
// MAGIC
// MAGIC généralement, il s'agirait de yarn ou de mésos en fonction de la configuration de votre cluster
// MAGIC (Attention: mesos est désormais obsolète et pas conseillé)
// MAGIC
// MAGIC Utilisez local[x] lors de l'exécution en mode autonome  
// MAGIC
// MAGIC x doit être une valeur entière et doit être supérieur à 0  
// MAGIC
// MAGIC cela représente le nombre de partitions qu'il doit créer lors de l'utilisation de RDD, DataFrame et Dataset  
// MAGIC
// MAGIC Idéalement, la valeur x devrait être le nombre de cœurs de processeur dont vous disposez]
// MAGIC
// MAGIC Utilisez `local[*]` pour faire choisir le nombre de cœurs à Spark

// COMMAND ----------

// Obtenir le SparkContext

val sc = spark1.sparkContext

// COMMAND ----------

// Examiner la configuration
val config=sc.getConf.getAll

for (conf <- config)
  println(conf._1 +", "+ conf._2)

// COMMAND ----------

// local[*] est aussi valable; il laisse à l'ordi le soin des sélectionner le nombre de processeurs
val spark2 = SparkSession.builder.master("local[*]").appName("Autre nom").getOrCreate()

// COMMAND ----------

// config vous permet de spécifier des options de configuration particulières
val spark3 = SparkSession.builder
  .master("local[1]")
  .appName("Un autre exemple")
  .config("spark.some.config.option", "config-value")
  .getOrCreate()


// COMMAND ----------

// Créer SparkSession avec support pour Hive 

// warehouse_location points to the default location for managed databases and tables

import java.io.File

val warehouseLocation = new File("spark-warehouse").getAbsolutePath

val spark = SparkSession
  .builder
  .appName("Python Spark SQL Hive integration example")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()

// COMMAND ----------

// Valable aussi: c'est la config minimale
val spark = SparkSession.builder.getOrCreate()

// Aussi valable
// val spark: SparkSession = SparkSession.builder().getOrCreate()

// COMMAND ----------

// MAGIC %md
// MAGIC # Spark – Créer un DataFrame
// MAGIC
// MAGIC #### Create DataFrame from RDD

// COMMAND ----------

// MAGIC %md
// MAGIC Pour créer un DataFrame il faut spécifier la valeurs des lignes du DataFrame et, en général, le nom dec colonnes  
// MAGIC Donc il faut utiliser des structures des données adaptées au stockage de plusieurs éléments  
// MAGIC La structure recommandée en Scala et la liste (List), mais Array et Seq sony valables aussi  
// MAGIC Seq représente une collection immuable tout comme List  
// MAGIC En revanche Array est muable

// COMMAND ----------

// MAGIC %md
// MAGIC Exemple avec List

// COMMAND ----------

// Avec toDF()
//val columns = List("language","users_count")
val data = List(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

// Ex List
val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF()
display(dfFromRDD1)

// COMMAND ----------

dfFromRDD1.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Choses à remarquer :
// MAGIC
// MAGIC * Nous n'avons pas spécifié un schéma ; Spark a assumé _1 et _2 comme noms de colonnes est string comme type.  
// MAGIC * Nous avons utlisé display pour la visualisation; show est valable aussi, mais moins joli.  
// MAGIC
// MAGIC En plus nous avons les avantages suivants sont lorsque nous utilisons display au lieu de show :
// MAGIC * Un maximum de 1 000 enregistrements sont affichés
// MAGIC * Vous pouvez exporter le tableau rendu au format CSV (le bouton existe)
// MAGIC * Vous pouvez trier les résultats par colonne en un clic sur le nom de la colonne  
// MAGIC
// MAGIC ## Attention: display existe sur le Databricks notebooks seulement  
// MAGIC https://stackoverflow.com/questions/46125604/databricks-display-function-equivalent-or-alternative-to-jupyter  
// MAGIC Sur jupyter notebook ou ligne de commande n'est pas disponible  
// MAGIC En revanche show est toujours disponible

// COMMAND ----------

// Ex List
// Affichons le résultat avec show pour varier
val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF()
dfFromRDD1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Affichons le schéma avec printSchema
// MAGIC * Il a assumé des string
// MAGIC * nullable = true veut dire que les valeurs nulles sont possible; c'est le défaut
// MAGIC * Faute de spécification de noms il a assumé _1 et _2

// COMMAND ----------

dfFromRDD1.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Exemple avec Array

// COMMAND ----------

// Avec toDF()
val data = Array(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
// Ex Array

val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF()
display(dfFromRDD1)

// COMMAND ----------

// MAGIC %md
// MAGIC Exemple avec Seq

// COMMAND ----------

// Avec toDF()
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
// Ex Seq

val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF()
display(dfFromRDD1)

// COMMAND ----------

// MAGIC %md
// MAGIC Cette fois on va spécifier un schéma  
// MAGIC Les colonnes s'appellent "language","users_count" comme spécifié

// COMMAND ----------

// Ex 2
val dfFromRDD1 = rdd.toDF("language","users_count")
dfFromRDD1.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Autre exemple de spécification du schéma  
// MAGIC L'astérisque signifie "Déballez la liste et considérez chacun de ses éléments un par un."  
// MAGIC Ici List est utilisé; Array et Seq sont valables aussi  

// COMMAND ----------

// Ex 3

val columns = List("language","users_count")
val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
dfFromRDD2.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Créer Spark DataFrame à partir de List et Seq Collection avec schéma et `Row` type
// MAGIC
// MAGIC createDataFrame() a une autre signature qui prend le type et le schéma RDD[Row] pour les noms de colonne comme arguments.  
// MAGIC
// MAGIC En Spark l'interface Row représente une ligne de sortie d’un opérateur relationnel comme une instruction SQL.  
// MAGIC Notez que Row est importé de SparkSQL : import org.apache.spark.sql.Row  
// MAGIC
// MAGIC Voici le lien : https://spark.apache.org/docs/3.0.0/api/scala/org/apache/spark/sql/Row.html
// MAGIC Il faut spécifier le schéma avec StructType comme ça 
// MAGIC
// MAGIC Pour l'utiliser, nous devons d'abord convertir notre objet "rdd" de RDD[T] en RDD[Row] et définir un schéma à l'aide de StructType & StructField.

// COMMAND ----------

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
val schema = StructType(Array(
                 StructField("language", StringType,true),
                 StructField("users", StringType,true)
             ))
val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
val dfFromRDD3 = spark.createDataFrame(rowRDD,schema)
display(dfFromRDD3)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Créer DataFrame à partir de `List Collection`
// MAGIC
// MAGIC Dans cette section, nous verrons plusieurs approches pour créer Spark DataFrame à partir de la collection Seq[T] ou List[T].  
// MAGIC Ces exemples seraient similaires à ce que nous avons vu dans la section ci-dessus avec RDD, mais nous utilisons l'objet "data" au lieu de l'objet "rdd".

// COMMAND ----------

// MAGIC %md
// MAGIC Avec List, no schéma spécifie

// COMMAND ----------

val data = List(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

// import spark.implicits._
val dfFromData1 = data.toDF()
display(dfFromData1)
dfFromData1.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Avec Seq, no schéma spécifié

// COMMAND ----------

val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

// import spark.implicits._
val dfFromData1 = data.toDF()
dfFromData1.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Avec List, schéma spécifié  

// COMMAND ----------

val columns = List("language","users_count")
val data = List(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

val dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
dfFromData2.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Exemple avec Row  
// MAGIC En Spark l'interface Row représente une ligne de sortie d’un opérateur relationnel comme une instruction SQL.  
// MAGIC Lien : https://spark.apache.org/docs/3.0.0/api/scala/org/apache/spark/sql/Row.html  
// MAGIC Il faut spécifier le schéma avec StructType comme ça  

// COMMAND ----------

import scala.collection.JavaConversions._

import org.apache.spark.sql.types.{StringType, StructField, StructType}

val schema = StructType(
  List(
    StructField("language", StringType,true),
    StructField("users", StringType,true)
  )
)

val rowData= List(Row("Java", "20000"), Row("Python", "100000"), Row("Scala", "3000"))
var dfFromData3 = spark.createDataFrame(rowData,schema)
dfFromData3.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### Utiliser createDataFrame() from SparkSession
// MAGIC Ici on utlilise Seq pour varier  
// MAGIC List et Array sont valables aussi  

// COMMAND ----------

val columns = Seq("language","users_count")
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

// COMMAND ----------

// MAGIC %md
// MAGIC Pour la visualisation on utlise show

// COMMAND ----------

val dfFromData = spark.createDataFrame(data).toDF(columns:_*)
dfFromData.printSchema()
dfFromData.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Qeul est le type ?

// COMMAND ----------

dfFromData.getClass

// COMMAND ----------

val dfFromData = data.toDF()
dfFromData.printSchema()
dfFromData.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Valable aussi

// COMMAND ----------

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

import scala.jdk.CollectionConverters

val schema = StructType(
  Array(
    StructField("language", StringType,true),
    StructField("users", StringType,true)
  )
)

import scala.collection.JavaConversions._

val rowData= Seq(
  Row("Java", "20000"), 
  Row("Python", "100000"), 
  Row("Scala", "3000")
)
val dfFromData3 = spark.createDataFrame(rowData,schema)

dfFromData.printSchema()
dfFromData.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Créer DataFrame avec schéma
// MAGIC
// MAGIC Les classes StructType et StructField sont utilisées pour spécifier le schéma dans le DataFrame et créer des colonnes complexes telles que des colonnes de structure, de tableau et de carte imbriquées.  
// MAGIC StructType est une collection de StructField qui définit le nom de la colonne, le type de données de la colonne, booléen pour spécifier si le champ peut être nullable ou non.

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

// Utlisons List
val data = List(
    Row("James","","Smith","36636","M",3000),
    Row("Michael","Rose","","40288","M",4000),
    Row("Robert","","Williams","42114","M",None),
    Row("Maria","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1)
  )

val schema = StructType(List (
    StructField("firstname",StringType,true),
    StructField("middlename",StringType,true),
    StructField("lastname",StringType,true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
  ))

val df = spark.createDataFrame(data,schema)
df.printSchema()

// Affichons avec show pour varier
df.show(truncate=false)

// COMMAND ----------

// Utlisons Seq

val data = Seq(
    Row("James","","Smith","36636","M",3000),
    Row("Michael","Rose","","40288","M",4000),
    Row("Robert","","Williams","42114","M",None),
    Row("Maria","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1)
  )

val schema = StructType(Array (
    StructField("firstname",StringType,true),
    StructField("middlename",StringType,true),
    StructField("lastname",StringType,true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
  ))

val df = spark.createDataFrame(data,schema)
df.printSchema()
df.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Créer DataFrame de sources de données
// MAGIC
// MAGIC ### Créer DataFrame d'un fichier csv
// MAGIC
// MAGIC On peut utiliser
// MAGIC * `spark.read.csv("path")`
// MAGIC * `spark.read.format("csv").load("path")`
// MAGIC
// MAGIC #### Solution la plus simple: remarquez que l' entête n'est pas reconnu mais il est traité comme tout autre ligne

// COMMAND ----------

// Necessaire pout utiliser des fichiers
import java.io.File

val path = new File(root,"employees.csv").getPath

val df2 = spark.read.csv(path)

df2.show(5)

// COMMAND ----------

val df2 = spark.read.format("csv").load(path)

df2.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Visualisons le schéma, remarquez que ScalaSpark assume que toutes les colonnes sont des chaînes de caractères

// COMMAND ----------

df2.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC #### On peut faire mieux
// MAGIC * Dire à ScalaSpark que le premiére ligne est un entête
// MAGIC * Dire à ScalaSpark d'essayer de déduire le schéma lui même

// COMMAND ----------

val df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
df2.show(5)

df2.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Valable aussi

// COMMAND ----------

val df2 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(path)

df2.show(5)

df2.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Important: le séparateur par défaut est ','
// MAGIC * Souvent les csv français utilisent ';' car ',' est réservé aux chiffres décimaux
// MAGIC * Donc il est souvent mieux spécifier le séparateur de façon explicite

// COMMAND ----------

val df2 = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ",").csv(path)
df2.show(5)

// COMMAND ----------

val df2 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("sep", ",").load(path)
df2.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Créer DataFrame d'un fichier txt
// MAGIC
// MAGIC On peut utiliser
// MAGIC * `spark.read.text("path")`
// MAGIC * `spark.read.format("txt").load("path")` 

// COMMAND ----------

val path = new File(root,"sample.txt").getPath

val df2 = spark.read.text(path)
df2.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Aussi valable

// COMMAND ----------

val df2 = spark.read.format("text").load(path)

// COMMAND ----------

df2.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Pas trop beau: voici une visualisation meilleure

// COMMAND ----------

df2.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Créer DataFrame d'un fichier json
// MAGIC * multiLine=True nécessaire si le fichier json est composé de plusieurs lignes
// MAGIC
// MAGIC On peut utiliser
// MAGIC
// MAGIC * `spark.read.json(path)`
// MAGIC * `spark.read.format("json").load(path)`

// COMMAND ----------

val path = new File(root,"sample.json").getPath

// Aussi valable
// df2 = spark.read.format("json").load(path, multiLine=True)
    
val df2 = spark.read.option("multiLine","True").json(path)
df2.printSchema()
df2.show(10, 100)

// COMMAND ----------

// MAGIC %md
// MAGIC Création de Dataset
// MAGIC
// MAGIC Un Dataset est un Dataframe avec un typage fort.  
// MAGIC
// MAGIC Il s'agit essentiellement d'une version fortement typée d'un DataFrame, où chaque ligne du Dataset est un objet d'un type spécifique, défini par une case class ou une classe Java

// COMMAND ----------

case class Person(name: String, age: Long)

val caseClassDS = Seq(Person("Andy", 32)).toDS()
caseClassDS.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Voici comment passer d'un DataFrame à un dataset  
// MAGIC
// MAGIC Attention: il faur avoir introduit une classe Person comme ça  
// MAGIC `case class Person(name: String, age: Long)`

// COMMAND ----------

val p1 = spark.createDataFrame(List(("Andy", 32))).toDF(List("name", "age"):_*)
println(p1.getClass())

val p2 = spark.createDataFrame(List(("Andy", 32))).toDF(List("name", "age"):_*).as[Person]
println(p2.getClass())

val p3 = p1.as[Person]
println(p3.getClass())

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

val columns = List("language","users_count")
val data = List(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

val dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
dfFromData2.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC # Visualiser un DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC ### ScalaSpark DataFrame show()  
// MAGIC #### Syntaxe & Exemple

// COMMAND ----------

// MAGIC %md
// MAGIC voici le définition de show avec ses défauts  
// MAGIC
// MAGIC * `def show()`  
// MAGIC * `def show(numRows : scala.Int)`   
// MAGIC * `def show(truncate : scala.Boolean)`    
// MAGIC * `def show(numRows : scala.Int, truncate : scala.Boolean)`  
// MAGIC * `def show(numRows : scala.Int, truncate : scala.Int)`    
// MAGIC * `def show(numRows : scala.Int, truncate : scala.Int, vertical : scala.Boolean)`    
// MAGIC
// MAGIC Voici un exemple

// COMMAND ----------

// Seq et Array sont aussi valables

val columns = List("Seqno","Quote")

val data = List(
    ("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")
)

val df = spark.createDataFrame(data).toDF(columns:_*)
df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Pas beau, voici un meilleure solution

// COMMAND ----------

df.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Par défaut show() montre 20 lignes, ou moins si le DataFrame a moins de 20 lignes

// COMMAND ----------

df.show(2,false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### On peut spécifier des valeurs pour truncate

// COMMAND ----------

df.show(2,25)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Et on peut afficher le DataFrame verticalement

// COMMAND ----------

df.show(3,25,true)

// COMMAND ----------

// MAGIC %md
// MAGIC Et on peut utliser display  
// MAGIC Mais seulement avec Databricks notebooks

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC # Afficher un résumé statistique
// MAGIC
// MAGIC Avec `describe` on peut créer un autre DataFrame avec un résumé statistique
// MAGIC Pout l'affichier il faut appeler `show`

// COMMAND ----------

val data = List(
    ("James","Smith",150000.0),
    ("Michael","Rose",100000.0),
    ("Robert","Williams",800000.0),
    ("Maria","Jones",250000.0)
)

val columns = List("firstname","lastname","salary")

val df = spark.createDataFrame(data).toDF(columns:_*)
df.show(false)
df.describe().show()
df.describe()

// COMMAND ----------

df.describe().getClass

// COMMAND ----------

// MAGIC %md
// MAGIC # Sélectionner les colonnes de DataFrame
// MAGIC
// MAGIC Créons un DataFrame d'exemple

// COMMAND ----------

val data = List(
    ("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
)

val columns = List("firstname","lastname","country","state")

val df = spark.createDataFrame(data).toDF(columns:_*)
df.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Selectionner plusieurs colonnes à la fois
// MAGIC
// MAGIC #### Ces façons sont valables et donnent le même résultat

// COMMAND ----------

df.select("firstname","lastname").show()

// COMMAND ----------

df.select(df("firstname"),df("lastname")).show()

// COMMAND ----------

import org.apache.spark.sql.functions.col

df.select(col("firstname"),col("lastname")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Création d'une Temporaryview et utlisation de synataxe SQL
// MAGIC
// MAGIC Si vous connaissez la syntaxe SQL, vous pouvez créer l'imitation d'une table SQL à partir d'un DataFrame et accéder à la donnée en utilisant des expressions SQL

// COMMAND ----------

df.createOrReplaceTempView("df")
spark.sql("select firstname, lastname from df").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Sélectionner toutes les colonnes d'une liste
// MAGIC * columns est défini comme `columns = ["firstname","lastname","country","state"]`

// COMMAND ----------

df.select("*").show()

// COMMAND ----------

spark.sql("select * from df").show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Union de DataFrames

// COMMAND ----------

val data1 = List(
    Row("James","","Smith","1991-04-01","M",3000.0),
    Row("Michael","Rose","","2000-05-19","M",4000.0),
    Row("Robert","","Williams","1978-09-05","M",4000.0)
)

val data2 = List(
    Row("Maria","Anne","Jones","1967-12-01","F",4000.0),
    Row("Jen","Mary","Brown","1980-02-17","F",-1.0)
)

val schema = StructType(Array(
                  StructField("First Name", StringType,true),
                  StructField("Middle Name", StringType,true),
                  StructField("Last Name", StringType,true),
                  StructField("Date of Birth", StringType,true),
                  StructField("Gender", StringType,true),
                  StructField("Salary", DoubleType,true)
             ))

val df1 = spark.createDataFrame(data1.asJava, schema)
display(df1)

// COMMAND ----------

val df2 = spark.createDataFrame(data2.asJava, schema)
display(df2)

// COMMAND ----------

val df3 = df1.union(df2)
display(df3)

// COMMAND ----------

// MAGIC %md
// MAGIC # Utilisation de ScalaSpark withColumn() avec des exemples
// MAGIC
// MAGIC #### withColumn() est une commande puissante, que il faut connaitre
// MAGIC #### Elle vous permet de modifier des colonnes 
// MAGIC
// MAGIC Créons un DataFrame

// COMMAND ----------

// Deprecated since 2.13 
// import scala.collection.JavaConverters._

/*
Since Scala 2.13 one must do

import scala.jdk.CollectionConverters._
and specify data.asJava
to not have a deprecation warning

See
https://stackoverflow.com/questions/62081253/upgrade-project-to-scala-2-13-javaconversions-is-not-a-member-of-package-collec

and
https://www.scala-lang.org/api/2.13.2/scala/jdk/CollectionConverters$.html


*/

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{StringType, DoubleType}

import org.apache.spark.sql.Row

val data = Seq(
    Row("James","","Smith","1991-04-01","M",3000.0),
    Row("Michael","Rose","","2000-05-19","M",4000.0),
    Row("Robert","","Williams","1978-09-05","M",4000.0),
    Row("Maria","Anne","Jones","1967-12-01","F",4000.0),
    Row("Jen","Mary","Brown","1980-02-17","F",-1.0)
)

val schema = StructType(Array(
                  StructField("First Name", StringType,true),
                  StructField("Middle Name", StringType,true),
                  StructField("Last Name", StringType,true),
                  StructField("Date of Birth", StringType,true),
                  StructField("Gender", StringType,true),
                  StructField("Salary", DoubleType,true)
             ))

val df = spark.createDataFrame(data.asJava, schema)

df.printSchema()

df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Change DataType using withColumn()

// COMMAND ----------

df.withColumn("salary",col("salary").cast("Integer")).show()

df.withColumn("salary",col("salary").cast("Integer")).printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Modifier le type de données à l'aide de withColumn()

// COMMAND ----------

df.withColumn("salary",col("salary")*100).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Créer une colonne à partir d'une colonne existante

// COMMAND ----------

df.withColumn("CopiedColumn",col("salary") * -1).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ajouter une nouvelle colonne à l'aide de withColumn()
// MAGIC
// MAGIC * La fonction lit() est utilisée pour ajouter une valeur constante ou littérale en tant que nouvelle colonne au DataFrame

// COMMAND ----------

import org.apache.spark.sql.functions.lit

df.withColumn("Country", lit("USA")).show()

// COMMAND ----------

df.withColumn("Country", lit("USA")).withColumn("anotherColumn",lit("anotherValue")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Renommer une colonne

// COMMAND ----------

df.withColumnRenamed("gender","sex").show(truncate=false) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Supprimer une colonne du DataFrame

// COMMAND ----------

df.drop("salary").show() 

// COMMAND ----------

// MAGIC %md
// MAGIC # Fonctions filter() et where()
// MAGIC
// MAGIC La fonction ScalaSpark filter() est utilisée pour filtrer les lignes de RDD/DataFrame en fonction de la condition ou de l'expression SQL donnée, vous pouvez également utiliser la clause where() au lieu du filter() si vous venez d'un background SQL, ces deux fonctions fonctionnent exactement de la même façon

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ArrayType}

val data = Array(
    Row(Row("James","","Smith"), Array("Java","Scala","C++"),"OH","M"),
    Row(Row("Anna","Rose",""), Array("Spark","Java","C++"),"NY","F"),
    Row(Row("Julia","","Williams"), Array("CSharp","VB"),"OH","F"),
    Row(Row("Maria","Anne","Jones"), Array("CSharp","VB"),"NY","M"),
    Row(Row("Jen","Mary","Brown"), Array("CSharp","VB"),"NY","M"),
    Row(Row("Mike","Mary","Williams"), Array("Python","VB"),"OH","M")
 )
        
val schema = StructType(Array(
    StructField("name",
                StructType(Array(
                    StructField("firstname", StringType, true),
                    StructField("middlename", StringType, true),
                    StructField("lastname", StringType, true)
                ))),
     StructField("languages", ArrayType(StringType), true),
     StructField("state", StringType, true),
     StructField("gender", StringType, true)
))

val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
df.printSchema()
df.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Selection sur une colonne

// COMMAND ----------

// MAGIC %md
// MAGIC #### Using equals condition
// MAGIC #### Use “===” for comparison

// COMMAND ----------

df.filter(df("state") === "OH").show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Même résultat avec where
// MAGIC #### Use “===” for comparison

// COMMAND ----------

df.where(df("state") === "OH").show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### not equals condition
// MAGIC #### =!= for expressing the difference

// COMMAND ----------

df.filter(df("state") =!= "OH").show(truncate=false)

// COMMAND ----------

df.filter(! (df("state") === "OH")).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Using SQL col() function
// MAGIC #### Use “===” for comparison

// COMMAND ----------

import org.apache.spark.sql.functions.col
df.filter(col("state") === "OH").show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC Utilisation de syntaxe SQL

// COMMAND ----------

df.createOrReplaceTempView("df")
spark.sql("select * from df where state = 'OH'").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## DataFrame filter() avec expressions SQL

// COMMAND ----------

df.filter("gender == 'M'").show()

// COMMAND ----------

df.filter("gender = 'M'").show()

// COMMAND ----------

df.filter("gender != 'M'").show()

// COMMAND ----------

df.filter("gender <> 'M'").show()

// COMMAND ----------

spark.sql("select * from df where gender = 'M'").show()

// COMMAND ----------

spark.sql("select * from df where gender == 'M'").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## filter avec plusieurs conditions
// MAGIC #### Use “===” for comparison
// MAGIC #### where et filter sont valables

// COMMAND ----------

df.filter(df("state")  === "OH" && df("gender")  === "M").show(truncate=false)  

// COMMAND ----------

df.where(df("state")  === "OH" && df("gender")  === "M").show(truncate=false)

// COMMAND ----------

spark.sql("select * from df where state = 'OH' and gender = 'M'").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Filtrer en fonction des valeurs de liste

// COMMAND ----------

// MAGIC %md
// MAGIC #### Créons une liste

// COMMAND ----------

val li=Seq("OH","CA","DE")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Filter IS IN List values

// COMMAND ----------

df.filter(df("state").isin(li:_*)).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Filter NOT IS IN List values

// COMMAND ----------

df.filter(! df("state").isin(li:_*)).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Filter() basé sur Starts With, Ends With, Contains

// COMMAND ----------

df.filter(df("state").startsWith("N")).show()

df.where(df("state").startsWith("N")).show()

// COMMAND ----------

df.filter(df("state").endsWith("H")).show()

df.where(df("state").endsWith("H")).show()

// COMMAND ----------

df.filter(df("state").contains("H")).show()

df.where(df("state").contains("H")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## filter() like and rlike
// MAGIC * Vous pouvez utliser unse syntaxe comme `like` de SQL
// MAGIC * Regex sont supportées

// COMMAND ----------

val data2 = List(
    (1,"Michael Rose"),
    (3,"Robert Williams"),
    (4,"Rames Rose"),
    (5,"Rames rose")
)

val columns = Seq("id","name")

val df2 = spark.createDataFrame(data2).toDF(columns:_*)

// COMMAND ----------

// MAGIC %md
// MAGIC #### like - SQL LIKE pattern

// COMMAND ----------

df2.filter(df2("name").like("%rose%")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### rlike - SQL RLIKE pattern (LIKE with Regex), case insensitive

// COMMAND ----------

df2.filter(df2("name").rlike("(?i)^*rose$")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Supprimer les lignes en double

// COMMAND ----------

import org.apache.spark.sql.functions.expr

val data = List(
    ("James", "Sales", 3000),
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Saif", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000)
  )
val columns= Seq("employee_name", "department", "salary")
val df = spark.createDataFrame(data).toDF(columns:_*)
df.printSchema()
df.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Obtenir des lignes distinctes (en comparant toutes les colonnes)
// MAGIC
// MAGIC Sur le DataFrame ci-dessus, nous avons un total de 10 lignes avec 2 lignes ayant toutes les valeurs dupliquées, une exécution distincte sur ce DataFrame devrait nous donner 9 après avoir supprimé 1 ligne en double.

// COMMAND ----------

val distinctDF = df.distinct()
println("Distinct count: " + distinctDF.count())
distinctDF.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Alternativement, vous pouvez également exécuter la fonction dropDuplicates() qui renvoie un nouveau DataFrame après avoir supprimé les lignes en double.

// COMMAND ----------

val df2 = df.dropDuplicates()
print("Distinct count: " + df2.count())
df2.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Eliminer les doublons sur des champs particuliers

// COMMAND ----------

val dropDisDF = df.dropDuplicates("department","salary")

print("Distinct count of department & salary : " + dropDisDF.count())
dropDisDF.show(truncate=false)

val dropDisDF2 = df.dropDuplicates(List("department","salary"))
print("Distinct count of department & salary : " + dropDisDF2.count())
dropDisDF2.show(truncate=false)

val dropDisDF3 = df.dropDuplicates(Seq("department","salary"))
print("Distinct count of department & salary : " + dropDisDF3.count())
dropDisDF2.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC # orderBy() and sort()

// COMMAND ----------

val simpleData = List(
    ("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
val columns= Seq("employee_name","department","state","salary","age","bonus")
val df = spark.createDataFrame(data = simpleData).toDF(columns:_*)
df.printSchema()
df.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tri DataFrame à l'aide de la fonction sort()
// MAGIC Voici la signature de la fonction `sort(self, *cols, **kwargs):`  
// MAGIC Par défaut, elle ordonne par ordre croissant.

// COMMAND ----------

df.sort("department","state").show(truncate=false)

df.sort(col("department"),col("state")).show(truncate=false)

df.sort(df("department"),df("state")).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tri DataFrame à l'aide de la fonction orderBy()
// MAGIC
// MAGIC ScalaSpark DataFrame fournit également la fonction orderBy() pour trier sur une ou plusieurs colonnes  
// MAGIC Par défaut, elle ordonne par ordre croissant.

// COMMAND ----------

df.orderBy("department","state").show(truncate=false)

df.orderBy(col("department"),col("state")).show(truncate=false)

df.orderBy(df("department"),df("state")).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Trier par ordre croissant (ASC)
// MAGIC
// MAGIC Si vous souhaitez spécifier explicitement l'ordre/le tri croissant sur DataFrame, vous pouvez utiliser la méthode asc de la fonction Column  

// COMMAND ----------

df.sort(df("department").asc,df("state").asc).show(truncate=false)

df.sort(col("department").asc,col("state").asc).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Trier par ordre décroissant (DESC)
// MAGIC
// MAGIC Si vous souhaitez spécifier le tri par ordre décroissant sur DataFrame, vous pouvez utiliser la méthode desc de la fonction Column  
// MAGIC Dans notre exemple, utilisons desc sur la colonne d'état.

// COMMAND ----------

df.sort(df("department").asc,df("state").desc).show(truncate=false)

df.sort(col("department").asc,col("state").desc).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Utilisation de SQL brut

// COMMAND ----------

df.createOrReplaceTempView("EMP")
spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC # Groupby
// MAGIC
// MAGIC Lorsque nous exécutons groupBy () sur ScalaSpark Dataframe, il renvoie l'objet GroupedData qui contient les fonctions d'agrégation ci-dessous.
// MAGIC
// MAGIC * count() - Renvoie le nombre de lignes pour chaque groupe
// MAGIC * mean() - Renvoie la moyenne des valeurs pour chaque groupe
// MAGIC * max() - Renvoie le maximum de valeurs pour chaque groupe
// MAGIC * min() - Renvoie le minimum de valeurs pour chaque groupe
// MAGIC * sum() - Renvoie le total des valeurs de chaque groupe
// MAGIC * avg() - Renvoie la moyenne des valeurs de chaque groupe
// MAGIC * agg() - En utilisant la fonction agg(), nous pouvons calculer plus d'un agrégat à la fois

// COMMAND ----------

val simpleData = List(
    ("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )

val schema = Seq("employee_name","department","state","salary","age","bonus")
val df = spark.createDataFrame(simpleData).toDF(schema:_*)
df.printSchema()
df.show(truncate=false)

// COMMAND ----------

df.groupBy("department").sum("salary").show(truncate=false)

// COMMAND ----------

df.groupBy("department").count().show(truncate=false)

// COMMAND ----------

df.groupBy("department").min("salary").show(truncate=false)

// COMMAND ----------

df.groupBy("department").max("salary").show(truncate=false)

// COMMAND ----------

df.groupBy("department").avg("salary").show(truncate=false)

df.groupBy("department").mean("salary").show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## groupBy et agrégation sur plusieurs colonnes

// COMMAND ----------

df.groupBy("department","state").sum("salary","bonus").show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exécuter plus d'agrégats à la fois
// MAGIC
// MAGIC En utilisant la fonction d'agrégation agg(), nous pouvons calculer plusieurs agrégations à la fois sur une seule instruction à l'aide des fonctions d'agrégation SQL ScalaSpark sum(), avg(), min(), max() mean() e.t.c.  
// MAGIC Pour les utiliser, nous devons importer  
// MAGIC import org.apache.spark.sql.functions.{sum,avg,max,min,mean,count}

// COMMAND ----------

import org.apache.spark.sql.functions.{sum,avg,max,min,mean,count}

// COMMAND ----------

df.groupBy("department")
    .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus")
     ).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Utilisation de filter sur les données agrégées
// MAGIC
// MAGIC Semblable à la clause SQL "HAVING", sur ScalaSpark DataFrame, nous pouvons utiliser la fonction where() ou filter() pour filtrer les lignes de données agrégées.

// COMMAND ----------

df.groupBy("department")
    .agg(sum("salary").as("sum_salary"),
      avg("salary").as("avg_salary"),
      sum("bonus").as("sum_bonus"),
      max("bonus").as("max_bonus"))
    .where(col("sum_bonus") >= 50000)
    .show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC # ScalaSpark Jointure de deux DataFrames
// MAGIC
// MAGIC ScalaSpark Join est utilisé pour combiner deux DataFrames et en les enchaînant, vous pouvez joindre plusieurs DataFrames  
// MAGIC Il prend en charge toutes les opérations de type jointure de base disponibles dans le SQL traditionnel comme
// MAGIC * INNER
// MAGIC * LEFT OUTER
// MAGIC * RIGHT OUTER
// MAGIC * LEFT ANTI
// MAGIC * LEFT SEMI
// MAGIC * CROSS
// MAGIC * SELF JOIN
// MAGIC
// MAGIC #### Synatxe
// MAGIC
// MAGIC `join(self, other, on=None, how=None)`
// MAGIC
// MAGIC 1: param other: Right side of the join  
// MAGIC 2: param on: a string for the join column name  
// MAGIC 3: param how: défault inner, un de:  
// MAGIC * inner
// MAGIC * cross
// MAGIC * outer
// MAGIC * full
// MAGIC * full_outer
// MAGIC * left
// MAGIC * left_outer
// MAGIC * right
// MAGIC * right_outer
// MAGIC * left_semi
// MAGIC * left_anti

// COMMAND ----------

val emp = List(
    (1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
    (6,"Brown",2,"2010","50","",-1)
)
val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
       "emp_dept_id","gender","salary")

val empDF = spark.createDataFrame(emp).toDF(empColumns:_*)
empDF.printSchema()
empDF.show(truncate=false)

val dept = List(
    ("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
)

val deptColumns = Seq("dept_name","dept_id")
val deptDF = spark.createDataFrame(dept).toDF(deptColumns:_*)
deptDF.printSchema()
deptDF.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ScalaSpark Inner Join DataFrame
// MAGIC ### Remarquez : l'égalité est : `===`, pas `==` ou `=`

// COMMAND ----------

empDF.join(
    deptDF,
    empDF("emp_dept_id") ===  deptDF("dept_id"),
    "inner"
).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ScalaSpark Full Outer Join
// MAGIC #### Les trois formes suivantes sont équivalentes

// COMMAND ----------

empDF.join(
    deptDF,
    empDF("emp_dept_id") ===  deptDF("dept_id"),
    "outer"
).show(truncate=false)

// COMMAND ----------

empDF.join(
    deptDF,
    empDF("emp_dept_id") ===  deptDF("dept_id"),
    "full"
).show(truncate=false)

// COMMAND ----------

empDF.join(
    deptDF,
    empDF("emp_dept_id") ===  deptDF("dept_id"),
    "fullouter"
).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ScalaSpark Left Outer Join

// COMMAND ----------

empDF.join(
    deptDF,
    empDF("emp_dept_id") ===  deptDF("dept_id"),
    "left"
).show(truncate=false)

// COMMAND ----------

empDF.join(
    deptDF,
    empDF("emp_dept_id") ===  deptDF("dept_id"),
    "leftouter"
).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Right Outer Join

// COMMAND ----------

empDF.join(
    deptDF,
    empDF("emp_dept_id") ===  deptDF("dept_id"),
    "right"
).show(truncate=false)

// COMMAND ----------

empDF.join(
    deptDF,
    empDF("emp_dept_id") ===  deptDF("dept_id"),
    "rightouter"
).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Utilisation d'expressions SQL
// MAGIC
// MAGIC Étant donné que ScalaSpark SQL prend en charge la syntaxe SQL native, nous pouvons également écrire des opérations de jointure après avoir créé des tables temporaires sur DataFrames et utiliser ces tables sur spark.sql().

// COMMAND ----------

empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")

spark.sql(
    "select * from EMP e, DEPT d where e.emp_dept_id = d.dept_id"
).show(truncate=false)

// COMMAND ----------

spark.sql(
    "select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id = d.dept_id"
).show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC # ScalaSpark fillna() & fill()
// MAGIC
// MAGIC Dans ScalaSpark, DataFrame.fillna() ou DataFrameNaFunctions.fill() est utilisé pour remplacer les valeurs NULL/None sur toutes ou plusieurs colonnes DataFrame sélectionnées par zéro (0), une chaîne vide, un espace ou toute valeur littérale constante

// COMMAND ----------

val emp = List(
    (1,"Smith",-1,3,"10","M",4000.0,Double.NaN),
    (2,"Williams",1,2010,"10","M",Double.NaN,175.0),
    (3,"Jones",2,2005,"10","F",2000.0,161.0),
    (4,"Brown",2,2010,"40","",-1.0,174.0),
    (5,"Brown",2,2010,"50","",-1.0,183.0)
)
val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
       "emp_dept_id","gender","salary","height")

val df = spark.createDataFrame(emp).toDF(empColumns:_*)

df.printSchema()
df.show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Remplacer NULL/None Values with Zero (0)

// COMMAND ----------

// Replace 0 for null for all integer columns
df.na.fill(value=0).show()

df.na.fill(value=0,Array("salary")).show()

df.na.fill(value=0,Array("height")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Remarque: `None` et `NaN` ne sont pas la même chose, mais ils peuvent representer le manque d'une valeur si la colonne est de type numérique

// COMMAND ----------

// MAGIC %md
// MAGIC ## Trouver les valeurs nulles, non nulles

// COMMAND ----------

val emp = List(
    (1,null,-1,3,"10","M",4000.0,Double.NaN),
    (2,"Williams",1,2010,"10","M",Double.NaN,175.0),
    (3,"Jones",2,2005,"10","F",2000.0,161.0),
    (4,"Brown",2,2010,"40","",-1.0,174.0),
    (5,"Brown",2,2010,"50","",-1.0,183.0)
)
val empColumns = List("emp_id","name","superior_emp_id","year_joined",
       "emp_dept_id","gender","salary","height")

val df = spark.createDataFrame(emp).toDF(empColumns:_*)

df.printSchema()
df.show(truncate=false)

// COMMAND ----------

df.filter(df("name").isNull).show()

df.filter(df("salary").isNaN).show()

// COMMAND ----------

df.filter(df("name").isNotNull).show()

df.filter(df("name").isNotNull && ! df("salary").isNaN).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Plus d'exemples de valeurs nulles / non nulles

// COMMAND ----------

// Create DataFrame
val data = List(
    ("James",null,"M"),
    ("Anna","NY","F"),
    ("Julia",null,null)
  )

val columns = Seq("name","state","gender")
val df = spark.createDataFrame(data).toDF(columns:_*)
df.show()

df.filter("state is NULL").show()
df.filter(df("state").isNull).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Avec 'col()'

// COMMAND ----------

import org.apache.spark.sql.functions.col
df.filter(col("state").isNull).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Plusieurs champs

// COMMAND ----------

df.filter("state IS NULL AND gender IS NULL").show()
df.filter(df("state").isNull && df("gender").isNull).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Avec la fonction isnull()

// COMMAND ----------

import org.apache.spark.sql.functions.isnull
df.select(isnull(df("state"))).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Avec isNotNull()

// COMMAND ----------

df.filter("state IS NOT NULL").show()

df.filter("NOT state IS NULL").show()

df.filter(df("state").isNotNull).show()

df.filter(col("state").isNotNull).show()

// COMMAND ----------

// MAGIC %md
// MAGIC avec Spark SQL

// COMMAND ----------

df.createOrReplaceTempView("DATA")
spark.sql("SELECT * FROM DATA where STATE IS NULL").show()

// COMMAND ----------

spark.sql("SELECT * FROM DATA where STATE IS NULL AND GENDER IS NULL").show()

// COMMAND ----------

spark.sql("SELECT * FROM DATA where STATE IS NOT NULL").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Imputation des valeurs nulles

// COMMAND ----------

// Utilisons Seq pour changer
val emp = Seq(
    (1,null,-1,3,10,"M",4000.0,Double.NaN),
    (2,"Williams",1,2010,10,"M",Double.NaN,175.0),
    (3,"Jones",2,2005,10,"F",2000.0,161.0),
    (4,"Brown",2,2010,40,"",-1.0,174.0),
    (5,"Brown",2,2010,50,"",-1.0,183.0)
)
val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
       "emp_dept_id","gender","salary","height")

val df = spark.createDataFrame(emp).toDF(empColumns:_*)

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Avec la moyenne

// COMMAND ----------

import org.apache.spark.ml.feature.Imputer

val imputer = new Imputer()
  .setInputCols(df.select("salary","height").columns)
  .setOutputCols(df.select("salary","height").columns.map(c => s"${c}_imputed"))
  .setStrategy("mean")

val imp_df = imputer.fit(df).transform(df)

display(imp_df)

// COMMAND ----------

// MAGIC %md
// MAGIC Avec la médiane

// COMMAND ----------

val imputer = new Imputer()
  .setInputCols(df.select("salary","height").columns)
  .setOutputCols(df.select("salary","height").columns.map(c => s"${c}_imputed"))
  .setStrategy("median")

val imp_df = imputer.fit(df).transform(df)

display(imp_df)

// COMMAND ----------

// MAGIC %md
// MAGIC # Sauvegarde des DataFrames

// COMMAND ----------

import java.io.File

val in_path = new File(root, "walmart_stock.csv").getPath

val out_path = new File(root, "save_walmart.csv").getPath

val df = spark.read.option("header", true).option("inferSchema", true).csv(in_path)

df.write.mode("overwrite").option("header", true).csv(out_path)

// df.write.option("header", true).csv(out_path)

// Ceci est el défaut
//df.write.mode("error").option("header", true).csv(out_path)

// COMMAND ----------

import java.io.File

val in_path = new File(root, "walmart_stock.csv").getPath

val out_path = new File(root, "save_walmart.csv").getPath

val df = spark.read.option("header", true).option("inferSchema", true).format("csv").load(in_path)

df.write.mode("overwrite").option("header", true).format("csv").save(out_path)

// COMMAND ----------

// MAGIC %md
// MAGIC Choses à noter:
// MAGIC
// MAGIC * Le DataFrame est coupé en fichiers csv et sauvegardé sur un répertoire
// MAGIC * Le nom est le nom du répertoire; les noms des fichiers csv sont assignés par Spark
// MAGIC * Si on veut supprimer l'ancien répertoire il faut mettre `mode('overwrite')`
// MAGIC * Par défaut, si on essaie de réecrire un répertoire ScalaSpark donne une erreur
// MAGIC
// MAGIC Le possibilités sont
// MAGIC
// MAGIC * `overwrite`: remplace l'ancien DataFrame
// MAGIC
// MAGIC * `append`: ajoute le DataFrame 
// MAGIC
// MAGIC * `ignore`: ne fait rien
// MAGIC
// MAGIC * `error`: le défaut

// COMMAND ----------

// MAGIC %md
// MAGIC # Lecture et écriture de fichiers parquet
// MAGIC
// MAGIC Les modes d'écriture sont le mêmes

// COMMAND ----------

val data = List(
    ("James ","","Smith","36636","M",3000),
    ("Michael ","Rose","","40288","M",4000),
    ("Robert ","","Williams","42114","M",4000),
    ("Maria ","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1))
val columns=Seq("firstname","middlename","lastname","dob","gender","salary")
val df=spark.createDataFrame(data).toDF(columns:_*)

// COMMAND ----------

import java.io.File

val path = new File(root, "people.parquet").getPath

df.write.mode("overwrite").parquet(path)

df.show()

val parDF=spark.read.parquet(path)

parDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Lecture et écriture de fichiers ORC

// COMMAND ----------

val data = List(
    ("James ","","Smith","36636","M",3000),
    ("Michael ","Rose","","40288","M",4000),
    ("Robert ","","Williams","42114","M",4000),
    ("Maria ","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1))
val columns=Seq("firstname","middlename","lastname","dob","gender","salary")
val df=spark.createDataFrame(data).toDF(columns:_*)

// COMMAND ----------

import java.io.File

val path = new File(root, "people.orc").getPath

df.write.mode("overwrite").orc(path)

df.show()

val parDF=spark.read.orc(path)

parDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Lecture et écriture de fichiers XML
// MAGIC ### Il faut créer une bibliothèque Databricks  
// MAGIC https://docs.databricks.com/en/external-data/xml.html#requirements  
// MAGIC

// COMMAND ----------

/*
import java.io.File

import com.databricks.spark.xml

val path = new File(root, "people.xml").getPath

df.write.format("com.databricks.spark.xml").mode("overwrite").save(path)
df.show()

val parDF=spark.read.format("com.databricks.spark.xml").load(path)
parDF.show()
*/

// COMMAND ----------

// MAGIC %md
// MAGIC # Lecture et écriture de fichiers Avro
// MAGIC Attention: il faut faire comme ça  
// MAGIC De la doc officielle:   
// MAGIC #_____________________________________________________________________________________________________________________________  
// MAGIC Since spark-avro module is external, there is no .avro API in DataFrameReader or DataFrameWriter.
// MAGIC
// MAGIC To load/save data in Avro format, you need to specify the data source option format as avro(or org.apache.spark.sql.avro).
// MAGIC #_____________________________________________________________________________________________________________________________  

// COMMAND ----------

import java.io.File

val path = new File(root, "people.avro").getPath

df.write.format("avro").mode("overwrite").save(path)
df.show()

val parDF=spark.read.format("avro").load(path)
parDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Lecture et écriture de fichiers JSON

// COMMAND ----------

import java.io.File

val path = new File(root, "people.json").getPath

df.write.format("json").mode("overwrite").save(path)
df.show()

val parDF=spark.read.format("json").load(path)
parDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Attention
// MAGIC Ceci vous montre que le nom des fichiers n' aucun interêt pour vous  
// MAGIC Sauvegardez les fichiers dans un format de votre choix, lisez-les avec Spark et ne les touchez pas autrement  

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/save_walmart.csv

// COMMAND ----------

// MAGIC %md
// MAGIC # Comment voire les contenu des dossier  
// MAGIC
// MAGIC Exemple: ceci est un large fichier csv  

// COMMAND ----------

val path = root + "/creditcard"

display(dbutils.fs.ls(path))

// COMMAND ----------

// MAGIC %md
// MAGIC Sauveardons-le comme ORC  
// MAGIC Il va être comprimé  

// COMMAND ----------

val df = spark.read.format("csv").load(path)

val outpath = root + "/creditcard.orc"

df.write.mode("overwrite").save(outpath)

// COMMAND ----------

display(dbutils.fs.ls(outpath))

// COMMAND ----------

// MAGIC %md
// MAGIC Affichons la somme des fichiers ORC dans lesquels le dataframe a été découpé  

// COMMAND ----------

// https://stackoverflow.com/questions/69726543/spark-use-dbutils-fs-ls-todf-in-jar-file

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

val ddlSchema = StructType(Array(
  StructField("path",StringType,true),
  StructField("name",StringType,true),
  StructField("size",IntegerType,true)
))

var file_details = dbutils.fs.ls(outpath)
var fileData = file_details.map(x => (x.path, x.name, x.size.toString))
var rdd = sc.parallelize(fileData)
val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2, attributes._3.toInt))

val fi = spark.createDataFrame(rowRDD, ddlSchema)

display(fi.select("size").agg(sum("size")))

// COMMAND ----------

// MAGIC %md
// MAGIC Le dataframe a été comprimé par rapport au csv, affichons le rapport de compression  

// COMMAND ----------

121364862.0 / 150828752

// COMMAND ----------

// MAGIC %md
// MAGIC Les fichiers ORC occupent 80% de l'espace du csv  
// MAGIC Donc pour grosses volumes de deonnées est mieux choisir ORC ou Parquet
