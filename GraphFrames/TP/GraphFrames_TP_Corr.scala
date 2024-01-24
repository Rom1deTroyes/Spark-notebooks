// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC Créer un graphe avec ces info  
// MAGIC   
// MAGIC vertices   
// MAGIC            ('1', 'Carter', 'Derrick', 50),   
// MAGIC            ('2', 'May', 'Derrick', 26),  
// MAGIC            ('3', 'Mills', 'Jeff', 80),  
// MAGIC            ('4', 'Hood', 'Robert', 65),  
// MAGIC            ('5', 'Banks', 'Mike', 93),  
// MAGIC            ('98', 'Berg', 'Tim', 28),
// MAGIC            ('99', 'Page', 'Allan', 16),  
// MAGIC names  
// MAGIC ['id', 'name', 'firstname', 'age']  
// MAGIC   
// MAGIC edges  
// MAGIC        ('1', '2', 'friend'),  
// MAGIC        ('2', '1', 'friend'),  
// MAGIC        ('3', '1', 'friend'),  
// MAGIC        ('1', '3', 'friend'),  
// MAGIC        ('2', '3', 'follows'),  
// MAGIC        ('3', '4', 'friend'),  
// MAGIC        ('4', '3', 'friend'),  
// MAGIC        ('5', '3', 'friend'),  
// MAGIC        ('3', '5', 'friend'),  
// MAGIC        ('4', '5', 'follows'),  
// MAGIC        ('98', '99', 'friend'),  
// MAGIC        ('99', '98', 'friend'),  
// MAGIC names  
// MAGIC ['src', 'dst', 'type']  

// COMMAND ----------

import org.apache.spark._
import org.graphframes._

import org.apache.spark.sql.SparkSession

// COMMAND ----------

val spark = SparkSession.builder.appName("graphFramesTP").getOrCreate()

// COMMAND ----------

val vData = List(("1", "Carter", "Derrick", 50), 
                 ("2", "May", "Derrick", 26),
                 ("3", "Mills", "Jeff", 80),
                 ("4", "Hood", "Robert", 65),
                 ("5", "Banks", "Mike", 93),
                 ("98", "Berg", "Tim", 28),
                 ("99", "Page", "Allan", 16))

val vColumns = Seq("id", "name", "firstname", "age")

val vertices = spark.createDataFrame(vData).toDF(vColumns:_*)

// COMMAND ----------

val eData = List(("1", "2", "friend"), 
                 ("2", "1", "friend"),
                 ("3", "1", "friend"),
                 ("1", "3", "friend"),
                 ("2", "3", "follows"),
                 ("3", "4", "friend"),
                 ("4", "3", "friend"),
                 ("5", "3", "friend"),
                 ("3", "5", "friend"),
                 ("4", "5", "follows"),
                 ("98", "99", "friend"),
                 ("99", "98", "friend"))

val eColumns = Seq("src", "dst", "type")

val edges = spark.createDataFrame(eData).toDF(eColumns:_*)

// COMMAND ----------

val g = GraphFrame(vertices, edges)

// COMMAND ----------

// MAGIC %md
// MAGIC Afficher sommets, arêtes et degré dans le graphe

// COMMAND ----------

g.vertices.show()

// COMMAND ----------

g.edges.show()

// COMMAND ----------

g.degrees.show()

// COMMAND ----------

// MAGIC %md
// MAGIC trouver tous les edges de 'follows' et touse les edges de 'friend'

// COMMAND ----------

val numFollows = g.edges.filter("type = 'follows'").count()
numFollows

// COMMAND ----------

val numFollows = g.edges.filter("type = 'friend'").count()
numFollows

// COMMAND ----------

// MAGIC %md
// MAGIC Trouver les motifs de la forme '(a)-[e1]->(b); (b)-[e2]->(c)'  
// MAGIC Combien il y en a ?

// COMMAND ----------

val motifs = g.find("(a)-[e1]->(b); (b)-[e2]->(c)")
motifs.show()
motifs.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Sauvegardez le graphe en format csv, lisez le graphe sauvegardé et assurez vous qu'ils sont pareils

// COMMAND ----------

// Save vertices and edges as Parquet to some location.
g.vertices.write.mode("overwrite").parquet("/FileStore/v.parquet")
g.edges.write.mode("overwrite").parquet("/FileStore/e.parquet")

// Load the vertices and edges back.
val sameV = spark.read.parquet("/FileStore/v.parquet")
val sameE = spark.read.parquet("/FileStore/e.parquet")

// Create an identical GraphFrame.
val sameG = GraphFrame(sameV, sameE)
sameG.vertices.orderBy("id").show()

// COMMAND ----------

g.vertices.orderBy("id").show()
