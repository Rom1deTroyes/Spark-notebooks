// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # Spark GraphFrames
// MAGIC
// MAGIC GraphFrames est l’API d’Apache Spark pour les graphes et le calcul parallèle.  
// MAGIC
// MAGIC GraphFrames unifie les processus ETL (Extract, Transform & Load), l'analyse exploratoire et le calcul de graphes itératif
// MAGIC au sein d'un même système.  
// MAGIC L’utilisation des graphiques est visible dans les amis de Facebook, les connexions LinkedIn, les routeurs Internet,
// MAGIC les relations entre les galaxies et les étoiles en astrophysique et dans les cartes de Google.  
// MAGIC
// MAGIC Même si le concept de calcul de graphe semble très simple, les applications de graphes sont littéralement illimitées
// MAGIC
// MAGIC # To use GraphFrames install graphframes-0.8.1-spark3.0-s_2.12.jar before attaching the cluster to the notebook

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC GraphFrames comprend une collection croissante d'algorithmes de graphes et de générateurs pour simplifier les tâches d'analyse de graphes.  
// MAGIC
// MAGIC GraphFrames étend le DataFrame Spark avec un graphe de propriétés distribuées résilient.  
// MAGIC
// MAGIC Le graphe de propriétés est un multigraphe dirigé qui peut avoir plusieurs arêtes en parallèle.  
// MAGIC
// MAGIC Chaque arête et chaque sommet sont associés à des propriétés définies par l'utilisateur.  
// MAGIC
// MAGIC Les arêtes parallèles permettent plusieurs relations entre les mêmes sommets.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC On appelle graphe tout ensemble de points, dont certaines paires
// MAGIC sont directement reliées par un (ou plusieurs) liens(s).  
// MAGIC
// MAGIC * Un graphe sert à manipuler des concepts, et à établir un lien entre ces concepts.
// MAGIC * Un problème comportant des objets avec des relations entre ces objets peut être modéliser par un graphe.
// MAGIC * Les graphes sont des outils très puissants et largement répandus qui se prêtent bien à la résolution de nombreux problèmes.

// COMMAND ----------

// MAGIC %md
// MAGIC Use cases graphes
// MAGIC * Réseaux sociaux: les sommets sont des individus, les arêtes sont les relations entre les individus
// MAGIC * Ordonnancement de tâches
// MAGIC * Minimisation de l'usage de ressources avec les arbres couvrants minimaux
// MAGIC * Recherche de plus court chemin dans un graphe

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Utiliser GraphFrames est délicat à cause des problèmes de version  
// MAGIC
// MAGIC Voici comment faire
// MAGIC
// MAGIC To use GraphFrames
// MAGIC
// MAGIC * Build a cluster with spark 3.0.1 and scala 2.12; this is the minimum that Databricks allows  
// MAGIC * Import the jar file version 0.8.1-spark3.0-s_2.12
// MAGIC * Link to the instructions below
// MAGIC * Link to include the jar in my library: below
// MAGIC * Link to importing the jar in the cluster below
// MAGIC
// MAGIC To load a jar file look here
// MAGIC https://docs.databricks.com/libraries/workspace-libraries.html
// MAGIC
// MAGIC The jar is at
// MAGIC https://mvnrepository.com/artifact/graphframes/graphframes/0.8.1-spark3.0-s_2.12

// COMMAND ----------

import org.apache.spark._
import org.graphframes._

// COMMAND ----------

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()

// COMMAND ----------

val data = List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30))

val columns = Seq("id", "name", "age")

val v = spark.createDataFrame(data).toDF(columns:_*)

// COMMAND ----------

val data = List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"))

val columns = Seq("src", "dst", "relationship")

val e = spark.createDataFrame(data).toDF(columns:_*)

// COMMAND ----------

val g = GraphFrame(v, e)

// COMMAND ----------

g.vertices.show()

// COMMAND ----------

g.edges.show()

// COMMAND ----------

g.degrees.show()

// COMMAND ----------

import org.graphframes.examples.Graphs

val g = Graphs.friends
// Get example graph

// COMMAND ----------

g.vertices.show()

// COMMAND ----------

g.edges.show()

// COMMAND ----------

// MAGIC %md
// MAGIC trouver le neoud qui représente la personne la plus jeune (hint: .groupby() sur la liste des vertices)

// COMMAND ----------

// Find the youngest user's age in the graph.
// This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()

// COMMAND ----------

// MAGIC %md
// MAGIC trouver tous les edges avec une relation de 'follow' (hint: .filter() sur la liste des edges )

// COMMAND ----------

// Count the number of "follows" in the graph.
// This queries the edge DataFrame.
val numFollows = g.edges.filter("relationship = 'follow'").count()
println(numFollows)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## Subgraphs
// MAGIC
// MAGIC Dans GraphFrames, la méthode subgraph () prend un triplet de bord (edge, src verte et dst vertex, plus attributs) et permet à l'utilisateur de sélectionner un sous-graphe en fonction des filtres de triplet et de sommet.  
// MAGIC Les GraphFrames offrent un moyen encore plus puissant de sélectionner des sous-graphes basés sur une combinaison de recherche de motif et de filtres DataFrame.  
// MAGIC Nous fournissons trois méthodes d'assistance pour la sélection de sous-graphes.  
// MAGIC filterVertices (condition), filterEdges (condition) et dropIsolatedVertices ().
// MAGIC
// MAGIC selectionner les subgraph des utilisateur avec age<30, relation de type 'friend' et supprimer les sommets qui verifient pas ces contraintes (hint: .filterVertices(), .filterEdges(), .dropIsolatedVertices())

// COMMAND ----------


//# from graphframes.examples import Graphs
val g = Graphs.friends
// Get example graph

// Select subgraph of users older than 30, and relationships of type "friend".
// Drop isolated vertices (users) which are not contained in any edges (relationships).
val g1 = g.filterVertices("age > 30").filterEdges("relationship = 'friend'").dropIsolatedVertices()

// COMMAND ----------

g1.vertices.show()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Trouver un motif
// MAGIC
// MAGIC a recherche de motif fait référence à la recherche de modèles structurels dans un graphique.
// MAGIC
// MAGIC La recherche de motif GraphFrame utilise un langage DSL (Domain-Specific Language) simple pour exprimer des requêtes structurelles.  
// MAGIC
// MAGIC Par exemple, graph.find ("(a) - [e] -> (b); (b) - [e2] -> (a)") recherchera des paires de sommets a, b reliés par des arêtes dans les deux sens.  
// MAGIC
// MAGIC Il renverra un DataFrame de toutes ces structures dans le graphique, avec des colonnes pour chacun des éléments nommés (sommets ou arêtes) du motif.  
// MAGIC
// MAGIC Dans ce cas, les colonnes renvoyées seront «a, b, e, e2»
// MAGIC
// MAGIC - L'unité de base d'un motif est un bord. Par exemple, "(a) - [e] -> (b)" exprime une arête e du sommet a au sommet b. Notez que les sommets sont désignés par des parenthèses (a), tandis que les arêtes sont indiqués par des crochets [e].
// MAGIC - Un motif est exprimé comme une union d'arêtes. Les motifs de contour peuvent être joints à des points-virgules. Motif "(a) - [e] -> (b); b) - [e2] -> (c)" spécifie deux arêtes de a à b à c.
// MAGIC
// MAGIC
// MAGIC - Dans un même motif, des noms peuvent être attribués aux vertices et aux arêtes. Par exemple, "(a) - [e] -> (b)" a trois éléments nommés: les sommets a, b et le bord e. Ces noms servent à deux fins:
// MAGIC     - Les noms peuvent identifier des éléments communs entre les arêtes. Par exemple, "(a) - [e] -> (b); (b) - [e2] -> (c)" spécifie que le même sommet b est la destination du bord e et la source du bord e2.
// MAGIC     - Les noms sont utilisés comme noms de colonne dans le résultat DataFrame. Si un motif contient le sommet nommé a, le résultat DataFrame contiendra une colonne «a» qui est un type StructType avec des sous-champs équivalents au schéma (colonnes) de GraphFrame.vertices. De même, une arête e dans un motif produira une colonne «e» dans le résultat DataFrame avec des sous-champs équivalents au schéma (colonnes) de GraphFrame.edges.
// MAGIC     - Sachez que les noms n'identifient pas des éléments distincts: deux éléments portant des noms différents peuvent faire référence au même élément graphique. Par exemple, dans le motif "(a) - [e] -> (b); (b) - [e2] -> (c)", les noms a et c pourraient faire référence au même sommet. Pour limiter les éléments nommés à des sommets ou des arêtes distincts, utilisez des filtres post-hoc tels que resultDataframe.filter ("a.id! = C.id").
// MAGIC - Il est acceptable d'omettre les noms de sommets ou d'arêtes dans les motifs lorsque cela n'est pas nécessaire. Par exemple, "(a) - [] -> (b)" exprime une arête entre les sommets a, b mais n'attribue pas de nom à l'arête. Il n'y aura pas de colonne pour le bord anonyme dans le résultat DataFrame. De même, "(a) - [e] -> ()" indique un bord extérieur du sommet a mais ne nomme pas le sommet de destination. Celles-ci sont appelées sommets et arêtes anonymes.
// MAGIC - Un bord peut être inversé pour indiquer qu'il ne doit pas être présent dans le graphique. Par exemple, "(a) - [] -> (b);! (B) - [] -> (a)" trouve les arêtes de a à b pour lesquelles il n'y a pas d'arête de b à a.

// COMMAND ----------

val g = Graphs.friends  // Get example graph

// Search for pairs of vertices with edges in both directions between them.
val motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()

// COMMAND ----------

// MAGIC %md
// MAGIC afficher les motifs qui verifient la condition "b.age > 30" (hint: .filter())

// COMMAND ----------

// More complex queries can be expressed by applying filters.
motifs.filter("b.age > 30").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Identifier ses chaînes de 4 sommets (hint: "(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

// COMMAND ----------

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.IntegerType
import org.graphframes.examples.Graphs

val g = Graphs.friends  // Get example graph

val chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")
chain4.show()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Enregistrement et chargement de GraphFrames
// MAGIC
// MAGIC Les GraphFrames étant construits autour de DataFrames, ils prennent automatiquement en charge l'enregistrement et le chargement vers et depuis le même ensemble de sources de données.  
// MAGIC Reportez-vous au Guide de l'utilisateur Spark SQL sur les sources de données pour plus de détails.

// COMMAND ----------

val g = Graphs.friends  // Get example graph

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



// COMMAND ----------

g.vertices.orderBy("id").show()
