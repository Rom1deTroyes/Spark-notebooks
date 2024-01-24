// Databricks notebook source
// MAGIC %md
// MAGIC Créez comme toujours SparkSession et SparkContext

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("tp").getOrCreate()
val sc = spark.sparkContext

// COMMAND ----------

// MAGIC %md
// MAGIC Utilisons un DataFrame d'animaux appellé pets, avec colonnes:
// MAGIC 
// MAGIC *    ID - ID number for the pet
// MAGIC *    Name - name of the pet
// MAGIC *    Animal - type of animal
// MAGIC 
// MAGIC Et un autre DataFrame, appellé owners, avec colonnes:
// MAGIC 
// MAGIC *    ID - ID number for the owner (different from the ID number for the pet)
// MAGIC *    Name - name of the owner
// MAGIC *    Pet_ID - ID number for the pet that belongs to the owner (which matches the ID number for the pet in the pets table)
// MAGIC 
// MAGIC Créez deux DataFrames pats et owners avec quelques valeurs
// MAGIC * Inserez des lignes dans les DataFrame qui n'ont pas de correspondance dans l'autre: animaux sans patron, patrons sans animaux
// MAGIC * Inserez des correspondance doubles: un patron avec plusieurs animaux
// MAGIC * Faites tout types des jointures: inner, outer, left, right, en utlisant les differents syntaxes

// COMMAND ----------

val pets = Seq(
    (1,"Fido","dog"),
    (2,"Pluto","dog"),
    (3,"Tom","cat"),
    (4,"Titi","bird"),
    (5,"Sylvester","cat"),
    (6,"Mickey","mouse"),
    (7, "Tony", "tiger")
)

val columns1= Seq("ID","Name","Animal")

val dfPets = spark.createDataFrame(pets).toDF(columns1:_*)
dfPets.printSchema()
dfPets.show(truncate=false)

// COMMAND ----------

val owners = Seq(
    (1, "Carl", 6),
    (2, "Mark", 5),
    (2, "Mark", 4),
    (3, "John", 2),
    (4, "Peter", 1),
    (5, "Paul", 3),
    (6, "Luke", 8)
)

val columns2= Seq("ID","Name","Pet_ID")

val dfOwners = spark.createDataFrame(owners).toDF(columns2:_*)
dfOwners.printSchema()
dfOwners.show(truncate=false)


// COMMAND ----------

dfPets.join(
    dfOwners,
    dfPets("ID") ===  dfOwners("Pet_ID"),
    "inner"
).show(truncate=false)

// COMMAND ----------

dfPets.join(
    dfOwners,
    dfPets("ID") ===  dfOwners("Pet_ID"),
    "left"
).show(truncate=false)

// COMMAND ----------

dfPets.join(
    dfOwners,
    dfPets("ID") ===  dfOwners("Pet_ID"),
    "leftouter"
).show(truncate=false)

// COMMAND ----------

dfPets.join(
    dfOwners,
    dfPets("ID") ===  dfOwners("Pet_ID"),
    "right"
).show(truncate=false)

// COMMAND ----------

dfPets.join(
    dfOwners,
    dfPets("ID") ===  dfOwners("Pet_ID"),
    "rightouter"
).show(truncate=false)

// COMMAND ----------

dfPets.join(
    dfOwners,
    dfPets("ID") ===  dfOwners("Pet_ID"),
    "full"
).show(truncate=false)

// COMMAND ----------

dfPets.join(
    dfOwners,
    dfPets("ID") ===  dfOwners("Pet_ID"),
    "outer"
).show(truncate=false)

// COMMAND ----------

dfPets.join(
    dfOwners,
    dfPets("ID") ===  dfOwners("Pet_ID"),
    "fullouter"
).show(truncate=false)
