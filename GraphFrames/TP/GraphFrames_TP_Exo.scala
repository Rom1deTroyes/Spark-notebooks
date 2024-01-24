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



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Afficher sommets, arêtes et degré dans le graphe

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC trouver tous les edges de 'follows' et touse les edges de 'friend'

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Trouver les motifs de la forme '(a)-[e1]->(b); (b)-[e2]->(c)'  
// MAGIC Combien il y en a ?

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC Sauvegardez le graphe en format csv, lisez le graphe sauvegardé et assurez vous qu'ils sont pareils

// COMMAND ----------



// COMMAND ----------


