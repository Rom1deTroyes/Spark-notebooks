// Databricks notebook source
// MAGIC %md
// MAGIC * Créez une liste avec valeurs seulement numériques et affichez sa longueur
// MAGIC * Créez une liste avec des valeus de type différent (numérique, string et booléen par exemple) et affichez sa longueur
// MAGIC Utlisez prinln() pour l'affichage

// COMMAND ----------

val l1 = List(1,2,3,4)
val l2 = List(10, "ceci est une liste", true)

println(l1.length)
println(l2.length)

// COMMAND ----------

// MAGIC %md
// MAGIC Faites la même chose avec un vecteur

// COMMAND ----------

val a1 = Array(1,2,3,4)
val a2 = Array(100, "ceci n'est pas est une liste", false)

println(a1.length)
println(a2.length)

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez quelque élément des listes crées à la main  
// MAGIC Essayes de dépasser les limites (par esemple: afficher le quatième élément d'une liste avec 3 éléments)

// COMMAND ----------

println(l1(0))
println(l1(1))
println(l1(2))
//println(l1(10))

println

println(l2(0))
println(l2(1))
println(l2(2))
//println(l2(4))

// COMMAND ----------

// MAGIC %md
// MAGIC Faites la même chose avec les vecteurs

// COMMAND ----------

println(a1(0))
println(a1(1))
println(a1(2))
// println(a1(6))

println

println(a2(0))
println(a2(1))
println(a2(2))
// println(a2(8))

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez les éléments des listes crées avec une boucle for

// COMMAND ----------

for (l <- l1 )
  println(l)

println

for (i <- 0 to l1.length - 1)
  println(l1(i))

println

for (l <- l2)
  println(l)

println

for (i <- 0 to l2.length - 1)
  println(l2(i))


// COMMAND ----------

// MAGIC %md
// MAGIC Créez un fonction pour afficher les valeurs d'une liste à l'aide d'une boucle for et utlisez-la pour afficher les listes crées  
// MAGIC Essaye d'utliser la même fonction pour les vecteurs; que remarquez vous ?

// COMMAND ----------

def show_list_elements(ls: List[Any]): Unit = {
  for (i <- ls)
    println(i)
}

// COMMAND ----------

show_list_elements(l1)

// COMMAND ----------

// show_list_elements(a1)

// COMMAND ----------

// MAGIC %md
// MAGIC Affichez les elements des listes et des vecteur crées à l'aide de foreach

// COMMAND ----------

l1.foreach(i => println(i))
println

l2.foreach(x => println(x))
println

a1.foreach(u => println(u))
println

a2.foreach(y => println(y))

// COMMAND ----------

// MAGIC %md
// MAGIC * Créez une liste avec des valeurs entières
// MAGIC * Obtenez une nouvelle liste qui contient les carrés de la première avec la fon ction map
// MAGIC * Affichez les deux listes
// MAGIC * Calulez et affichez la somme des valeurs de la liste avec reduce
// MAGIC * calulez et affichez la somme des carrés de la liste evec map et reduce  

// COMMAND ----------

val l3 = List(0,1,2,3)

val l4 = l3.map(i => i*i)

l3.foreach(i => println(i))
println
l4.foreach(j => println(j))
println

val r1 = l3.reduce((x,y) => x+y)
println(r1)

val r2 = l3.map(z=>z*z).reduce((x1,x2)=>x1+x2)
println(r2)
println

// COMMAND ----------

// MAGIC %md
// MAGIC Sauvegardez un fichier avec un texte de votre choix es lisez-le

// COMMAND ----------

import java.io._

val path = "sortie.txt"

val fileWriter = new FileWriter(new File(path))

fileWriter.write("Scala\n")
fileWriter.write("est\n")
fileWriter.write("un\n")
fileWriter.write("langage\n")
fileWriter.write("cool\n")


fileWriter.close()

import scala.io.Source

val bufferedSource = Source.fromFile(path)

val lines = bufferedSource.getLines.toList

bufferedSource.close()

lines.foreach(i => println(i))

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala pour calculer la somme des deux valeurs entières données. Si les deux valeurs sont identiques, renvoie le triple de leur somme.  

// COMMAND ----------

def test_somme(x:Int, y:Int) : Int =
{
  if (x == y) (x + y) * 3 else x + y
}

val s1 = test_somme(3,4)

val s2 = test_somme(5,5)

// COMMAND ----------

// MAGIC %md
// MAGIC Les questions suivantes vous demandent d'écrire des fonctions.  
// MAGIC
// MAGIC Mettez les fonctions dans un objet qui a une méthode main qui appelle la fonction plusieurs fois et appellez la méthode main  

// COMMAND ----------

// MAGIC %md
// MAGIC Écrire une fonction qui calcule la factorielle d'un nombre  

// COMMAND ----------

object FactorialCalculator {
  def factorial(n: Int): BigInt = {
    if (n == 0 || n == 1) {
      1
    } else {
      n * factorial(n - 1)
    }
  }

  def main(): Unit = {
    val number = 4
    val result = factorial(number)
    println(s"The factorial of $number is: $result")
    val number1 = 10
    val result1 = factorial(number1)
    println(s"The factorial of $number1 is: $result1")
  }
}

FactorialCalculator.main()

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez une fonction Scala pour trouver le maximum d'éléments dans un tableau.

// COMMAND ----------

object PowerCalculator {
  def calculatePower(base: Double, exponent: Int): Double = {
    var result = 1.0
    var i = 0

    while (i < exponent) {
      result *= base
      i += 1
    }

    result
  }

  def main(): Unit = {
    val base = 3.0
    val exponent = 4
    val result = calculatePower(base, exponent)
    println(s"The result of $base raised to the power of $exponent is: $result")
  }
}

PowerCalculator.main()

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez une fonction Scala pour vérifier si un nombre donné est un carré parfait.

// COMMAND ----------

object SquareChecker {
  def isPerfectSquare(number: Int): Boolean = {
    val sqrt = math.sqrt(number).toInt
    sqrt * sqrt == number
  }

  def main(args: Array[String]): Unit = {
    val number1 = 36
    val number2 = 19

    println(s"Is $number1 is a perfect square? ${isPerfectSquare(number1)}")
    println(s"Is $number2 is a perfect square? ${isPerfectSquare(number2)}")
  }
}

SquareChecker.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala pour parcourir une liste pour imprimer les éléments et calculer la somme et le produit de tous les éléments de cette liste.  

// COMMAND ----------

object Scala_List
{
  def main(args: Array[String]): Unit = 
  {
     //Iterate over a list
    val nums = List(1, 3, 5, 7, 9)
    println("Iterate over a list:")
    for( i <- nums)
    {  
     println(i)
    } 
   
    println("Sum all the items of the said list:")
    //Applying sum method 
    val result = nums.sum 
    println(result) 

    println("Multiplies all the items of the said list:")
    val result1 = nums.product 
    println(result1) 
    }
} 

Scala_List.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala pour imprimer une map  
// MAGIC Affichez la valeur d'une clè particulière    
// MAGIC Utlisez "getOrElse" pour traiter le cas ou la clé est absente  

// COMMAND ----------

object RetrieveValueFromMapExample {
  def main(args: Array[String]): Unit = {
    // Create a map
    val color_map = Map("Red" -> 1, "Green" -> 2, "Blue" -> 3, "Orange" -> 4)
    // Print the map
    println("Map: " + color_map)
    // Retrieve the value associated with a key
    val color_key = "Blue"
    val value = color_map.getOrElse(color_key, "Key not found")

    // Print the result
    println(s"The value associated with key '$color_key' is: $value")
  }
}

RetrieveValueFromMapExample.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala pour créer une Map et mettre à jour la valeur associée à une clé donnée.

// COMMAND ----------

object UpdateValueInMapExample {
  def main(args: Array[String]): Unit = {
    // Create a mutable map
    var color_map = collection.mutable.Map("Red" -> 1, "Green" -> 2, "Blue" -> 3, "Orange" -> 4)
    
    // Print the original map
    println("Original map: " + color_map)

    // Update the value associated with a key
    val key = "Green"
    val newValue = 7
    color_map(key) = newValue

    // Print the updated map
    println("Updated map: " + color_map)
  }
}

UpdateValueInMapExample.main(Array())
