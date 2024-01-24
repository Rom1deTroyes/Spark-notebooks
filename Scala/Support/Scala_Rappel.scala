// Databricks notebook source
// MAGIC %md
// MAGIC Création de variables
// MAGIC val veut dire que la variable ne peut pas être changée

// COMMAND ----------

val x = 1

// COMMAND ----------

// MAGIC %md
// MAGIC Ceci est une erreur

// COMMAND ----------

// x = 2

// COMMAND ----------

// MAGIC %md
// MAGIC Une variable crée avec val a une valeur définitive; il est une erreur de la réassigner  
// MAGIC En revanche le suivant est valable

// COMMAND ----------

val x = 2

// COMMAND ----------

// MAGIC %md
// MAGIC Ici on n'a pas rédefini l'ancienne variable; on a défini une nouvelle variable un utilisant le même nom  
// MAGIC En revanche var vous permet de rédefinir la valeur d'une variable  
// MAGIC Utlisez println') pour afficher une ligne

// COMMAND ----------

var y = 1

// COMMAND ----------

y = 2

// COMMAND ----------

// MAGIC %md
// MAGIC Créons une list et utilisons une boucle for pour afficher les éléments.  
// MAGIC Souvenez-vous cous que :
// MAGIC * Les éléments sont accédés avec des parenthèses.
// MAGIC * Les éléments sont 0 based ; le premier élément est l'élément 0

// COMMAND ----------

val l = List(1,2,3)

// COMMAND ----------

for (i <- 0 to l.length - 1) {
  println(i, l(i))
}

// COMMAND ----------

// MAGIC %md
// MAGIC Même chose avec Array

// COMMAND ----------

val arr = Array(1, 2, 3)

// COMMAND ----------

for (i <- 0 to arr.length - 1)
  println(i, arr(i))

// COMMAND ----------

// MAGIC %md
// MAGIC Néanmoins, il y a une différence entre List et Array.
// MAGIC * List est inchangeable.
// MAGIC * Array est changeable.

// COMMAND ----------

arr(0) = 10
for (i <- 0 to arr.length - 1)
  println(i, arr(i))

// COMMAND ----------

// MAGIC %md
// MAGIC Le suivant est une erreur

// COMMAND ----------

// l(0) = 10

// COMMAND ----------

// MAGIC %md
// MAGIC Voici comment obtenir la classe des variables

// COMMAND ----------

println(Array(0,1,2).getClass())
println(List(1,2,3).getClass())
println(1.getClass())
println("a".getClass())

// COMMAND ----------

// MAGIC %md
// MAGIC On n'utlise pas beaoucoup le boucles for en Scala  
// MAGIC Le suivant n'est pas une erreur mais on peut fair de façon plus rapide 

// COMMAND ----------

println(l)
println(l.length)
println

for (i <- 0 to l.length - 1)
  println(l(i))

println()

for (i <- l)
  println(i)

// COMMAND ----------

// MAGIC %md
// MAGIC Le suivant est beaoucoup plus concis 

// COMMAND ----------

l.foreach(i => println(i))
println()
l.foreach(x => println(x))

// COMMAND ----------

// MAGIC %md
// MAGIC On peur définir de calculs plus compliqués

// COMMAND ----------

//println(l)
//println

l.foreach(x => println(x, x*x, x*x*x))

// COMMAND ----------

// MAGIC %md
// MAGIC Structures if then else

// COMMAND ----------

val x = -2
if (x > 0)
{
  println("Positif")
}
else
{
  println("Not positif")
}

// COMMAND ----------

// MAGIC %md
// MAGIC Définissons une fonction simple  
// MAGIC Choses à remarquer
// MAGIC * Les type des paramètres doit être specifié
// MAGIC * La valeur remontée doit etre spécifiée; si rien est remonté mettez Unit  
// MAGIC Exemples
// MAGIC * `def f1(x: Int): Int` paramétre entier, entier renvoié
// MAGIC * `def f2(y: List[Int]): Double` paramètre liste d'entiers, Double renvoyé
// MAGIC * `def f3(x1: Int, x2: String): List[Any]` paramètres entier et string, renvoie une liste de n'import quel type d'éléments  
// MAGIC Remarquez `List[Int]` est différente de `List[Any]`

// COMMAND ----------

// Cett fonction prends un paramètre entier et renvoie un Unit (c'est à dire rien) 

def fonction_simple(x: Int): Unit = {
  if (x > 0)
  {
    println(s"$x est positif")
  }
  else
  {
    if (x == 0) {
      println(s"$x est zéro")
    } else {
      println(s"$x est négatif")
    }
  }
}

var l = List(-1, 0, 1, 2, 3)

l.foreach(i => fonction_simple(i))

// COMMAND ----------

// MAGIC %md
// MAGIC Autres exemples de fonctions

// COMMAND ----------

def mon_carré(x: Double): Double = {
  return x*x
}

List(0,1,2,3).foreach(a => println(mon_carré(a)))

// COMMAND ----------

val numbers = List(1, 2, 3, 4, 5, 6)

def somme(numbers: List[Int]): Int = {
    var sum: Int = 0
    for (number <- numbers) {
      sum = sum + number
      // sum += number
    }
    // Pour remonter un resultat tout simplement écrire ça
    sum

    // Pas erreur, mais inutile
    // return sum
}
val s = somme(numbers)
println(s"Voici la somme de la liste $s")

// COMMAND ----------

def another_func(x1: Int, x2: String): List[Any] = {
  List(x1+x1, x2+ " " + x2)
}

val ret_val = another_func(2, "Hello")
for (rr <-ret_val)
  println(rr)

// Ceci est une erreur
// another_func(1,2)

// COMMAND ----------

// MAGIC %md
// MAGIC Valeurs booléennes

// COMMAND ----------

val b = true

if (b)
  println("OK")
else
  println("Cela ne devrait pas arriver")

val bb = !b

if (bb)
  println("Cela ne devrait pas arriver")
else
  println("OK")


// COMMAND ----------

// MAGIC %md
// MAGIC Strings

// COMMAND ----------

val ex_string = "Salut tout le monde"

println(ex_string)
println(ex_string.length)
println

for (i <- ex_string)
  println(i)

println

println(ex_string(0))
println(ex_string(4))

// COMMAND ----------

// MAGIC %md
// MAGIC Interpolation des strings 

// COMMAND ----------

val foo = 2
val bar = s"ceci est une string interpolée avec la valeur $foo"

println(bar)

// COMMAND ----------

// MAGIC %md
// MAGIC Voici des exemples de map et reduce  
// MAGIC `map()` prends une collection (List ou Array) et en renvoie une autre en appliquant la fonction spécifiée
// MAGIC `reduce()` prends une prends une collection (List ou Array) et en renvoie une autre en appliquant la fonction spécifiée sur une paire de valeurs

// COMMAND ----------

val numbers : List[Int] = List(1, 2, 3, 4, 5, 6)

val doubles = numbers.map(i => 2*i)

val triplets = numbers.map(z => (z, z*z, z*z*z))

val sum_numbers = numbers.reduce((x,y) => x+y)

val sum_doubles = numbers.map(x => 2*x).reduce((x,y) => x+y)

// COMMAND ----------

// MAGIC %md
// MAGIC Même chose avec les arrays

// COMMAND ----------

val anumbers = Array(1, 2, 3, 4, 5, 6)

val doubles = anumbers.map(i => 2*i)

val triplets = anumbers.map(z => (z, z*z, z*z*z))

val sum_numbers = anumbers.reduce((x,y) => x+y)

val sum_doubles = anumbers.map(x => 2*x).reduce((x,y) => x+y)

// COMMAND ----------

// MAGIC %md
// MAGIC Utlisation de fichiers

// COMMAND ----------

import java.io._

val path = "hello.txt"

val fileWriter = new FileWriter(new File(path))

fileWriter.write("hello there\n")
fileWriter.write("hello there again\n")
fileWriter.write("hello there third time\n")

fileWriter.close()

// COMMAND ----------

import scala.io.Source

val bufferedSource = Source.fromFile(path)

println( bufferedSource)

val lines = bufferedSource.getLines.toList

bufferedSource.close()

lines.foreach(i => println(i))
