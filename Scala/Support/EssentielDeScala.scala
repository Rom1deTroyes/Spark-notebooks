// Databricks notebook source
// MAGIC %md
// MAGIC Pour faire pratique de Scala 3  
// MAGIC https://scastie.scala-lang.org/

// COMMAND ----------

// MAGIC %md
// MAGIC # Variables

// COMMAND ----------

// MAGIC %md
// MAGIC ### Deux types de variables  
// MAGIC
// MAGIC Lorsque vous créez une nouvelle variable dans Scala, vous déclarez si la variable est immuable ou mutable :  
// MAGIC
// MAGIC Description du type de variable  
// MAGIC
// MAGIC * val crée une variable immuable, comme final en Java.  
// MAGIC Il est mieux en général de créer une variable avec val, sauf s'il y a une raison pour laquelle vous avez besoin d'une variable mutable.  
// MAGIC
// MAGIC * var crée une variable mutable et ne doit être utilisée que lorsque le contenu d'une variable change au fil du temps.  
// MAGIC
// MAGIC Ces exemples montrent comment créer des variables val et var :  

// COMMAND ----------

// immutable
val a = 0

// mutable
var b = 1

// COMMAND ----------

// MAGIC %md
// MAGIC # Ceci est une erreur

// COMMAND ----------

//val msg = "Hello, world"
//msg = "Aloha"   // "reassignment to val" error; this won’t compile

// COMMAND ----------

// MAGIC %md
// MAGIC A l’inverse, une var peut être réaffectée :  

// COMMAND ----------

var msg = "Hello, world"
msg = "Aloha"   // this compiles because a var can be reassigned

// COMMAND ----------

// MAGIC %md
// MAGIC Déclaration des types de variables  
// MAGIC
// MAGIC Lorsque vous créez une variable, vous pouvez déclarer explicitement son type ou laisser le compilateur déduire le type :  

// COMMAND ----------

val x: Int = 1   // explicit

// COMMAND ----------

val x = 1        // implicit; the compiler infers the type

// COMMAND ----------

// MAGIC %md
// MAGIC La deuxième forme est connue sous le nom d’inférence de type et constitue un excellent moyen de maintenir ce type de code concis.  
// MAGIC Le compilateur Scala peut généralement déduire le type de données pour vous :  

// COMMAND ----------

val x = 1

// COMMAND ----------

val s = "a string"

// COMMAND ----------

val nums = List(1, 2, 3)

// COMMAND ----------

// MAGIC %md
// MAGIC Vous pouvez toujours déclarer le type d’une variable si vous préférez, mais dans des affectations simples comme celles-ci, ce n’est pas nécessaire :

// COMMAND ----------

val x: Int = 1
val s: String = "a string"

// COMMAND ----------

// MAGIC %md
// MAGIC # Scala types
// MAGIC
// MAGIC https://docs.scala-lang.org/scala3/book/first-look-at-types.html
// MAGIC
// MAGIC Any est le supertype de tous les types, également appelé type supérieur.
// MAGIC Il définit certaines méthodes universelles telles que equals, hashCode et toString.
// MAGIC
// MAGIC Le type supérieur Any a un sous-type Matchable, qui est utilisé pour marquer tous les types sur lesquels nous pouvons effectuer une correspondance de modèles.  
// MAGIC
// MAGIC Il est important de garantir une propriété appelée « paramétricité ».
// MAGIC
// MAGIC Nous n'entrerons pas dans les détails ici, mais en résumé, cela signifie que nous ne pouvons pas faire de correspondance de modèle sur des valeurs de type Any, mais uniquement sur des valeurs qui sont un sous-type de Matchable.
// MAGIC
// MAGIC La documentation de référence contient plus d’informations sur Matchable.
// MAGIC
// MAGIC Matchable a deux sous-types importants : AnyVal et AnyRef.
// MAGIC
// MAGIC * AnyVal représente les types de valeur.  
// MAGIC Il existe quelques types de valeurs prédéfinis, et ils ne peuvent pas être nuls : Double, Float, Long, Int, Short, Byte, Char, Unit et Boolean.  
// MAGIC Unit un type valeur qui ne contient aucune information significative.  
// MAGIC Il existe exactement une instance de Unit que nous pouvons appeler : ().  
// MAGIC
// MAGIC * AnyRef représente les types de référence.  
// MAGIC Tous les types sans valeur sont définis comme types de référence.  
// MAGIC Chaque type défini par l'utilisateur dans Scala est un sous-type d'AnyRef.  
// MAGIC Si Scala est utilisé dans le contexte d'un environnement d'exécution Java, AnyRef correspond à java.lang.Object.  

// COMMAND ----------

val list: List[Any] = List(
  "a string",
  732,  // an integer
  'c',  // a character
  '\'', // a character with a backslash escape
  true, // a boolean value
  () => "an anonymous function returning a string"
)

list.foreach(element => println(element))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Types de données standard  
// MAGIC
// MAGIC Scala est livré avec les types de données numériques standard auxquels vous vous attendez, et ce sont tous des instances de classes à part entière. Chez Scala, tout est objet.  

// COMMAND ----------

val b: Byte = 1
val i: Int = 1
val l: Long = 1
val s: Short = 1
val d: Double = 2.0
// F is necessary
val f: Float = 3.0F

// COMMAND ----------

// MAGIC %md
// MAGIC Étant donné que Int et Double sont les types numériques par défaut, vous les créez généralement sans déclarer explicitement le type de données :  

// COMMAND ----------

val i = 123   // defaults to Int
val j = 1.0   // defaults to Double

// COMMAND ----------

// MAGIC %md
// MAGIC Dans votre code, vous pouvez également ajouter les caractères L, D et F (et leurs équivalents minuscules) aux nombres pour spécifier qu'il s'agit de valeurs Long, Double ou Float :  

// COMMAND ----------

val x = 1000L   // val x: Long = 1000
val y = 2.2D     // val y: Double = 2.2
val z = 3.3F     // val z: Float = 3.3

// COMMAND ----------

// MAGIC %md
// MAGIC Lorsque vous avez besoin de très grands nombres, utilisez les types BigInt et BigDecimal :

// COMMAND ----------

var a = BigInt(1234567890987654321L)
var b = BigDecimal(123456.789)

// Scala 3 seulement
// var a = BigInt(1_234_567_890_987_654_321L)
// var b = BigDecimal(123_456.789)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Strings
// MAGIC
// MAGIC Les strings Scala sont similaires aux Java strings, mais elles possèdent deux fonctionnalités supplémentaires intéressantes :  
// MAGIC
// MAGIC * Ils prennent en charge l'interpolation de chaînes
// MAGIC * Il est facile de créer des chaînes multilignes
// MAGIC
// MAGIC Interpolation de chaîne
// MAGIC
// MAGIC L'interpolation de chaînes fournit un moyen très lisible d'utiliser des variables à l'intérieur de chaînes. Par exemple, étant donné ces trois variables :  

// COMMAND ----------

val firstName = "John"
val mi = 'C'
val lastName = "Doe"

// COMMAND ----------

println(s"Name: $firstName $mi $lastName")   // "Name: John C Doe"

// COMMAND ----------

// MAGIC %md
// MAGIC Faites simplement précéder la chaîne de la lettre s, puis placez un symbole $ avant les noms de vos variables à l'intérieur de la chaîne.  
// MAGIC
// MAGIC Pour intégrer des expressions arbitraires dans une chaîne, placez-les entre accolades :  

// COMMAND ----------

println(s"2 + 2 = ${2 + 2}")   // prints "2 + 2 = 4"

val x = -1
println(s"x.abs = ${x.abs}")   // prints "x.abs = 1"

// COMMAND ----------

// MAGIC %md
// MAGIC L'interpolateur f (f-Strings)
// MAGIC
// MAGIC Ajouter f à n'importe quelle chaîne littérale permet la création de chaînes formatées simples, similaires à printf dans d'autres langages.  
// MAGIC Lors de l'utilisation de l'interpolateur f, toutes les références de variables doivent être suivies d'une chaîne de format de style printf, comme %d  

// COMMAND ----------

val height = 1.9d
val name = "James"
println(f"$name%s is $height%2.2f meters tall")  // "James is 1.90 meters tall

// COMMAND ----------

// use %% to get a literal % character in the output string
println(f"3/19 is less than 20%%")  // "3/19 is less than 20%"

// COMMAND ----------

// MAGIC %md
// MAGIC Les chaînes multilignes sont créées en incluant la chaîne entre trois guillemets :  

// COMMAND ----------

val quote = """The essence of Scala:
                  Fusion of functional and object-oriented
                  programming in a typed setting."""

// COMMAND ----------

// MAGIC %md
// MAGIC Nothing est un sous-type de tous les types, également appelé type inférieur.  
// MAGIC Aucune valeur n’a le type Nothing.  
// MAGIC Une utilisation courante consiste à signaler une non-terminaison, comme une exception levée, une sortie de programme ou une boucle infinie, c'est-à-dire qu'il s'agit du type d'une expression qui n'évalue pas en valeur ou d'une méthode qui ne renvoie pas normalement.  
// MAGIC
// MAGIC Null est un sous-type de tous les types référence (c'est-à-dire n'importe quel sous-type d'AnyRef).  
// MAGIC Il a une valeur unique identifiée par le mot-clé littéral null.  
// MAGIC Actuellement, l’utilisation de null est considérée comme une mauvaise pratique. Il doit être utilisé principalement pour l'interopérabilité avec d'autres langages JVM.  
// MAGIC Une option opt-in du compilateur modifie le statut Null pour corriger les mises en garde liées à son utilisation.  
// MAGIC Cette option pourrait devenir la valeur par défaut dans une future version de Scala.  
// MAGIC   
// MAGIC null ne devrait presque jamais être utilisé dans le code Scala.  

// COMMAND ----------

// MAGIC %md
// MAGIC ### Structures de contrôle
// MAGIC
// MAGIC Scala possède les structures de contrôle que vous trouvez dans d'autres langages de programmation, et dispose également de puissants expressions et expressions de correspondance :  
// MAGIC
// MAGIC if/else

// COMMAND ----------

val x = -1

if (x < 0) {
  println("negative")
} else if (x == 0) {
  println("zero")
} else {
  println("positive")
}

// Scala 3
// if x < 0 then
// println("negative")
// else if x == 0 then
//    println("zero")
// else
//   println("positive")

// COMMAND ----------

// MAGIC %md
// MAGIC Notez qu’il s’agit en réalité d’une expression et non d’une déclaration.  
// MAGIC Cela signifie qu'il renvoie une valeur, vous pouvez donc affecter le résultat à une variable :  

// COMMAND ----------

val a = 0
val b = 2
val x = if (a < b) { a } else { b }

// Scala 3
// val x = if a < b then a else b

// COMMAND ----------

// MAGIC %md
// MAGIC for loops

// COMMAND ----------

val ints = List(1, 2, 3, 4, 5)

for (i <- ints) println(i)

// Scala 3
// for i <- ints do println(i)

// COMMAND ----------

// MAGIC %md
// MAGIC Lorsque vous utilisez le mot-clé yield au lieu de do, vous créez des expressions qui sont utilisées pour calculer et produire des résultats.

// COMMAND ----------

val ints = List(1, 2, 3, 4, 5)
val doubles = for (i <- ints) yield i * 2

// Scala 3
// val doubles = for i <- ints yield i * 2

// COMMAND ----------

val doubles1 = for (i <- ints) yield i * 2
val doubles2 = for (i <- ints) yield (i * 2)
val doubles3 = for { i <- ints } yield (i * 2)

// Scala 3
// val doubles = for i <- ints yield i * 2     // style shown above
// val doubles = for (i <- ints) yield i * 2
// val doubles = for (i <- ints) yield (i * 2)
// val doubles = for { i <- ints } yield (i * 2)

// COMMAND ----------

val names = List("chris", "ed", "maurice")
val capNames = for (name <- names) yield name.capitalize

// Scala 3
// val names = List("chris", "ed", "maurice")
// val capNames = for name <- names yield name.capitalize

// COMMAND ----------

// MAGIC %md
// MAGIC match expressions

// COMMAND ----------

val i = 1

i match {
  case 1 => println("one")
  case 2 => println("two")
  case _ => println("other")
}

// Scala 3
// i match
//   case 1 => println("one")
//   case 2 => println("two")
//   case _ => println("other")

// COMMAND ----------

// MAGIC %md
// MAGIC Cependant, match est en réalité une expression, ce qui signifie qu'elle renvoie un résultat basé sur la correspondance de modèle, que vous pouvez lier à une variable :  

// COMMAND ----------

val result = i match {
  case 1 => "one"
  case 2 => "two"
  case _ => "other"
}

// COMMAND ----------

// MAGIC %md
// MAGIC En fait, une expression match peut être utilisée pour tester une variable par rapport à de nombreux types de modèles différents.  
// MAGIC Cet exemple montre (a) comment utiliser une expression match comme corps d'une méthode, et (b) comment faire correspondre tous les différents types affichés :  

// COMMAND ----------

// getClassAsString is a method that takes a single argument of any type.
def getClassAsString(x: Any): String = x match {
  case s: String => s"'$s' is a String"
  case i: Int => "Int"
  case d: Double => "Double"
  case l: List[_] => "List"
  case _ => "Unknown"
}

// Scala 3
// def getClassAsString(x: Matchable): String = x match
//   case s: String => s"'$s' is a String"
//   case i: Int => "Int"
//   case d: Double => "Double"
//   case l: List[?] => "List"
//   case _ => "Unknown"

// examples
println(getClassAsString(1))               // Int
println(getClassAsString("hello"))         // 'hello' is a String
println(getClassAsString(List(1, 2, 3)))   // List
println(getClassAsString(Array(1, 2, 3)))  // Unknown

// COMMAND ----------

// MAGIC %md
// MAGIC while loops

// COMMAND ----------

var x = 1

while (x < 3) {
  println(x)
  x += 1
}

// Scala 3
// while
//   x < 3
// do
//   println(x)
//   x += 1

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fonctions

// COMMAND ----------

def sum(a: Int, b: Int): Int = a + b
def concatenate(s1: String, s2: String): String = s1 + s2

// COMMAND ----------

val x = sum(1,3)
val y = concatenate("Hello", "World")

// COMMAND ----------

// MAGIC %md
// MAGIC Fonction multiligne

// COMMAND ----------

def exMultiLigne(x : Int): Unit = {
  val l = 3
  val m = l * x
  println(m)
}

exMultiLigne(4)

// Scala 3
// def exMultiLigne(x : Int): Unit =
//   val l = 3
//   val m = l * x
//   println(m)



// COMMAND ----------

// MAGIC %md
// MAGIC ## First-Class Functions  
// MAGIC
// MAGIC Scala possède la plupart des fonctionnalités que vous attendez dans un langage de programmation fonctionnel, notamment :  
// MAGIC
// MAGIC * Lambdas (fonctions anonymes)
// MAGIC * Fonctions d'ordre supérieur (HOF)
// MAGIC * Collections immuables dans la bibliothèque standard
// MAGIC
// MAGIC Les lambdas, également connues sous le nom de fonctions anonymes, jouent un rôle important pour garder votre code concis mais lisible.  
// MAGIC La méthode map de la classe List est un exemple typique de fonction d'ordre supérieur, une fonction qui prend une fonction comme paramètre.  
// MAGIC Ces deux exemples sont équivalents et montrent comment multiplier chaque nombre d'une liste par 2 en passant un lambda dans la méthode map :  

// COMMAND ----------

val a = List(1, 2, 3).map(i => i * 2)   // List(2,4,6)
val b = List(1, 2, 3).map(_ * 2)        // List(2,4,6)

// COMMAND ----------

// MAGIC %md
// MAGIC Ces exemples sont également équivalents au code suivant, qui utilise une méthode double au lieu d'un lambda : 

// COMMAND ----------

def m_double(i: Int): Int = i * 2

val a = List(1, 2, 3).map(i => m_double(i))   // List(2,4,6)
val b = List(1, 2, 3).map(m_double)           // List(2,4,6)

// COMMAND ----------

// MAGIC %md
// MAGIC Autres exemples

// COMMAND ----------

val ints = List(1, 2, 3, 4, 5)

// COMMAND ----------

val doubledInts = ints.map(_ * 2)

// COMMAND ----------

val doubledInts = ints.map((i: Int) => i * 2)

// COMMAND ----------

val doubledInts = ints.map((i) => i * 2)

// COMMAND ----------

val doubledInts = ints.map(i => i * 2)

// COMMAND ----------

val doubledInts = ints.map(_ * 2)

// COMMAND ----------

// MAGIC %md
// MAGIC Aller encore plus court  
// MAGIC
// MAGIC Dans d'autres exemples, vous pouvez simplifier davantage vos fonctions anonymes.  
// MAGIC Par exemple, en commençant par la forme la plus explicite, vous pouvez imprimer chaque élément dans des entiers en utilisant cette fonction anonyme avec la méthode foreach de la classe List :

// COMMAND ----------

ints.foreach((i: Int) => println(i))

// COMMAND ----------

ints.foreach(i => println(i))

// COMMAND ----------

ints.foreach(println(_))

// COMMAND ----------

ints.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC # Classes
// MAGIC
// MAGIC Comme dans d'autres langages, une classe dans Scala est un modèle pour la création d'instances d'objet  

// COMMAND ----------

class Person(var name: String, var vocation: String)
class Book(var title: String, var author: String, var year: Int)
class Movie(var name: String, var director: String, var year: Int)

// COMMAND ----------

val p = new Person("Robert Allen Zimmerman", "Harmonica Player")

// Scala 3
// val p = Person("Robert Allen Zimmerman", "Harmonica Player")

// COMMAND ----------

p.name       

// COMMAND ----------

p.vocation

// COMMAND ----------

// MAGIC %md
// MAGIC tous ces paramètres ont été créés sous forme de champs var, vous pouvez donc également les muter :

// COMMAND ----------

p.name = "Bob Dylan"
p.vocation = "Musician"

// COMMAND ----------

p.name

// COMMAND ----------

p.vocation

// COMMAND ----------

p.name = "Bob Dylan"
p.vocation = "Musician"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Champs et méthodes
// MAGIC
// MAGIC Les classes peuvent également avoir des méthodes et des champs supplémentaires qui ne font pas partie des constructeurs.  
// MAGIC Ils sont définis dans le corps de la classe.  
// MAGIC Le corps est initialisé dans le cadre du constructeur par défaut :

// COMMAND ----------

class Person(var firstName: String, var lastName: String) {

  println("initialization begins")
  val fullName = firstName + " " + lastName

  // a class method
  def printFullName: Unit =
    // access the `fullName` field, which is created above
    println(fullName)

  printFullName
  println("initialization ends")
}

// Scala 3
//class Person(var firstName: String, var lastName: String):
//  println("initialization begins")
//  val fullName = firstName + " " + lastName
  // a class method
//  def printFullName: Unit =
    // access the `fullName` field, which is created above
//    println(fullName)
//  printFullName
//  println("initialization ends")

// COMMAND ----------

val john = new Person("John", "Doe")

// Scala 3
// val john = Person("John", "Doe")

// COMMAND ----------

john.printFullName

// COMMAND ----------

// MAGIC %md
// MAGIC Valeurs des paramètres par défaut

// COMMAND ----------

class Socket(val timeout: Int = 5000, val linger: Int = 5000) {
  override def toString = s"timeout: $timeout, linger: $linger"
}
// Scala 3
// class Socket(val timeout: Int = 5_000, val linger: Int = 5_000):
//   override def toString = s"timeout: $timeout, linger: $linger"

// COMMAND ----------

// MAGIC %md
// MAGIC # Objets
// MAGIC
// MAGIC Un objet est une classe qui possède exactement une instance.  
// MAGIC
// MAGIC Il est initialisé paresseusement lorsque ses membres sont référencés, semblable à un val paresseux.  
// MAGIC
// MAGIC Les objets dans Scala permettent de regrouper des méthodes et des champs sous un seul espace de noms, de la même manière que vous utilisez des membres statiques sur une classe en Java, Javascript (ES6) ou @staticmethod en Python.  
// MAGIC
// MAGIC Déclarer un objet est similaire à déclarer une classe.  
// MAGIC
// MAGIC Voici un exemple d'objet « utilitaires de chaînes » qui contient un ensemble de méthodes permettant de travailler avec des chaînes :  

// COMMAND ----------

object StringUtils {
  def truncate(s: String, length: Int): String = s.take(length)
  def containsWhitespace(s: String): Boolean = s.matches(".*\\s.*")
  def isNullOrEmpty(s: String): Boolean = s == null || s.trim.isEmpty
}

/*
object StringUtils:
  def truncate(s: String, length: Int): String = s.take(length)
  def containsWhitespace(s: String): Boolean = s.matches(".*\\s.*")
  def isNullOrEmpty(s: String): Boolean = s == null || s.trim.isEmpty
*/

// COMMAND ----------

// Java 2 et 3

StringUtils.truncate("Chuck Bartowski", 5)  // "Chuck"

// COMMAND ----------

// MAGIC %md
// MAGIC L'importation dans Scala est très flexible et nous permet d'importer tous les membres d'un objet :

// COMMAND ----------

import StringUtils._
truncate("Chuck Bartowski", 5)       // "Chuck"
containsWhitespace("Sarah Walker")   // true
isNullOrEmpty("John Casey")          // false

/*
import StringUtils.*
truncate("Chuck Bartowski", 5)       // "Chuck"
containsWhitespace("Sarah Walker")   // true
isNullOrEmpty("John Casey")          // false
*/

// COMMAND ----------

// MAGIC %md
// MAGIC ou juste quelques membres :

// COMMAND ----------

// 2 et 3

import StringUtils.{truncate, containsWhitespace}
truncate("Charles Carmichael", 7)       // "Charles"
containsWhitespace("Captain Awesome")   // true
isNullOrEmpty("Morgan Grimes")          // Not found: isNullOrEmpty (error)

// COMMAND ----------

object MathConstants {
  val PI = 3.14159
  val E = 2.71828
}

println(MathConstants.PI)   // 3.14159

/*
object MathConstants:
  val PI = 3.14159
  val E = 2.71828

println(MathConstants.PI)   // 3.14159

*/

// COMMAND ----------

// MAGIC %md
// MAGIC # Objets compagnons
// MAGIC
// MAGIC Un objet qui porte le même nom qu’une classe et qui est déclaré dans le même fichier que la classe est appelé « objet compagnon ».  
// MAGIC
// MAGIC De même, la classe correspondante est appelée classe compagnon de l’objet.  
// MAGIC
// MAGIC Une classe ou un objet compagnon peut accéder aux membres privés de son compagnon.  
// MAGIC
// MAGIC Les objets compagnon sont utilisés pour les méthodes et les valeurs qui ne sont pas spécifiques aux instances de la classe compagnon.  
// MAGIC
// MAGIC Par exemple, dans l'exemple suivant, la classe Circle a un membre nommé Area qui est spécifique à chaque instance, et son objet compagnon a une méthode nommée calculateArea qui (a) n'est pas spécifique à une instance et (b) est disponible pour chaque instance. :  

// COMMAND ----------

import scala.math._

class Circle(val radius: Double) {
  def area: Double = Circle.calculateArea(radius)
}

object Circle {
  private def calculateArea(radius: Double): Double = Pi * pow(radius, 2.0)
}

val circle1 = new Circle(5.0)
circle1.area

/*
Scala 3

import scala.math.*

class Circle(val radius: Double):
  def area: Double = Circle.calculateArea(radius)

object Circle:
  private def calculateArea(radius: Double): Double = Pi * pow(radius, 2.0)

val circle1 = Circle(5.0)
circle1.area
*/

// COMMAND ----------

// MAGIC %md
// MAGIC Autres utilisations  
// MAGIC
// MAGIC Les objets compagnon peuvent être utilisés à plusieurs fins :  
// MAGIC
// MAGIC * Comme indiqué, elles peuvent être utilisées pour regrouper les méthodes « statiques » sous un espace de noms.  
// MAGIC * Ces méthodes peuvent être publiques ou privées  
// MAGIC * Si calculateArea était public, il serait accessible en tant que Circle.calculateArea
// MAGIC * Ils peuvent contenir des méthodes d'application qui, grâce à un peu de sucre syntaxique, fonctionnent comme des méthodes d'usine pour construire de nouvelles instances.
// MAGIC * Ils peuvent contenir des méthodes d'annulation d'application, utilisées pour déconstruire des objets, par exemple avec la correspondance de modèles.
// MAGIC
// MAGIC Voici un aperçu rapide de la façon dont les méthodes apply peuvent être utilisées comme méthodes d’usine pour créer de nouveaux objets :

// COMMAND ----------

class Person {
  var name = ""
  var age = 0
  override def toString = s"$name is $age years old"
}

object Person {
  // a one-arg factory method
  def apply(name: String): Person = {
    var p = new Person
    p.name = name
    p
  }

  // a two-arg factory method
  def apply(name: String, age: Int): Person = {
    var p = new Person
    p.name = name
    p.age = age
    p
  }
}

val joe = Person("Joe")
val fred = Person("Fred", 29)

//val joe: Person = Joe is 0 years old
//val fred: Person = Fred is 29 years old

/*
Scala 3
class Person:
  var name = ""
  var age = 0
  override def toString = s"$name is $age years old"

object Person:

  // a one-arg factory method
  def apply(name: String): Person =
    var p = new Person
    p.name = name
    p

  // a two-arg factory method
  def apply(name: String, age: Int): Person =
    var p = new Person
    p.name = name
    p.age = age
    p

end Person

val joe = Person("Joe")
val fred = Person("Fred", 29)
*/

// COMMAND ----------

// MAGIC %md
// MAGIC # Traits  
// MAGIC
// MAGIC Si vous êtes familier avec Java, un trait Scala est similaire à une interface dans Java 8+.  
// MAGIC
// MAGIC Les traits peuvent contenir :  
// MAGIC * Méthodes et champs abstraits  
// MAGIC * Méthodes et domaines concrets
// MAGIC
// MAGIC Dans une utilisation basique, un trait peut être utilisé comme interface, définissant uniquement les membres abstraits qui seront implémentés par d'autres classes :

// COMMAND ----------

trait Employee {
  def id: Int
  def firstName: String
  def lastName: String
}

/*
trait Employee:
  def id: Int
  def firstName: String
  def lastName: String
*/

// COMMAND ----------

// MAGIC %md
// MAGIC Cependant, les traits peuvent également contenir des membres concrets.  
// MAGIC
// MAGIC Par exemple, le trait suivant définit deux membres abstraits (numLegs et walk()) et possède également une implémentation concrète d'une méthode stop() :  

// COMMAND ----------

trait HasLegs {
  def numLegs: Int
  def walk(): Unit
  def stop() = println("Stopped walking")
}

/*
trait HasLegs:
  def numLegs: Int
  def walk(): Unit
  def stop() = println("Stopped walking")
*/

// COMMAND ----------

trait HasTail {
  def tailColor: String
  def wagTail() = println("Tail is wagging")
  def stopTail() = println("Tail is stopped")
}

/*
trait HasTail:
  def tailColor: String
  def wagTail() = println("Tail is wagging")
  def stopTail() = println("Tail is stopped")
*/

// COMMAND ----------

class IrishSetter(name: String) extends HasLegs with HasTail {
  val numLegs = 4
  val tailColor = "Red"
  def walk() = println("I’m walking")
  override def toString = s"$name is a Dog"
}

/*
class IrishSetter(name: String) extends HasLegs, HasTail:
  val numLegs = 4
  val tailColor = "Red"
  def walk() = println("I’m walking")
  override def toString = s"$name is a Dog"
*/

// COMMAND ----------

val d = new IrishSetter("Big Red")   // "Big Red is a Dog"

/*
val d = IrishSetter("Big Red")   // "Big Red is a Dog"
*/

// COMMAND ----------

// MAGIC %md
// MAGIC Case classes  
// MAGIC
// MAGIC Les classes de cas sont utilisées pour modéliser des structures de données immuables.  
// MAGIC Prenons l'exemple suivant :  puisque nous déclarons Person comme case classes, les champs nom et relation sont publics et immuables par défaut.  

// COMMAND ----------

case class Person(name: String, relation: String)

// COMMAND ----------

val christina = Person("Christina", "niece")

// COMMAND ----------

// ERREUR

// christina.name = "Fred"   // error: reassignment to val

// COMMAND ----------

// MAGIC %md
// MAGIC # Main()
// MAGIC
// MAGIC Ceci est le point du depart d'un programme Scala

// COMMAND ----------

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello, World!")
  }
}

HelloWorld.main(Array())

// COMMAND ----------

object HelloWorld2 {
  def main(): Unit = {
    println("Hello, World!")
  }
}

HelloWorld2.main()

// COMMAND ----------

object HelloWorld3 {
  def main(args: Array[String]): Unit = {
    for (i <- 0 to args.length - 1) {
      println(args(i))
    }
  }
}

HelloWorld3.main(Array("Hello", "World", "Here", "I", "Am"))

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Collections en Scala
// MAGIC
// MAGIC https://docs.scala-lang.org/scala3/book/collections-classes.html
// MAGIC
// MAGIC Performances des différentes collections  
// MAGIC https://docs.scala-lang.org/overviews/collections-2.13/performance-characteristics.html
// MAGIC
// MAGIC Trois grandes catégories de collections  
// MAGIC
// MAGIC En regardant les collections Scala d'un niveau élevé, vous avez le choix entre trois catégories principales :
// MAGIC
// MAGIC * Les séquences **(Seq)** sont une collection séquentielle d'éléments et peuvent être indexées (comme un tableau) ou linéaires (comme une liste chaînée)
// MAGIC * Les cartes **(Map)** contiennent une collection de paires clé/valeur, comme une carte Java, un dictionnaire Python ou Ruby Hash
// MAGIC * Les ensembles **(Set)** sont une collection désordonnée d'éléments uniques
// MAGIC
// MAGIC Ce sont tous des types de base et ont des sous-types à des fins spécifiques, telles que la concurrence, la mise en cache et le streaming.  
// MAGIC En plus de ces trois catégories principales, il existe d'autres types de collections utiles, notamment les plages, les piles et les files d'attente.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Une note sur les collections immutables
// MAGIC
// MAGIC Dans les sections qui suivent, chaque fois que le mot immutable est utilisé, il est raisonnable de supposer que le type est destiné à être utilisé dans un style de programmation fonctionnelle (FP).  
// MAGIC Avec ces types, vous ne modifiez pas la collection ; vous appliquez des méthodes fonctionnelles à la collection pour créer un nouveau résultat.
// MAGIC
// MAGIC Choisir une séquence
// MAGIC
// MAGIC Lorsque vous choisissez une séquence (une collection séquentielle d'éléments), vous avez deux décisions principales :
// MAGIC
// MAGIC * La séquence doit-elle être indexée (comme un tableau), permettant un accès rapide à n'importe quel élément, ou doit-elle être implémentée sous forme de liste chaînée linéaire ?
// MAGIC * Voulez-vous une collection mutable ou immutable ?
// MAGIC
// MAGIC Les collections séquentielles recommandées, à usage général, « à consulter » pour les combinaisons mutable/immuable et indexé/linéaire sont présentées ici :
// MAGIC
// MAGIC |Type/Category| 	Immutable| 	Mutable|
// MAGIC | -------- | ------- | ---------- |
// MAGIC |Indexed| 	Vector| 	ArrayBuffer|
// MAGIC |Linear (Linked lists)| 	List| 	ListBuffer|

// COMMAND ----------

// MAGIC %md
// MAGIC Les exemples suivants sont valables en Scala 2 et 3  
// MAGIC Vous pouvez utliser https://scastie.scala-lang.org  
// MAGIC Pour faire pratique de Scala 3

// COMMAND ----------

// MAGIC %md
// MAGIC Le type List est une séquence linéaire et immuable.  
// MAGIC Cela signifie simplement qu’il s’agit d’une liste chaînée que vous ne pouvez pas modifier.  
// MAGIC Chaque fois que vous souhaitez ajouter ou supprimer des éléments de liste, vous créez une nouvelle liste à partir d'une liste existante.
// MAGIC
// MAGIC Voici comment créer une liste initiale :

// COMMAND ----------

val ints = List(1, 2, 3)
val names = List("Joel", "Chris", "Ed")

// another way to construct a List
val namesAgain = "Joel" :: "Chris" :: "Ed" :: Nil

// COMMAND ----------

// MAGIC %md
// MAGIC Vous pouvez également déclarer le type de la Liste, si vous préférez, même si cela n’est généralement pas nécessaire :

// COMMAND ----------

val ints: List[Int] = List(1, 2, 3)
val names: List[String] = List("Joel", "Chris", "Ed")

// COMMAND ----------

// MAGIC %md
// MAGIC Ajouter des éléments à une liste
// MAGIC
// MAGIC Parce que List est immuable, vous ne pouvez pas y ajouter de nouveaux éléments.  
// MAGIC Au lieu de cela, vous créez une nouvelle liste en préfixant ou en ajoutant des éléments à une liste existante.  
// MAGIC Par exemple, étant donné cette liste :

// COMMAND ----------

val a = List(1, 2, 3)

// COMMAND ----------

// MAGIC %md
// MAGIC Lorsque vous travaillez avec une liste, ajoutez un élément avec :: et une autre liste avec :::, comme indiqué ici :

// COMMAND ----------

val b = 0 :: a              // List(0, 1, 2, 3)
val c = List(-1, 0) ::: a   // List(-1, 0, 1, 2, 3)

// COMMAND ----------

// MAGIC %md
// MAGIC Vous pouvez également ajouter des éléments à une liste, mais comme List est une liste chaînée ("single link"), vous ne devez généralement y ajouter que des éléments ; y ajouter des éléments est une opération relativement lente, surtout lorsque vous travaillez avec de grandes séquences.
// MAGIC
// MAGIC #### Astuce : Si vous souhaitez préfixer et ajouter des éléments à une séquence immutable, utilisez plutôt Vector.
// MAGIC
// MAGIC Parce que List est une liste chaînée, vous ne devriez pas essayer d'accéder aux éléments des grandes listes par leur valeur d'index.  
// MAGIC Par exemple, si vous avez une liste contenant un million d'éléments, l'accès à un élément tel que myList(999999) prendra un temps relativement long, car cette requête doit parcourir tous ces éléments.  
// MAGIC Si vous avez une grande collection et vous souhaitez accéder aux éléments par leur index, utilisez plutôt un Vector ou un ArrayBuffer.

// COMMAND ----------

// MAGIC %md
// MAGIC Comment parcourir des listes
// MAGIC
// MAGIC Étant donné une liste de noms, vous pouvez imprimer chaque chaîne comme ceci :

// COMMAND ----------

val names = List("Joel", "Chris", "Ed")

for (name <- names) println(name)

// Scala 3 synatxe
// for name <- names do println(name)

// COMMAND ----------

// MAGIC %md
// MAGIC Vector
// MAGIC
// MAGIC Le vecteur (Vector) est une séquence indexée et immuable.  
// MAGIC La partie « indexée » de la description signifie qu'elle fournit un accès aléatoire et une mise à jour en temps effectivement constant, de sorte que vous pouvez accéder rapidement aux éléments Vector par leur valeur d'index, par exemple en accédant à listOfPeople(123456789).
// MAGIC
// MAGIC En général, à la différence que :  
// MAGIC * Vector est indexé et List ne l'est pas
// MAGIC * List a la méthode ::  
// MAGIC
// MAGIC les deux types fonctionnent de la même manière, nous allons donc passer rapidement en revue les exemples suivants.
// MAGIC
// MAGIC Ce lien https://docs.scala-lang.org/overviews/collections-2.13/performance-characteristics.html  
// MAGIC passe en revue les différentes collection et leur temps d'access 
// MAGIC
// MAGIC Voici quelques façons de créer un vecteur :

// COMMAND ----------

val nums = Vector(1, 2, 3, 4, 5)

val strings = Vector("one", "two")

case class Person(name: String)
val people = Vector(
  Person("Bert"),
  Person("Ernie"),
  Person("Grover")
)

// COMMAND ----------

val a = Vector(1,2,3)         // Vector(1, 2, 3)
val b = a :+ 4                // Vector(1, 2, 3, 4)
val c = a ++ Vector(4, 5)     // Vector(1, 2, 3, 4, 5)

// COMMAND ----------

val a = Vector(1,2,3)         // Vector(1, 2, 3)
val b = 0 +: a                // Vector(0, 1, 2, 3)
val c = Vector(-1, 0) ++: a   // Vector(-1, 0, 1, 2, 3)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ArrayBuffer
// MAGIC
// MAGIC Utilisez ArrayBuffer lorsque vous avez besoin d'une séquence indexée mutable à usage général dans vos applications Scala.  
// MAGIC Il est mutable, vous pouvez donc modifier ses éléments et également le redimensionner.  
// MAGIC Parce qu’il est indexé, l’accès aléatoire aux éléments est rapide.
// MAGIC
// MAGIC Création d'un ArrayBuffer:  
// MAGIC
// MAGIC Pour utiliser un ArrayBuffer, importez-le d’abord :
// MAGIC

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer

// COMMAND ----------

// MAGIC %md
// MAGIC Si vous devez commencer avec un ArrayBuffer vide, spécifiez simplement son type :

// COMMAND ----------

var strings = ArrayBuffer[String]()
var ints = ArrayBuffer[Int]()
var people = ArrayBuffer[Person]()

// COMMAND ----------

// MAGIC %md
// MAGIC Si vous connaissez la taille approximative de votre ArrayBuffer, vous pouvez le créer avec une taille initiale :

// COMMAND ----------

val buf = new ArrayBuffer[Int](100000)

// COMMAND ----------

// MAGIC %md
// MAGIC Pour créer un nouveau ArrayBuffer avec des éléments initiaux, spécifiez simplement ses éléments initiaux, tout comme une liste ou un vecteur :

// COMMAND ----------

val nums = ArrayBuffer(1, 2, 3)
val people = ArrayBuffer(
  Person("Bert"),
  Person("Ernie"),
  Person("Grover")
)

// COMMAND ----------

// MAGIC %md
// MAGIC Ajout d'éléments à un ArrayBuffer
// MAGIC
// MAGIC Ajoutez de nouveaux éléments à un ArrayBuffer avec les méthodes += et ++=.  
// MAGIC Ou si vous préférez les méthodes avec des noms textuels, vous pouvez également utiliser append, appendAll, insert, insertAll, prepend et prependAll.  
// MAGIC
// MAGIC Voici quelques exemples de += et ++= :  

// COMMAND ----------

val nums = ArrayBuffer(1, 2, 3)   // ArrayBuffer(1, 2, 3)
nums += 4                         // ArrayBuffer(1, 2, 3, 4)
nums ++= List(5, 6)               // ArrayBuffer(1, 2, 3, 4, 5, 6)

// COMMAND ----------

// MAGIC %md
// MAGIC Supprimer des éléments d'un ArrayBuffer  
// MAGIC
// MAGIC ArrayBuffer est mutable, il a donc des méthodes telles que -=, --=, clear, delete, etc. Ces exemples illustrent les méthodes -= et --= :  

// COMMAND ----------

val a = ArrayBuffer.range('a', 'h')   // ArrayBuffer(a, b, c, d, e, f, g)
a -= 'a'                              // ArrayBuffer(b, c, d, e, f, g)
a --= Seq('b', 'c')                   // ArrayBuffer(d, e, f, g)
a --= Set('d', 'e')                   // ArrayBuffer(f, g)

// COMMAND ----------

// MAGIC %md
// MAGIC Mise à jour des éléments ArrayBuffer
// MAGIC
// MAGIC Mettez à jour les éléments dans un ArrayBuffer en réaffectant l'élément souhaité ou en utilisant la méthode update :

// COMMAND ----------

val a = ArrayBuffer.range(1,5)        // ArrayBuffer(1, 2, 3, 4)
a(2) = 50                             // ArrayBuffer(1, 2, 50, 4)
a.update(0, 10)                       // ArrayBuffer(10, 2, 50, 4)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Maps
// MAGIC
// MAGIC Une Map est une collection itérable composée de paires de clés et de valeurs.  
// MAGIC Scala a des types Map mutables et immuables, et cette section montre comment utiliser la Map immuable.  

// COMMAND ----------

// MAGIC %md
// MAGIC #### Créer une Map immuable
// MAGIC
// MAGIC Créez une Map immuable comme celle-ci : 

// COMMAND ----------

val states = Map(
  "AK" -> "Alaska",
  "AL" -> "Alabama",
  "AZ" -> "Arizona"
)

// COMMAND ----------

// MAGIC %md
// MAGIC Une fois que vous avez une Map, vous pouvez parcourir ses éléments dans une boucle for comme celle-ci :  

// COMMAND ----------

for ((k, v) <- states) println(s"key: $k, value: $v")

// Scala 3
// for (k, v) <- states do println(s"key: $k, value: $v")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Accéder aux éléments
// MAGIC
// MAGIC Accédez aux éléments en spécifiant la valeur de clé souhaitée entre parenthèses :

// COMMAND ----------

val ak = states("AK")   // ak: String = Alaska
val al = states("AL")   // al: String = Alabama

val ak2 = states.getOrElse("AK", "key not found")   // ak: String = Alaska
val al2 = states.getOrElse("AL", "key not found")   // al: String = Alabama
val nf = states.getOrElse("CA", "key not found") // String = key not found

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ajouter des éléments
// MAGIC
// MAGIC Ajoutez des éléments en utilisant + et ++, en n'oubliant pas d'attribuer le résultat à une nouvelle variable  

// COMMAND ----------

val a = Map(1 -> "one")    // a: Map(1 -> one)
val b = a + (2 -> "two")   // b: Map(1 -> one, 2 -> two)
val c = b ++ Seq(
  3 -> "three",
  4 -> "four"
)
// c: Map(1 -> one, 2 -> two, 3 -> three, 4 -> four)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Supprimer des éléments  
// MAGIC
// MAGIC Supprimez des éléments en utilisant - ou -- et les valeurs clés à supprimer, en n'oubliant pas d'attribuer le résultat à une nouvelle variable :  

// COMMAND ----------

val a = Map(
  1 -> "one",
  2 -> "two",
  3 -> "three",
  4 -> "four"
)

val b = a - 4       // b: Map(1 -> one, 2 -> two, 3 -> three)
val c = a - 4 - 3   // c: Map(1 -> one, 2 -> two)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Mise à jour des éléments
// MAGIC
// MAGIC Pour mettre à jour des éléments, utilisez la méthode mise à jour (ou l'opérateur +) tout en affectant le résultat à une nouvelle variable :  

// COMMAND ----------

val a = Map(
  1 -> "one",
  2 -> "two",
  3 -> "three"
)

val b = a.updated(3, "THREE!")   // b: Map(1 -> one, 2 -> two, 3 -> THREE!)
val c = a + (2 -> "TWO...")      // c: Map(1 -> one, 2 -> TWO..., 3 -> three)

// COMMAND ----------

// MAGIC %md
// MAGIC Parcourir une Map
// MAGIC
// MAGIC Comme indiqué précédemment, il s'agit d'une manière courante de parcourir manuellement des éléments à l'aide d'une boucle for :  

// COMMAND ----------

val states = Map(
  "AK" -> "Alaska",
  "AL" -> "Alabama",
  "AZ" -> "Arizona"
)

for ((k, v) <- states) println(s"key: $k, value: $v")

// Scala 3
//for (k, v) <- states do println(s"key: $k, value: $v")

// COMMAND ----------

// MAGIC %md
// MAGIC # Travailler avec des ensembles (Set)
// MAGIC
// MAGIC Le Scala Set est une collection itérable sans éléments en double.
// MAGIC
// MAGIC Scala a des types Set mutables et immuables. Cette section montre l'ensemble immuable.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Créer un ensemble
// MAGIC
// MAGIC Créez de nouveaux ensembles vides comme ceci :  

// COMMAND ----------

val nums = Set[Int]()
val letters = Set[Char]()

// COMMAND ----------

// MAGIC %md
// MAGIC Créez des ensembles avec des données initiales comme ceci :

// COMMAND ----------

val nums = Set(1, 2, 3, 3, 3)           // Set(1, 2, 3)
val letters = Set('a', 'b', 'c', 'c')   // Set('a', 'b', 'c')

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ajouter des éléments à un ensemble
// MAGIC
// MAGIC Ajoutez des éléments à un Set immuable en utilisant + et ++, en n'oubliant pas d'attribuer le résultat à une nouvelle variable :

// COMMAND ----------

val a = Set(1, 2)                // Set(1, 2)
val b = a + 3                    // Set(1, 2, 3)
val c = b ++ Seq(4, 1, 5, 5)     // HashSet(5, 1, 2, 3, 4)

// COMMAND ----------

// MAGIC %md
// MAGIC Notez que lorsque vous essayez d’ajouter des éléments en double, ils sont discrètement supprimés.  
// MAGIC
// MAGIC Notez également que l'ordre d'itération des éléments est arbitraire.  

// COMMAND ----------

// MAGIC %md
// MAGIC Supprimer des éléments d'un ensemble  
// MAGIC
// MAGIC Supprimez des éléments d'un ensemble immuable en utilisant - et --, en attribuant à nouveau le résultat à une nouvelle variable :  

// COMMAND ----------

val a = Set(1, 2, 3, 4, 5)   // HashSet(5, 1, 2, 3, 4)
val b = a - 5                // HashSet(1, 2, 3, 4)
val c = b -- Seq(3, 4)       // HashSet(1, 2)

// COMMAND ----------

// MAGIC %md
// MAGIC # Range
// MAGIC
// MAGIC Scala range est souvent utilisée pour remplir des structures de données et parcourir des boucles.  
// MAGIC Ces exemples REPL montrent comment les créer:

// COMMAND ----------

1 to 5         // Range(1, 2, 3, 4, 5)
1 until 5      // Range(1, 2, 3, 4)
1 to 10 by 2   // Range(1, 3, 5, 7, 9)
'a' to 'c'     // NumericRange(a, b, c)

// COMMAND ----------

// MAGIC %md
// MAGIC Vous pouvez utiliser Range pour remplir les collections :

// COMMAND ----------

val x = (1 to 5).toList     // List(1, 2, 3, 4, 5)
val y = (1 to 5).toBuffer   // ArrayBuffer(1, 2, 3, 4, 5)

// COMMAND ----------

for (i <- 1 to 3) println(i)

// Scala 3
// for i <- 1 to 3 do println(i)

// COMMAND ----------

Vector.range(1, 5)       // Vector(1, 2, 3, 4)
List.range(1, 10, 2)     // List(1, 3, 5, 7, 9)
// Does not work
//Set.range(1, 10)         // HashSet(5, 1, 6, 9, 2, 7, 3, 8, 4)

// COMMAND ----------

// MAGIC %md
// MAGIC Lorsque vous exécutez des tests, les Range sont également utiles pour générer des collections de tests :  

// COMMAND ----------

val evens = (0 to 10 by 2).toList     // List(0, 2, 4, 6, 8, 10)
val odds = (1 to 10 by 2).toList      // List(1, 3, 5, 7, 9)
val doubles = (1 to 5).map(_ * 2.0)   // Vector(2.0, 4.0, 6.0, 8.0, 10.0)

// create a Map
val map = (1 to 3).map(e => (e,s"$e")).toMap
    // map: Map[Int, String] = Map(1 -> "1", 2 -> "2", 3 -> "3")

// COMMAND ----------

// MAGIC %md
// MAGIC # Méthodes de bibliothèque standard  
// MAGIC
// MAGIC Vous aurez rarement besoin d'écrire à nouveau une boucle for personnalisée, car les dizaines de méthodes fonctionnelles prédéfinies dans la bibliothèque standard Scala vous feront gagner du temps et contribueront à rendre le code plus cohérent entre différentes applications.   
// MAGIC
// MAGIC Les exemples suivants montrent certaines des méthodes de collections intégrées, et il en existe de nombreuses autres.  
// MAGIC Bien que celles-ci utilisent toutes la classe List, les mêmes méthodes fonctionnent avec d'autres classes de collections telles que Seq, Vector, LazyList, Set, Map, Array et ArrayBuffer.  
// MAGIC
// MAGIC Voici quelques exemples:  

// COMMAND ----------

List.range(1, 3)                          // List(1, 2)
List.range(start = 1, end = 6, step = 2)  // List(1, 3, 5)
List.fill(3)("foo")                       // List(foo, foo, foo)
List.tabulate(3)(n => n * n)              // List(0, 1, 4)
List.tabulate(4)(n => n * n)              // List(0, 1, 4, 9)

val a = List(10, 20, 30, 40, 10)          // List(10, 20, 30, 40, 10)
a.distinct                                // List(10, 20, 30, 40)
a.drop(2)                                 // List(30, 40, 10)
a.dropRight(2)                            // List(10, 20, 30)
a.dropWhile(_ < 25)                       // List(30, 40, 10)
a.filter(_ < 25)                          // List(10, 20, 10)
a.filter(_ > 100)                         // List()
a.find(_ > 20)                            // Some(30)
a.head                                    // 10
a.headOption                              // Some(10)
a.init                                    // List(10, 20, 30, 40)
a.intersect(List(19,20,21))               // List(20)
a.last                                    // 10
a.lastOption                              // Some(10)
a.map(_ * 2)                              // List(20, 40, 60, 80, 20)
a.slice(2, 4)                             // List(30, 40)
a.tail                                    // List(20, 30, 40, 10)
a.take(3)                                 // List(10, 20, 30)
a.takeRight(2)                            // List(40, 10)
a.takeWhile(_ < 30)                       // List(10, 20)
a.filter(_ < 30).map(_ * 10)              // List(100, 200, 100)

val fruits = List("apple", "pear")
fruits.map(_.toUpperCase)                 // List(APPLE, PEAR)
fruits.flatMap(_.toUpperCase)             // List(A, P, P, L, E, P, E, A, R)

val nums = List(10, 5, 8, 1, 7)
nums.sorted                               // List(1, 5, 7, 8, 10)
nums.sortWith(_ < _)                      // List(1, 5, 7, 8, 10)
nums.sortWith(_ > _)                      // List(10, 8, 7, 5, 1)

// COMMAND ----------

// MAGIC %md
// MAGIC Bonnes pratiques 
// MAGIC
// MAGIC Les idiomes Scala encouragent les meilleures pratiques de plusieurs manières.  
// MAGIC Pour l'immuabilité, créez des déclarations val immuables :  

// COMMAND ----------

val a = 1                 // immutable variable

// COMMAND ----------

// MAGIC %md
// MAGIC Vous êtes également encouragé à utiliser des classes de collections immuables telles que List et Map :  

// COMMAND ----------

val b = List(1,2,3)       // List is immutable
val c = Map(1 -> "one")   // Map is immutable

// COMMAND ----------

def printIt(a: Any): Unit = println(a)

printIt("Hello")
printIt(3)
printIt(List(1,2,3))

// COMMAND ----------

// MAGIC %md
// MAGIC Autres exemples

// COMMAND ----------

List(1,2,3).foreach(println)

val ls = List(1,2,3) ::: List(4,5,6)

val ls2 = List(4,5,6) ::: List(1,2,3)

val ls3 = List(1,2,3) :: 0 :: Nil

val ls4 = 20 :: 10 :: 0 :: Nil

val contains2 = List(1,2,3).contains(2)

val are_all_ints = List(1,2,3).forall(i => i.isInstanceOf[Int])

val are_all_ints2 = List(1,2.0,3).forall(i => i.isInstanceOf[Int])

val is_there = List(1,2,3).exists(i => i == 3)

val is_there2 = List(1,2,3).exists(i => i == 4)
