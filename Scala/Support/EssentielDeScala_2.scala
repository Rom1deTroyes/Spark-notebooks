// Databricks notebook source
// MAGIC %md
// MAGIC Définir une classe  
// MAGIC
// MAGIC Une définition de classe minimale est simplement la classe de mot-clé et un identifiant.  
// MAGIC
// MAGIC Les noms de classe doivent être en majuscules.  

// COMMAND ----------

class User

val user1 = new User

// COMMAND ----------

// MAGIC %md
// MAGIC Le mot-clé new est utilisé pour créer une instance de la classe.  
// MAGIC
// MAGIC Nous appelons la classe comme une fonction, comme User(), pour créer une instance de la classe.  
// MAGIC
// MAGIC User dispose d'un constructeur par défaut qui ne prend aucun argument car aucun constructeur n'a été défini.  
// MAGIC
// MAGIC Cependant, vous aurez souvent besoin d’un constructeur et d’un corps de classe. Voici un exemple de définition de classe pour un point :  

// COMMAND ----------

class Point(var x: Int, var y: Int) {
  def move(dx: Int, dy: Int): Unit = {
    x = x + dx
    y = y + dy
  }

  override def toString: String =
    s"($x, $y)"
}

val point1 = new Point(2, 3)
println(point1.x)  // prints 2
println(point1)    // prints (2, 3)

// COMMAND ----------

// MAGIC %md
// MAGIC Cette classe Point comporte quatre membres : les variables x et y et les méthodes move et toString.  
// MAGIC
// MAGIC Contrairement à de nombreux autres langages, le constructeur principal se trouve dans la signature de classe (var x : Int, var y : Int).  
// MAGIC
// MAGIC La méthode move prend deux arguments entiers et renvoie la valeur Unit (), qui ne contient aucune information.  
// MAGIC
// MAGIC Cela correspond à peu près à void dans les langages de type Java.  
// MAGIC
// MAGIC toString, en revanche, ne prend aucun argument mais renvoie une valeur String.  
// MAGIC
// MAGIC Puisque toString remplace toString depuis AnyRef, il est étiqueté avec le mot-clé override.

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC Constructeurs
// MAGIC
// MAGIC Les constructeurs peuvent avoir des paramètres facultatifs en fournissant une valeur par défaut comme ceci :  

// COMMAND ----------

class Point(var x: Int = 0, var y: Int = 0) {
  override def toString: String =
    s"($x, $y)"
}

val origin = new Point    // x and y are both set to 0
val point1 = new Point(1) // x is set to 1 and y is set to 0
println(point1)           // prints (1, 0)

// COMMAND ----------

// MAGIC %md
// MAGIC Dans cette version de la classe Point, x et y ont la valeur par défaut 0 donc aucun argument n'est requis.  
// MAGIC
// MAGIC Cependant, comme le constructeur lit les arguments de gauche à droite, si vous souhaitez simplement transmettre une valeur y, vous devrez nommer le paramètre.  

// COMMAND ----------

class Point(var x: Int = 0, var y: Int = 0) {
  override def toString: String =
    s"($x, $y)"
}
val point2 = new Point(y = 2)
println(point2)               // prints (0, 2)

// COMMAND ----------

// MAGIC %md
// MAGIC Membres privés et syntaxe Getter/Setter  
// MAGIC
// MAGIC Les membres sont publics par défaut.  
// MAGIC
// MAGIC Utilisez le modificateur d'accès private pour les masquer de l'extérieur de la classe.  

// COMMAND ----------

class newPoint {
  private var _x = 0
  private var _y = 0
  private val bound = 100

  def x: Int = _x
  // x_= sans espace
  def x_= (newValue: Int): Unit = {
    if (newValue < bound)
      _x = newValue
    else
      printWarning()
  }

  def y: Int = _y
  def y_= (newValue: Int): Unit = {
    if (newValue < bound)
      _y = newValue
    else
      printWarning()
  }

  private def printWarning(): Unit =
    println("WARNING: Out of bounds")

  override def toString: String =
    s"($x, $y)"
}

val point1 = new newPoint()
point1.x = 99
point1.y = 101 // prints the warning

// COMMAND ----------

// MAGIC %md
// MAGIC Dans cette version de la classe Point, les données sont stockées dans des variables privées _x et _y.  
// MAGIC
// MAGIC Il existe des méthodes def x et def y pour accéder aux données privées.  
// MAGIC
// MAGIC def x_= et def y_= servent à valider et à définir la valeur de _x et _y.  
// MAGIC
// MAGIC Notez la syntaxe spéciale pour les setters : la méthode a _= ajouté à l'identifiant du getter et les paramètres viennent après.  
// MAGIC
// MAGIC Les paramètres sans val ou var sont des valeurs privées, visibles uniquement au sein de la classe.  

// COMMAND ----------

// MAGIC %md
// MAGIC Voici une autre possibilité; les méthodes s'appellent get_x et get_y et pas x et y  
// MAGIC On ne peut pas faire point1.x = comme en haut

// COMMAND ----------

class newPoint {
  private var x = 0
  private var y = 0
  private val bound = 100

  def set_x(newValue: Int): Unit = {
    if (newValue < bound)
      x = newValue
    else
      printWarning()
  }

  def set_y(newValue: Int): Unit = {
    if (newValue < bound)
      y = newValue
    else
      printWarning()
  }

  private def printWarning(): Unit =
    println("WARNING: Out of bounds")

  override def toString: String =
    s"($x, $y)"
}

val point1 = new newPoint()
point1.set_x(99)
point1.set_y(101) // prints the warning

// COMMAND ----------

// MAGIC %md
// MAGIC Case classes  
// MAGIC Elles sont comme des classes ordinaires avec quelques différences clés que nous passerons en revue.  
// MAGIC
// MAGIC Les classes de cas sont utiles pour modéliser des données immuables.  
// MAGIC
// MAGIC Définir case class:  
// MAGIC
// MAGIC Une case class minimale nécessite les mots-clés case class, un identifiant et une liste de paramètres (qui peuvent être vides) :

// COMMAND ----------

case class Book(isbn: String)

val frankenstein = Book("978-0486282114")  // Notez: pas de mot-clé "new"

// COMMAND ----------

// MAGIC %md
// MAGIC Une case class "final" ne peut pas avoir sousclasses

// COMMAND ----------

final case class Book(isbn: String)

val frankenstein = Book("978-0486282114")  // Notez: pas de mot-clé "new"

// COMMAND ----------

// MAGIC %md
// MAGIC Bien que cela soit généralement laissé de côté, il est possible d'utiliser explicitement le mot-clé new, comme new Book().  
// MAGIC
// MAGIC En effet, les case class ont par défaut une méthode apply qui prend en charge la construction des objets.  
// MAGIC
// MAGIC Lorsque vous créez une classe de cas avec des paramètres, les paramètres sont des public val.  
// MAGIC
// MAGIC Vous ne pouvez pas réaffecter message1.sender car il s'agit d'un val (c'est-à-dire immuable).  
// MAGIC
// MAGIC Il est possible d'utiliser des var dans case class mais cela est déconseillé  

// COMMAND ----------

case class Message(sender: String, recipient: String, body: String)
val message1 = Message("guillaume@quebec.ca", "jorge@catalonia.es", "Ça va ?")

println(message1.sender)  // prints guillaume@quebec.ca
// message1.sender = "travis@washington.us"  // this line does not compile

// COMMAND ----------

// MAGIC %md
// MAGIC Les instances de classes de cas sont comparées par structure et non par référence  
// MAGIC
// MAGIC Même si message2 et message3 font référence à des objets différents, la valeur de chaque objet est égale.  

// COMMAND ----------

case class Message(sender: String, recipient: String, body: String)

val message2 = Message("jorge@catalonia.es", "guillaume@quebec.ca", "Com va?")
val message3 = Message("jorge@catalonia.es", "guillaume@quebec.ca", "Com va?")
val messagesAreTheSame = message2 == message3  // true

// COMMAND ----------

// MAGIC %md
// MAGIC Un objet est une classe qui possède exactement une instance.  
// MAGIC
// MAGIC Il est créé paresseusement lorsqu'il est référencé  
// MAGIC
// MAGIC En tant que valeur de niveau supérieur, un objet est un singleton.
// MAGIC
// MAGIC En tant que membre d'une classe englobante ou en tant que valeur locale, il se comporte exactement comme un val paresseux.  
// MAGIC
// MAGIC Définir un objet singleton
// MAGIC
// MAGIC Un objet est une valeur. La définition d'un objet ressemble à une classe, mais utilise le mot-clé object :  

// COMMAND ----------

object Box

// COMMAND ----------

package logging

object Logger {
  def info(message: String): Unit = println(s"INFO: $message")
}

// COMMAND ----------

// MAGIC %md
// MAGIC La méthode info peut être importée de n'importe où dans le programme.  
// MAGIC
// MAGIC La création de méthodes utilitaires comme celle-ci est un cas d'utilisation courant pour les objets singleton.  
// MAGIC
// MAGIC La méthode info est visible grâce à l'instruction d'importation, import logging. Logger.info  

// COMMAND ----------

import logging.Logger.info

class Project(name: String, daysToComplete: Int)

class Test {
  val project1 = new Project("TPS Reports", 1)
  val project2 = new Project("Website redesign", 5)
  info("Created projects")  // Prints "INFO: Created projects"
}

// COMMAND ----------

// MAGIC %md
// MAGIC Generic classes   
// MAGIC
// MAGIC Les classes génériques sont des classes qui prennent un type comme paramètre.  
// MAGIC
// MAGIC Elles sont particulièrement utiles pour parcourir une collection.  
// MAGIC
// MAGIC Les classes génériques prennent un type comme paramètre entre crochets [].  
// MAGIC
// MAGIC Une convention consiste à utiliser la lettre A comme identifiant de paramètre de type, bien que n'importe quel nom de paramètre puisse être utilisé.  

// COMMAND ----------

class Stack[A] {
  private var elements: List[A] = Nil
  def push(x: A): Unit =
    elements = x :: elements
  def peek: A = elements.head
  def pop(): A = {
    val currentTop = peek
    elements = elements.tail
    currentTop
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Cette implémentation d'une classe Stack prend n'importe quel type A comme paramètre.  
// MAGIC
// MAGIC Cela signifie que la liste sous-jacente, var elements : List[A] = Nil, ne peut stocker que des éléments de type A.  
// MAGIC
// MAGIC La procédure def push n'accepte que les objets de type A (remarque : 'elements = x :: elements' réaffecte les éléments à une nouvelle liste créée en ajoutant x aux éléments actuels).  
// MAGIC
// MAGIC Nil est ici une liste vide et ne doit pas être confondu avec null.  
// MAGIC
// MAGIC Pour utiliser une classe générique, mettez le type entre crochets à la place de A.  
// MAGIC
// MAGIC

// COMMAND ----------

val stack = new Stack[Int]
stack.push(1)
stack.push(2)
println(stack.pop())  // prints 2
println(stack.pop())  // prints 1

// COMMAND ----------

// MAGIC %md
// MAGIC cette instance de stack ne peut prendre que des Ints.  
// MAGIC
// MAGIC Cependant, si l'argument type avait des sous-types, ceux-ci pourraient être transmis :

// COMMAND ----------

class Fruit
class Apple extends Fruit
class Banana extends Fruit

val stack = new Stack[Fruit]
val apple = new Apple
val banana = new Banana

stack.push(apple)
stack.push(banana)

// COMMAND ----------

// MAGIC %md
// MAGIC Les méthodes de Scala peuvent être paramétrées par type ainsi que par valeur.  
// MAGIC
// MAGIC La syntaxe est similaire à celle des classes génériques.  
// MAGIC
// MAGIC Les paramètres de type sont placés entre crochets, tandis que les paramètres de valeur sont placés entre parenthèses.  
// MAGIC
// MAGIC Voici un exemple:  

// COMMAND ----------

def listOfDuplicates[A](x: A, length: Int): List[A] = {
  if (length < 1)
    Nil
  else
    x :: listOfDuplicates(x, length - 1)
}
println(listOfDuplicates[Int](3, 4))  // List(3, 3, 3, 3)
println(listOfDuplicates("La", 8))  // List(La, La, La, La, La, La, La, La)

// COMMAND ----------

// MAGIC %md
// MAGIC La méthode listOfDuplicates prend un paramètre de type A et des paramètres de valeur x et length.  
// MAGIC
// MAGIC La valeur x est de type A.  
// MAGIC
// MAGIC Si longueur < 1 nous renvoyons une liste vide.  
// MAGIC
// MAGIC Sinon, nous ajoutons x à la liste des doublons renvoyée par l'appel récursif.  
// MAGIC (Notez que :: signifie ajouter un élément à gauche à une liste à droite.)
// MAGIC
// MAGIC Dans le premier exemple d'appel, nous fournissons explicitement le paramètre type en écrivant [Int].  
// MAGIC
// MAGIC Par conséquent, le premier argument doit être un Int et le type de retour sera un List[Int].  
// MAGIC
// MAGIC Le deuxième exemple d’appel montre que vous n’avez pas toujours besoin de fournir explicitement le paramètre type.  
// MAGIC
// MAGIC Le compilateur peut souvent le déduire en fonction du contexte ou des types d'arguments de valeur.  
// MAGIC
// MAGIC Dans cet exemple, "La" est une chaîne donc le compilateur sait que A doit être une chaîne.

// COMMAND ----------

// MAGIC %md
// MAGIC Omettre le type  
// MAGIC
// MAGIC Le compilateur Scala peut souvent déduire le type d’une expression afin que vous n’ayez pas à la déclarer explicitement.

// COMMAND ----------

val businessName = "Montreux Jazz Café"

def squareOf(x: Int) = x * x

// COMMAND ----------

// MAGIC %md
// MAGIC Le compilateur peut déduire que le type de retour est un Int, donc aucun type de retour explicite n'est requis.  
// MAGIC
// MAGIC Pour les méthodes récursives, le compilateur n'est pas capable de déduire un type de résultat.  
// MAGIC
// MAGIC Voici un programme qui fera échouer le compilateur pour cette raison :  

// COMMAND ----------

//def fac(n: Int) = if (n == 0) 1 else n * fac(n - 1) // Pas bon

def fac(n: Int): Int = if (n == 0) 1 else n * fac(n - 1) // Ceci est bon

// COMMAND ----------

// MAGIC %md
// MAGIC Il n'est pas non plus obligatoire de spécifier des paramètres de type lorsque des méthodes polymorphes sont appelées ou que des classes génériques sont instanciées.  
// MAGIC
// MAGIC Le compilateur Scala déduira ces paramètres de type manquants à partir du contexte et des types des paramètres réels de la méthode/du constructeur.  

// COMMAND ----------

case class MyPair[A, B](x: A, y: B)
val p = MyPair(1, "scala") // type: MyPair[Int, String]

def id[T](x: T) = x
val q = id(1)              // type: Int

// COMMAND ----------

// MAGIC %md
// MAGIC Le compilateur ne déduit jamais les types de paramètres de méthode.  
// MAGIC
// MAGIC Cependant, dans certains cas, il peut déduire des types de paramètres de fonction anonymes lorsque la fonction est passée en argument.  
// MAGIC
// MAGIC Le paramètre de map est f : A => B.  
// MAGIC
// MAGIC Parce que nous mettons des entiers dans Seq, le compilateur sait que A est Int (c'est-à-dire que x est un entier).  
// MAGIC
// MAGIC Par conséquent, le compilateur peut déduire de x * 2 que B est de type Int.

// COMMAND ----------

Seq(1, 3, 4).map(x => x * 2)  // List(2, 6, 8)

// COMMAND ----------

// MAGIC %md
// MAGIC Quand ne pas utiliser l’inférence de type
// MAGIC
// MAGIC Il est généralement considéré comme plus lisible de déclarer le type de membres exposés dans une API publique.  
// MAGIC
// MAGIC Par conséquent, il est recommandé de rendre le type explicite pour toutes les API qui seront exposées aux utilisateurs de votre code.

// COMMAND ----------

// MAGIC %md
// MAGIC Gestion des exceptions

// COMMAND ----------

import java.io._

import scala.io.Source

def openAndReadAFile(filename: String): String = {
  val bufferedSource = Source.fromFile(filename)
  val lines = bufferedSource.getLines.toList.mkString(" ")
  bufferedSource.close()
  lines
}

var text = ""
try {
  text = openAndReadAFile("foo.txt")
} catch {
  case fnf: FileNotFoundException => fnf.printStackTrace()
  case ioe: IOException => ioe.printStackTrace()
} finally {
  println("Came to the 'finally' clause.")
}

// COMMAND ----------

// MAGIC %md
// MAGIC Programmation fonctionnelle

// COMMAND ----------

// MAGIC %md
// MAGIC Les fonctions d'ordre supérieur prennent d'autres fonctions comme paramètres ou renvoient une fonction en conséquence. Cela est possible car les fonctions sont des valeurs de première classe dans Scala.  
// MAGIC
// MAGIC La terminologie peut devenir un peu confuse à ce stade, et nous utilisons l'expression « fonction d'ordre supérieur » pour les méthodes et les fonctions qui prennent des fonctions comme paramètres ou qui renvoient une fonction.  
// MAGIC
// MAGIC Dans un monde purement orienté objet, une bonne pratique consiste à éviter d’exposer des méthodes paramétrées avec des fonctions qui pourraient divulguer l’état interne de l’objet.  
// MAGIC
// MAGIC Une fuite d'état interne pourrait briser les invariants de l'objet lui-même, violant ainsi l'encapsulation.
// MAGIC
// MAGIC L'un des exemples les plus courants est map, disponible pour les collections dans Scala.  
// MAGIC
// MAGIC Fonctions qui acceptent des fonctions
// MAGIC
// MAGIC L’une des raisons d’utiliser des fonctions d’ordre supérieur est de réduire le code redondant.  
// MAGIC
// MAGIC Disons que vous souhaitiez des méthodes permettant d’augmenter les salaires d’une personne selon divers facteurs.  
// MAGIC
// MAGIC Sans créer une fonction d’ordre supérieur, cela pourrait ressembler à ceci :

// COMMAND ----------

object SalaryRaiser {

  def smallPromotion(salaries: List[Double]): List[Double] =
    salaries.map(salary => salary * 1.1)

  def greatPromotion(salaries: List[Double]): List[Double] =
    salaries.map(salary => salary * math.log(salary))

  def hugePromotion(salaries: List[Double]): List[Double] =
    salaries.map(salary => salary * salary)
}

// COMMAND ----------

// MAGIC %md
// MAGIC Remarquez comment chacune des trois méthodes varie uniquement en fonction du facteur de multiplication. Pour simplifier, vous pouvez extraire le code répété dans une fonction d'ordre supérieur comme ceci :  

// COMMAND ----------

object SalaryRaiser {

  private def promotion(salaries: List[Double], promotionFunction: Double => Double): List[Double] =
    salaries.map(promotionFunction)

  def smallPromotion(salaries: List[Double]): List[Double] =
    promotion(salaries, salary => salary * 1.1)

  def greatPromotion(salaries: List[Double]): List[Double] =
    promotion(salaries, salary => salary * math.log(salary))

  def hugePromotion(salaries: List[Double]): List[Double] =
    promotion(salaries, salary => salary * salary)
}

// COMMAND ----------

// MAGIC %md
// MAGIC La nouvelle méthode, promotion, prend les salaires plus une fonction de type Double => Double (c'est-à-dire une fonction qui prend un Double et renvoie un Double) et renvoie le produit.  
// MAGIC
// MAGIC Les méthodes et les fonctions expriment généralement des comportements ou des transformations de données. Par conséquent, le fait d'avoir des fonctions qui composent en fonction d'autres fonctions peut aider à créer des mécanismes génériques. Ces opérations génériques reportent le verrouillage de l'ensemble du comportement de l'opération, offrant ainsi aux clients un moyen de contrôler ou de personnaliser davantage certaines parties de l'opération elle-même.  

// COMMAND ----------

// MAGIC %md
// MAGIC Fonctions qui renvoient des fonctions  
// MAGIC
// MAGIC Il existe certains cas où vous souhaitez générer une fonction.  
// MAGIC
// MAGIC Voici un exemple de méthode qui renvoie une fonction.

// COMMAND ----------

def urlBuilder(ssl: Boolean, domainName: String): (String, String) => String = {
  val schema = if (ssl) "https://" else "http://"
  (endpoint: String, query: String) => s"$schema$domainName/$endpoint?$query"
}

val domainName = "www.example.com"
def getURL = urlBuilder(ssl=true, domainName)
val endpoint = "users"
val query = "id=1"
val url = getURL(endpoint, query) // "https://www.example.com/users?id=1": String

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------


