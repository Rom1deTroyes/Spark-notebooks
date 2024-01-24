// Databricks notebook source
// MAGIC %md
// MAGIC Écrivez un programme Scala qui crée une classe appelée Person avec des propriétés telles que le nom, l'âge et le pays.  
// MAGIC
// MAGIC Implémentez des méthodes pour obtenir et définir des propriétés.  

// COMMAND ----------

class Person(var name: String, var age: Int, var country: String) {
  def getName: String = name

  def setName(newName: String): Unit = {
    name = newName
  }

  def getAge: Int = age

  def setAge(newAge: Int): Unit = {
    age = newAge
  }

  def getCountry: String = country

  def setCountry(newCountry: String): Unit = {
    country = newCountry
  }
}

object PersonApp {
  def main(args: Array[String]): Unit = {
    val person = new Person("Andrey Ira", 35, "France")

    println("Original Person:")
    println(s"Name: ${person.getName}")
    println(s"Age: ${person.getAge}")
    println(s"Country: ${person.getCountry}")

    person.setName("Lior Daniela")
    person.setAge(30)
    person.setCountry("Canada")

    println("\nUpdated Person:")
    println(s"Name: ${person.getName}")
    println(s"Age: ${person.getAge}")
    println(s"Country: ${person.getCountry}")
  }
}

PersonApp.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala qui crée une sous-classe Student qui étend la classe Person.  
// MAGIC
// MAGIC Ajoutez une propriété appelée grade et implémentez des méthodes pour l'obtenir et la définir.  

// COMMAND ----------

class Person(var name: String, var age: Int, var country: String) {
  def getName: String = name

  def setName(newName: String): Unit = {
    name = newName
  }

  def getAge: Int = age

  def setAge(newAge: Int): Unit = {
    age = newAge
  }

  def getCountry: String = country

  def setCountry(newCountry: String): Unit = {
    country = newCountry
  }
}

class Student(name: String, age: Int, country: String, var grade: String)
  extends Person(name, age, country) {

  def getGrade: String = grade

  def setGrade(newGrade: String): Unit = {
    grade = newGrade
  }
}

object StudentApp {
  def main(args: Array[String]): Unit = {
    val student = new Student("Saturnino Nihad", 18, "USA", "A")

    println("Original Student:")
    println(s"Name: ${student.getName}")
    println(s"Age: ${student.getAge}")
    println(s"Country: ${student.getCountry}")
    println(s"Grade: ${student.getGrade}")

    student.setName("Albino Ellen")
    student.setAge(20)
    student.setCountry("Canada")
    student.setGrade("B")

    println("\nUpdated Student:")
    println(s"Name: ${student.getName}")
    println(s"Age: ${student.getAge}")
    println(s"Country: ${student.getCountry}")
    println(s"Grade: ${student.getGrade}")
  }
}
  
  
StudentApp.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala qui crée une classe abstraite Shape avec une zone de méthode abstraite.  
// MAGIC
// MAGIC Implémentez les sous-classes Rectangle et Circle qui remplacent la méthode Area.  

// COMMAND ----------

abstract class Shape {
  def area: Double
}

class Rectangle(width: Double, height: Double) extends Shape {
  override def area: Double = width * height
}

class Circle(radius: Double) extends Shape {
  override def area: Double = math.Pi * radius * radius
}

object ShapeApp {
  def main(args: Array[String]): Unit = {
    val rectangle = new Rectangle(7, 5)
    println(s"Rectangle Area: ${rectangle.area}")

    val circle = new Circle(4.5)
    println(s"Circle Area: ${circle.area}")
  }
}

ShapeApp.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala qui crée une classe BankAccount avec les propriétés accountNumber et balance.  
// MAGIC Mettez en œuvre des méthodes pour déposer et retirer de l’argent du compte.  

// COMMAND ----------

class BankAccount(val accountNumber: String, var balance: Double) {
  def deposit(amount: Double): Unit = {
    balance += amount
    println(s"Deposited $amount. New balance: $balance")
  }

  def withdraw(amount: Double): Unit = {
    if (amount <= balance) {
      balance -= amount
      println(s"Withdrew $amount. New balance: $balance")
    } 
    else 
    {
      println(s"Want to withdraw $amount? Insufficient balance!")
    }
  }
}

object BankAccountApp {
  def main(args: Array[String]): Unit = {
    val account = new BankAccount("SB-1234", 1000.0)

    println(s"Account Number: ${account.accountNumber}")
    println(s"Initial Balance: ${account.balance}")

    account.deposit(500.0)
    account.withdraw(200.0)
    account.withdraw(2000.0)
  }
}

BankAccountApp.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala qui crée une classe Employee avec des propriétés telles que le nom, l'âge et la désignation.   
// MAGIC
// MAGIC Implémentez une méthode pour afficher les détails des employés.

// COMMAND ----------

class Employee(val name: String, val age: Int, val designation: String) {
  def displayDetails(): Unit = {
    println("Employee Details:")
    println(s"Name: $name")
    println(s"Age: $age")
    println(s"Designation: $designation")
  }
}

object EmployeeApp {
  def main(args: Array[String]): Unit = {
    val employee = new Employee("Pero Janae", 30, "Sales Manager")
    employee.displayDetails()
  }
}

EmployeeApp.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala qui crée un trait Resizabale
// MAGIC  avec une méthode de redimensionnement qui modifie la taille d'un objet.  
// MAGIC Implémentez une classe Rectangle qui étend le trait  

// COMMAND ----------

trait Resizable {
  def resize(newSize: Int): Unit
}

class Rectangle(var width: Int, var height: Int) extends Resizable {
  override def resize(newSize: Int): Unit = {
    width = newSize
    height = newSize
  }

  override def toString: String = s"Rectangle (width: $width, height: $height)"
}

object ResizableApp {
  def main(args: Array[String]): Unit = {
    val rectangle = new Rectangle(7, 4)
    println(rectangle) // Rectangle (width: 7, height: 4)

    rectangle.resize(12)
    println(rectangle) // Rectangle (width: 12, height: 12)
  }
}

ResizableApp.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala qui crée une classe enum Color avec des valeurs pour différentes couleurs.  
// MAGIC
// MAGIC Utilisez la classe enum pour représenter la couleur d'un objet.  

// COMMAND ----------

sealed trait Color
case object Red extends Color
case object Green extends Color
case object Blue extends Color
case object Orange extends Color

object ColorApp {
  def main(args: Array[String]): Unit = {
    val myColor: Color = Red
    //val myColor: Color = Blue
    printColor(myColor)
  }

  def printColor(color: Color): Unit = color match {
    case Red   => println("The color is Red.")
    case Green => println("The color is Green.")
    case Blue  => println("The color is Blue.")
    case Orange  => println("The color is Orange.")
    case _     => println("Unknown color.")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala qui crée une classe ContactInfo avec le nom, l'adresse e-mail et l'adresse des propriétés. Créez une classe Customer qui inclut un objet ContactInfo. 

// COMMAND ----------

class ContactInfo(val name: String, val email: String, val address: String)

class Customer(val contactInfo: ContactInfo)

object CustomerApp {
  def main(args: Array[String]): Unit = {
    val contact = new ContactInfo("Serafim Eline", "serafim@example.com", "11 Open Street")
    val customer = new Customer(contact)
    
    println(s"Customer Name: ${customer.contactInfo.name}")
    println(s"Customer Email: ${customer.contactInfo.email}")
    println(s"Customer Address: ${customer.contactInfo.address}")
  }
}

CustomerApp.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala qui crée un tuple à partir de deux listes.  

// COMMAND ----------

object TupleFromListsExample {
  def main(args: Array[String]): Unit = {
    // Create two lists
    val list1 = List("Red", "Green", "Blue")
    val list2 = List(1, 3, 5)
    // Print said two lists
    println("List1: " + list1)
    println("List2: " + list2)
    // Create a tuple from the lists
    val new_tuple = list1.zip(list2)
    // Print the tuple
    println("Tuple from said two lists: " + new_tuple)
  }
}

TupleFromListsExample.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC Écrivez un programme Scala et utilisez une case class pour définir un article de panier.  
// MAGIC
// MAGIC Chaque article du panier doit avoir les propriétés suivantes, à savoir un nom, un prix et une quantité achetée.  
// MAGIC
// MAGIC Créez trois éléments de panier pour les éléments suivants :
// MAGIC * 10 glaces vanille à 2,99$ pièce
// MAGIC * 3 biscuits au chocolat à 3,99$ chacun
// MAGIC * 5 cupcakes à 4,99$ chacun
// MAGIC
// MAGIC Utilisez une structure de données appropriée pour stocker les éléments du panier mentionnés ci-dessus.  
// MAGIC
// MAGIC Ensuite, définissez et utilisez une méthode qui imprimera tous les articles d'un panier donné.

// COMMAND ----------

final case class ShoppingCartItem(name: String, price: Double, qtyBought: Int)

 val item1 = ShoppingCartItem("vanilla ice cream", 2.99, 10)
 val item2 = ShoppingCartItem("chocolate biscuits", 3.99, 3)
 val item3 = ShoppingCartItem("cupcakes", 4.99, 5)
 val basket = List(item1, item2, item3)

 def printCartItems(basket: List[ShoppingCartItem]): Unit = {
   basket.foreach { item =>
     println(s"${item.qtyBought} ${item.name} at $$${item.price} each")
   }
 }

 printCartItems(basket)


 def printIceCream(basket: List[ShoppingCartItem]): Unit = {
   basket.foreach { {
     case ShoppingCartItem("cupcakes", _, _) => println(s"Found a cupcake item.")
     case ShoppingCartItem(_,_,_) => println("Found another item.")
     }
   }
 }

 printIceCream(basket)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------


