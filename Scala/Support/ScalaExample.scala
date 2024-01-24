// Databricks notebook source
def scalaExample{  
    println("Hello Scala")  
}  

// COMMAND ----------

scalaExample

// COMMAND ----------

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello, World!")
  }
}

// COMMAND ----------

HelloWorld.main(Array())

// COMMAND ----------

HelloWorld.main(Array("Salut"))

// COMMAND ----------

val x = Array(1,2,3)

// COMMAND ----------

x(0)

// COMMAND ----------

x[0]

// COMMAND ----------

object MaximumFinder {
  def main(args: Array[String]): Unit = {
    val n1: Int = 20 // First number
    val n2: Int = 15 // Second number

    var max: Int = 0 // Initialize the maximum variable

    if (n1 > n2) {
      max = n1
    } else {
      max = n2
    }

    println(s"The maximum of $n1 and $n2 is: $max")
  }
}

// COMMAND ----------

MaximumFinder.main(Array())

// COMMAND ----------

object EvenOddChecker {
  def main(args: Array[String]): Unit = {
    //val number: Int = 15 // Number you want to check
      val number: Int = 20 // Number you want to check

    if (number % 2 == 0) {
      println(s"The number $number is even.")
    } else {
      println(s"The number $number is odd.")
    }
  }
}

// COMMAND ----------

EvenOddChecker.main(Array())

// COMMAND ----------

object FactorialCalculator {
  def main(args: Array[String]): Unit = {
    val number: Int = 4 // Number for which you want to find the factorial

    var factorial: Long = 1 // Initialize the factorial variable as 1
    var i: Int = number

    while (i > 0) {
      factorial *= i
      i -= 1
    }

    println(s"The factorial of $number is: $factorial")
  }
}

// COMMAND ----------

FactorialCalculator.main(Array())

// COMMAND ----------

object ArraySum {
  def main(args: Array[String]): Unit = {
    val numbers: Array[Int] = Array(1, 2, 3, 4, 5, 6) //Array containing the numbers

    var sum: Int = 0
    for (number <- numbers) {
      sum += number
    }
    println("Original Array elements:")
     // Print all the array elements
      for ( x <- numbers ) {
         print(s"${x}, ")        
       }
    println(s"\nThe sum of the array elements is: $sum")
  }
}

// COMMAND ----------

ArraySum.main(Array())

// COMMAND ----------



// COMMAND ----------

object PrimeChecker {
  def isPrime(n: Int): Boolean = {
    if (n <= 1) {
      false
    } else if (n <= 3) {
      true
    } else if (n % 2 == 0 || n % 3 == 0) {
      false
    } else {
      var i = 5
      while (i * i <= n) {
        if (n % i == 0 || n % (i + 2) == 0) {
          return false
        }
        i += 6
      }
      true
    }
  }

  def main(args: Array[String]): Unit = {
    val number = 13
    val isPrimeNumber = isPrime(number)
    if (isPrimeNumber) {
      println(s"$number is prime.")
    } else {
      println(s"$number is not prime.")
    }
  }
}

// COMMAND ----------

PrimeChecker.main(Array())

// COMMAND ----------

object PalindromeChecker {
  def isPalindrome(str: String): Boolean = {
    val reversed = str.reverse
    str == reversed
  }

  def main(args: Array[String]): Unit = {
    val input1 = "madam"
    val isPalindrome1 = isPalindrome(input1)
    println(s"$input1 is a palindrome: $isPalindrome1")

    val input2 = "scala"
    val isPalindrome2 = isPalindrome(input2)
    println(s"$input2 is a palindrome: $isPalindrome2")
  }
}

// COMMAND ----------

PalindromeChecker.main(Array())

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

// COMMAND ----------

PersonApp.main(Array())

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

// COMMAND ----------

StudentApp.main(Array())

// COMMAND ----------


