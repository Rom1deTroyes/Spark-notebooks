// Databricks notebook source
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
println

// COMMAND ----------

class Point(var x: Int = 0, var y: Int = 0) {
 def move(dx: Int, dy: Int): Unit = {
    x = x + dx
    y = y + dy
  }

  override def toString: String =
    s"($x, $y)"
}

val origin = new Point    // x and y are both set to 0
val point1 = new Point(1) // x is set to 1 and y is set to 0
println(point1)           // prints (1, 0)
println

val point2 = new Point(y = 2)
println(point2) 
println

val point3 = new Point(x = 3, y = 3)
println(point3) 
println

point3.move(2,2)
println(point3)
println

// ceci est une erreur
// point3 = new Point(x = 10, y = 12)


// COMMAND ----------

class Point2(val x: Int, val y: Int)
val point = new Point2(1, 2)
//point.x = 3  // <-- does not compile

// COMMAND ----------

class Circle extends Point
{
    // Methods and fields
}

