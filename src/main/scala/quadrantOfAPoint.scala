import scala.io.StdIn.readLine

  object quadrantOfAPoint {
    def main(args: Array[String]): Unit = {
      println("Enter the value of x: ")
      val x = scala.io.StdIn.readInt()
      println("Enter the value of y: ")
      val y = scala.io.StdIn.readInt()

      if (x > 0 && y > 0)
        println("Quadrant I")
      else if (x < 0 && y > 0)
        println("Quadrant II")
      else if (x < 0 && y < 0)
        println("Quadrant III")
      else if (x > 0 && y < 0)
        println("Quadrant IV")
      else
        println("Origin (0,0)")
    }


}


