package org.broadinstitute.dsde.rawls

import spray.json._
import scala.util.Random

/**
 * Created by abaumann on 4/21/15.
 */
object TestClass extends DefaultJsonProtocol {
  implicit val testClassFormat = jsonFormat4(TestClass)

  case class TestClass(byte:Byte, short:Short, int:Int, long:Long)  {
    def print(): Unit = {
      println(byte + " " + short + " " + long)
    }
  }

  def apply(seed:Int) = {
    val rand:Random = new Random(seed)

    val testByte = rand.nextInt.toByte
    val testShort = rand.nextInt.toShort
    val testInt = rand.nextInt
    val testLong = rand.nextLong//new Random().nextLong()

    new TestClass(testByte, testShort, testInt, testLong)
  }
}