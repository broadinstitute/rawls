package org.broadinstitute.dsde.rawls.expressions

import org.apache.commons.jexl2.{Expression, JexlContext, JexlEngine, MapContext}
import org.scalatest.FunSuite

import scala.collection.JavaConversions._

/**
 * Created by abaumann on 5/21/15.
 */
class JexlTest  extends FunSuite {
  test("jexl") {
    // Create or retrieve a JexlEngine
    val jexl = new JexlEngine();
    jexl.setLenient(false)
    jexl.setDebug(true)
    jexl.setSilent(false)
    // Create an expression object
    //    val jexlExp = "foo.innerFoo().baz() + 10";
    //    val jexlExp = "foo.innerFoo().bar() + \"barbar\"";
    val jexlExp = "foo.innerFoo().baq().filter(\"yay\")";
    val e:Expression = jexl.createExpression( jexlExp );

    class Funcs {
      def func2 = (a:String) => a == "yay"
      def filter(a:Seq[String], filt:String) = a.filter(_ == filt)
    }
    val funcs:Map[String, Object] = Map("filter" -> new Funcs());

    jexl.setFunctions(funcs)
    // Create a context and add data
    val jc:JexlContext = new MapContext();

    case class Baq(list:Seq[String]) {
      def filter(f:String): Seq[String] = {
        list.filter(_ == f)
      }
    }
    case class InnerFoo() {
      def bar():String = {
        "bar!"
      }
      def baz():Int = {
        10
      }

      def baq():Baq = {
        Baq(List("hah", "yay"))
      }

      //      def gab():java.util.List[String] = {
      //        baq().asJava
      //      }
    }
    class Foo() {
      val innerFoo:InnerFoo = new InnerFoo();
    }

    def func(a:String):Boolean = {
      a == "yay"
    }

    //    println(new Foo().innerFoo.bar())
    jc.set("foo", new Foo() );
    //    jc.set("func", func );

    // Now evaluate the expression, getting the result
    val o = e.evaluate(jc);

    println(o)
  }
}
