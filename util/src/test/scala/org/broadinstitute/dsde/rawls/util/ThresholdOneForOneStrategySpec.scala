package org.broadinstitute.dsde.rawls.util

import akka.actor.SupervisorStrategy.{Directive, Restart}
import akka.actor.{Actor, ActorRef, ActorSystem, Props, SupervisorStrategy}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._

class ThresholdOneForOneStrategySpec
    extends TestKit(ActorSystem("ThresholdOneForOneStrategySpec"))
    with ScalaFutures
    with Eventually
    with AnyFlatSpecLike
    with MockitoSugar
    with Matchers
    with BeforeAndAfterAll {

  // This configures how long the calls to `whenReady(Future)` and `eventually` will wait
  // before giving up and failing the test.
  // See: http://doc.scalatest.org/2.2.4/index.html#org.scalatest.concurrent.Futures
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val alwaysRestart: PartialFunction[Throwable, Directive] = { case _ =>
    Restart
  }

  object TestException extends Exception("boom")

  case class ChildLifeCheck(a: ActorRef)

  class SupervisorTestActor(override val supervisorStrategy: SupervisorStrategy) extends Actor {
    override def receive = {
      // create new child to supervise
      case "newchild" =>
        val ref = context.actorOf(Props(new ChildTestActor()))
        sender() ! ref

      // is this child alive?
      case ChildLifeCheck(child) =>
        sender() ! context.children.toSet.contains(child)
    }
  }

  class ChildTestActor() extends Actor {
    var counter: Int = 0

    override def receive = {
      case "increment" =>
        counter += 1
        sender() ! counter

      case "exception" =>
        throw TestException
    }
  }

  "ThresholdOneForOneStrategy" should "implement OneForOneStrategy when threshold > retries" in {
    implicit val timeout = Timeout(5.seconds)

    val varInit = 1000
    var testVar: Int = varInit

    // threshold function
    def setVar(cause: Throwable, v: Int): Unit = {
      assertResult(TestException)(cause)
      testVar = v
    }

    val strategy = new ThresholdOneForOneStrategy(thresholdLimit = 3, maxNrOfRetries = Option(1))(alwaysRestart)(setVar)
    val supervisor: ActorRef = system.actorOf(Props(new SupervisorTestActor(strategy)))

    // retrieve a new supervised child
    val childRef = (supervisor ? "newchild").mapTo[ActorRef].futureValue

    // start count = 0
    // increment a few times and retrieve new counts

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }
    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 2
    }
    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 3
    }

    // throw an exception, triggering a supervised restart

    childRef ! "exception"
    Thread.sleep(100)

    // the child reference is still alive, pointing to a new Actor

    whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
      resp shouldBe a[java.lang.Boolean]
      resp.asInstanceOf[java.lang.Boolean] shouldEqual true
    }

    // threshold function was not run
    testVar shouldEqual varInit

    // start count is 0 again so it increments to 1

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }

    // throw another exception, but we have no restarts remaining

    childRef ! "exception"
    Thread.sleep(100)

    whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
      resp shouldBe a[java.lang.Boolean]
      resp.asInstanceOf[java.lang.Boolean] shouldEqual false
    }

    // threshold function was not run
    testVar shouldEqual varInit
  }

  it should "execute the threshold function when threshold < retries" in {
    implicit val timeout = Timeout(5.seconds)

    val varInit = 1000
    var testVar: Int = varInit

    // threshold function
    def setVar(cause: Throwable, v: Int): Unit = {
      assertResult(TestException)(cause)
      testVar = v
    }

    val strategy = new ThresholdOneForOneStrategy(thresholdLimit = 1, maxNrOfRetries = Option(3))(alwaysRestart)(setVar)
    val supervisor: ActorRef = system.actorOf(Props(new SupervisorTestActor(strategy)))

    // retrieve a new supervised child
    val childRef = (supervisor ? "newchild").mapTo[ActorRef].futureValue

    // start count = 0
    // increment a few times and retrieve new counts

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }
    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 2
    }
    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 3
    }

    // throw an exception, triggering a supervised restart

    childRef ! "exception"
    Thread.sleep(100)

    // the child reference is still alive, pointing to a new Actor

    whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
      resp shouldBe a[java.lang.Boolean]
      resp.asInstanceOf[java.lang.Boolean] shouldEqual true
    }

    // threshold function was not run
    testVar shouldEqual varInit

    // start count is 0 again so it increments to 1

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }

    // throw an exception, triggering a supervised restart

    childRef ! "exception"
    Thread.sleep(100)

    // the child reference is still alive, pointing to a new Actor

    whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
      resp shouldBe a[java.lang.Boolean]
      resp.asInstanceOf[java.lang.Boolean] shouldEqual true
    }

    // threshold function was run
    testVar shouldEqual 2

    // start count is 0 again so it increments to 1

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }

    // throw an exception, triggering a supervised restart

    childRef ! "exception"
    Thread.sleep(100)

    // the child reference is still alive, pointing to a new Actor

    whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
      resp shouldBe a[java.lang.Boolean]
      resp.asInstanceOf[java.lang.Boolean] shouldEqual true
    }

    // threshold function was run
    testVar shouldEqual 3

    // start count is 0 again so it increments to 1

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }

    // throw another exception, but we have no restarts remaining

    childRef ! "exception"
    Thread.sleep(100)

    whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
      resp shouldBe a[java.lang.Boolean]
      resp.asInstanceOf[java.lang.Boolean] shouldEqual false
    }

    testVar shouldEqual 3
  }

  it should "execute the threshold function when retries is unlimited" in {
    implicit val timeout = Timeout(5.seconds)

    val varInit = 1000
    var testVar: Int = varInit

    // threshold function
    def setVar(cause: Throwable, v: Int): Unit = {
      assertResult(TestException)(cause)
      testVar = v
    }

    val strategy = new ThresholdOneForOneStrategy(thresholdLimit = 1)(alwaysRestart)(setVar)
    val supervisor: ActorRef = system.actorOf(Props(new SupervisorTestActor(strategy)))

    // retrieve a new supervised child
    val childRef = (supervisor ? "newchild").mapTo[ActorRef].futureValue

    // start count = 0
    // increment a few times and retrieve new counts

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }
    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 2
    }
    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 3
    }

    // throw an exception, triggering a supervised restart

    childRef ! "exception"
    Thread.sleep(100)

    // the child reference is still alive, pointing to a new Actor

    whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
      resp shouldBe a[java.lang.Boolean]
      resp.asInstanceOf[java.lang.Boolean] shouldEqual true
    }

    // threshold function was not run
    testVar shouldEqual varInit

    // start count is 0 again so it increments to 1

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }

    // throw an exception, triggering a supervised restart

    childRef ! "exception"
    Thread.sleep(100)

    // the child reference is still alive, pointing to a new Actor

    whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
      resp shouldBe a[java.lang.Boolean]
      resp.asInstanceOf[java.lang.Boolean] shouldEqual true
    }

    // threshold function was run
    testVar shouldEqual 2

    // start count is 0 again so it increments to 1

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }

    // throw an exception, triggering a supervised restart

    childRef ! "exception"
    Thread.sleep(100)

    // the child reference is still alive, pointing to a new Actor

    whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
      resp shouldBe a[java.lang.Boolean]
      resp.asInstanceOf[java.lang.Boolean] shouldEqual true
    }

    // threshold function was run
    testVar shouldEqual 3

    // start count is 0 again so it increments to 1

    whenReady(childRef ? "increment") { resp =>
      resp shouldBe an[Integer]
      resp.asInstanceOf[Integer] shouldEqual 1
    }
  }

  Seq(-1, 0) foreach { thresh =>
    it should s"always execute the threshold function when threshold = $thresh" in {
      implicit val timeout = Timeout(5.seconds)

      val varInit = 1000
      var testVar: Int = varInit

      // threshold function
      def setVar(cause: Throwable, v: Int): Unit = {
        assertResult(TestException)(cause)
        testVar = v
      }

      val strategy = new ThresholdOneForOneStrategy(thresholdLimit = thresh)(alwaysRestart)(setVar)
      val supervisor: ActorRef = system.actorOf(Props(new SupervisorTestActor(strategy)))

      // retrieve a new supervised child
      val childRef = (supervisor ? "newchild").mapTo[ActorRef].futureValue

      // start count = 0
      // increment a few times and retrieve new counts

      whenReady(childRef ? "increment") { resp =>
        resp shouldBe an[Integer]
        resp.asInstanceOf[Integer] shouldEqual 1
      }
      whenReady(childRef ? "increment") { resp =>
        resp shouldBe an[Integer]
        resp.asInstanceOf[Integer] shouldEqual 2
      }
      whenReady(childRef ? "increment") { resp =>
        resp shouldBe an[Integer]
        resp.asInstanceOf[Integer] shouldEqual 3
      }

      // throw an exception, triggering a supervised restart

      childRef ! "exception"
      Thread.sleep(100)

      // the child reference is still alive, pointing to a new Actor

      whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
        resp shouldBe a[java.lang.Boolean]
        resp.asInstanceOf[java.lang.Boolean] shouldEqual true
      }

      // threshold function was run
      testVar shouldEqual 1

      // start count is 0 again so it increments to 1

      whenReady(childRef ? "increment") { resp =>
        resp shouldBe an[Integer]
        resp.asInstanceOf[Integer] shouldEqual 1
      }

      // throw an exception, triggering a supervised restart

      childRef ! "exception"
      Thread.sleep(100)

      // the child reference is still alive, pointing to a new Actor

      whenReady(supervisor ? ChildLifeCheck(childRef)) { resp =>
        resp shouldBe a[java.lang.Boolean]
        resp.asInstanceOf[java.lang.Boolean] shouldEqual true
      }

      // threshold function was run
      testVar shouldEqual 2

      // start count is 0 again so it increments to 1

      whenReady(childRef ? "increment") { resp =>
        resp shouldBe an[Integer]
        resp.asInstanceOf[Integer] shouldEqual 1
      }
    }
  }
}
