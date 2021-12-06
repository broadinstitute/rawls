package org.broadinstitute.dsde.rawls

import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}

import scala.concurrent.{Future, blocking}

// to run just this spec:
// export SBT_OPTS="-Xmx2G -Xms1G -Dmysql.host=localhost -Dmysql.port=3310"
// sbt "testOnly *UnitTestExamples"

/** My personal checklist for writing tests:
  *   1. Don't just check that your unit test passes. Change the expected value in your unit test and ensure it fails.
  *   2. When viewing your test failure, look at the error message. Is it helpful? If not, rewrite your test.
  *   3. Always err on the side of including a clue in your test - e.g. via withClue - to make it super easy to debug.
  *   4. Put yourself in the shoes of QA, or devops, who might be looking at test failures but without intimate knowledge
  *       of the codebase. What clues can you add to the error messages to help them?
  *   5. Also check the test names themselves; are they helpful?
  *
  */
class UnitTestExamples extends AsyncFlatSpec
  with Eventually
  with ScalaFutures
  with Matchers {

  val actual = "This is my custom error message!"

  behavior of "ScalaTest assertions and error messages on synchronous tests"

  it should "be very unhelpful when just working with a boolean" in {
    /* because this test precalculates the "isSuccess" value, the assertion only accepts a Boolean
        and therefore can't offer any good error message.
     */
    val isSuccess = actual.startsWith("whoops")
    assert( isSuccess )
    // message: isSuccess was false
  }

  it should "also be unhelpful when using booleans in assertResult" in {
    /* here, even though we include the startsWith logic inside an assertion, the assertResult
        is explicitly only looking for a Boolean. Scalatest valiantly tries to offer a good
        message but still only knows about booleans.
     */
    assertResult(true) { actual.startsWith("whoops") }
    // message: Expected true, but got false
  }

  it should "be better when including the comparison logic inside an assertion" in {
    /* by including the logic inside an assertion, Scalatest can work its magic.
        See https://www.scalatest.org/user_guide/using_assertions for detail on assertions.
     */
    assert( actual.startsWith("whoops") )
    // message: "This is my custom error message!" did not start with "whoops"
  }

  it should "be most helpful with a custom clue" in {
    /* by including the logic inside an assertion AND providing a custom clue, we offer the most
        debugging ability to anyone looking at test results.
        See https://www.scalatest.org/user_guide/using_assertions for detail on assertions.
     */
    assert( actual.startsWith("whoops"), "when looking inside the custom error message" )
    // message: "This is my custom error message!" did not start with "whoops" when looking inside the custom error message
  }

  it should "allow using the withClue closure" in {
    /* Similar to the previous example, but using an explicit "withClue" to put the clue first.
        Some assertions like assertThrows require withClue as they don't have anywhere else that accepts a clue.
        See https://www.scalatest.org/user_guide/using_assertions for detail on assertions.
     */
    withClue("when looking inside the custom error message, ") {
      assert( actual.startsWith("whoops"))
    }
    // message: when looking inside the custom error message, "This is my custom error message!" did not start with "whoops"
  }

  it should "allow readable code and good error messages with the matchers DSL" in {
    /* Here, we use Scalatest's Matchers DSL instead of assertions.  The Matchers library has a lot of features
        and can be more readable ... though it's a personal preference and has its own quirks. I've found that matchers
        can sometimes offer helpful error messages even when assertions cannot (though I can't come up with an example)
        See https://www.scalatest.org/user_guide/using_matchers for detail on matchers.
     */
    actual should startWith ("whoops")
    // message: "This is my custom error message!" did not start with substring "whoops"
  }





  behavior of "Assertions and error messages against Futures"

  // this future will return, but only after 3 seconds; we use this to test timeouts
  def threeSecondFuture(returnValue: Int): Future[Int] = {
    for {
      _ <- Future(blocking { Thread.sleep(3000) })
    } yield {
      returnValue
    }
  }

  // this config tells future-related utilities in this spec to only wait for 100 millis
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(100, Millis), interval = Span(50, Millis))

  it should "using map" in {
    /* this test passes! This is because the "map" construct here doesn't honor our patienceConfig so it waits long
         enough to succeed. This is very bad that we can't specify timeouts! If you MUST use Future.map in your test,
         look at org.scalatest.concurrent.TimeLimits or org.scalatest.concurrent.AsyncTimeLimitedTests
     */
    threeSecondFuture(1) map { x => assert(x == 1)}
  }


  it should "using futureValue" in {
    /* Using ScalaFuture's .futureValue is super convenient, but does not give good error messages on timeouts
     */
      threeSecondFuture(2).futureValue shouldBe 99
    // message: A timeout occurred waiting for a future to complete. Waited 100 milliseconds
  }

  it should "using whenReady" in {
    /* Using ScalaFuture's .whenReady is also convenient, but also does not give good error messages on timeouts
     */
    whenReady(threeSecondFuture(3)) { x =>
      x shouldBe 99
    }
    // message: A timeout occurred waiting for a future to complete. Waited 100 milliseconds.
  }

  it should "using eventually" in {
    /* Using eventually{...} - from the Eventually trait - is a bit more helpful on timeout. At least it displays
        the result of its last iteration. Still, since the future hasn't completed yet there's not much for
        the framework to display in an error message.
     */
    val fut = threeSecondFuture(4)
    eventually {
      assert(fut == Future.successful(99))
    }
    // message: The code passed to eventually never returned normally. Attempted 10 times over 104.635906 milliseconds. Last failure message: Future(<not completed>) did not equal Future(Success(99))
  }

  it should "using eventually with a clue" in {
    /* When testing with Futures, if there is ANY chance of the Future timing out (and there always is), you really
        should add a custom clue. Without a custom clue, there's very little chance an error message will be helpful.
     */
    val fut = threeSecondFuture(5)
    withClue("when checking the three-second future") {
      eventually {
        assert(fut == Future.successful(99))
      }
    }
    // message: when checking the three-second future The code passed to eventually never returned normally. Attempted 11 times over 109.193647 milliseconds. Last failure message: Future(<not completed>) did not equal Future(Success(99))
  }

}
