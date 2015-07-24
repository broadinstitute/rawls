package org.broadinstitute.dsde.rawls.integrationtest

/**
 * Generic timing class. Keeps track of the number of operations, so it makes the most sense
 * to have only one type of operation for a given timer instance.
 */
class Timer(name: String) {
  var secondsElapsed = 0.0
  var numOperations = 0

  def timedOperation[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    secondsElapsed += (t1 - t0) / 1000.0
    numOperations += 1
    result
  }

  def printElapsed = println(s"[time] - $name completed $numOperations operations in $secondsElapsed seconds")
}
