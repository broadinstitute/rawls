package org.broadinstitute.dsde.rawls.util

import akka.actor.{ActorContext, ActorRef, ChildRestartStats, SupervisorStrategy}

//Retries up to maxNrOfRetries regardless of time range.
//Once the number of retries is above thresholdLimit, call thresholdFunc before restarting child.
class ThresholdOneForOneStrategy(thresholdLimit: Int,
                                 maxNrOfRetries: Option[Int] = None,
                                 override val loggingEnabled: Boolean = true
)(override val decider: SupervisorStrategy.Decider = SupervisorStrategy.defaultDecider)(
  thresholdFunc: (Throwable, Int) => Unit
) extends SupervisorStrategy {

  // this is a stupid hack because
  // 1. akka stupid makes this stupid private
  // 2. ChildRestartStats doesn't increment the retry count when maxNrOfRetries is unlimited so we pretend with maxint
  protected val retriesWindow: (Option[Int], Option[Int]) = {
    val countWindow = maxNrOfRetries match {
      // other strategies use a -1 sentinel to mean unlimited so you can do that here too
      case Some(retries) if retries >= 0 => maxNrOfRetries
      case _                             => Some(Int.MaxValue)
    }
    val timeOutWindow = None // not used here
    (countWindow, timeOutWindow)
  }

  override def processFailure(context: ActorContext,
                              restart: Boolean,
                              child: ActorRef,
                              cause: Throwable,
                              stats: ChildRestartStats,
                              children: Iterable[ChildRestartStats]
  ): Unit =
    // it's necessary to call stats.requestRestartPermission() to increment stats' retry count
    if (restart && stats.requestRestartPermission(retriesWindow)) {
      // this is post-increment so thresholdLimit = 0 means this will always run
      if (stats.maxNrOfRetriesCount > thresholdLimit) {
        thresholdFunc(cause, stats.maxNrOfRetriesCount)
      }
      restartChild(child, cause, suspendFirst = false)
    } else {
      context.stop(child)
    }

  // the built-in strategies don't do anything here either
  override def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit = ()
}
