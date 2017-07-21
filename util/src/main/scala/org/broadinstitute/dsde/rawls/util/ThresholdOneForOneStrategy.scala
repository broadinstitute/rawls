package org.broadinstitute.dsde.rawls.util

import akka.actor.{ActorContext, ActorRef, ChildRestartStats, OneForOneStrategy, SupervisorStrategy}

import scala.concurrent.duration.Duration

//Retries up to maxNrOfRetries within withinTimeRange.
//Once the number of retries is above thresholdLimit, call thresholdFunc before restarting child.
class ThresholdOneForOneStrategy(maxNrOfRetries: Int = -1,
                                 loggingEnabled: Boolean = true,
                                 thresholdLimit: Int = -1)
                                (decider: SupervisorStrategy.Decider)
                                (thresholdFunc: Int => Unit)
  extends OneForOneStrategy(maxNrOfRetries, Duration.Inf, loggingEnabled)(decider) {

  //stupid akka stupid makes this stupid private so i redefine it here because screw you, akka
  protected val retriesWindow: (Option[Int], Option[Int]) = ( if(maxNrOfRetries < 0) None else Some(maxNrOfRetries), None )

  override def processFailure(context: ActorContext, restart: Boolean, child: ActorRef, cause: Throwable, stats: ChildRestartStats, children: Iterable[ChildRestartStats]): Unit = {
    if (restart && stats.requestRestartPermission(retriesWindow) ) {
      if (stats.maxNrOfRetriesCount > thresholdLimit) {
        thresholdFunc(stats.maxNrOfRetriesCount)
      }
      restartChild(child, cause, suspendFirst = false)
    } else {
      context.stop(child)
    }
  }
}
