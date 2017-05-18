package org.broadinstitute.dsde.rawls.health

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.UserDirectoryDAO
import org.broadinstitute.dsde.rawls.model.{StatusCheckResponse, SubsystemStatus}

import scala.concurrent.Future
import HealthMonitorService._

/**
  * Created by rtitle on 5/17/17.
  */
object HealthMonitorService {
  case object Check
  case class Store(system: String, status: SubsystemStatus)
  case object GetCurrentStatus

  def props(userDirectoryDAO: UserDirectoryDAO): Props =
    Props(new HealthMonitorService(userDirectoryDAO))
}

class HealthMonitorService(val userDirectoryDAO: UserDirectoryDAO) extends Actor with LazyLogging {

  import context.dispatcher

  var data: Map[String, SubsystemStatus] = _

  override def receive: Receive = {
    case Check => checkAll

    case Store(system, status) => store(system, status)

    case GetCurrentStatus => sender ! getCurrentStatus
  }

  private def checkAll: Unit = {
    check(userDirectoryDAO)
  }

  private def check[A: Monitorable](a: A): Unit = {
    val res = implicitly[Monitorable[A]].test(a)
    Future.traverse(res) { case (k, v) =>
      v.map { _ =>
        SubsystemStatus(true, None)
      } recover { case ex =>
        SubsystemStatus(false, Some(ex.getMessage))
      } map {
        Store(k, _)
      }
    } pipeTo self
  }

  private def store(system: String, status: SubsystemStatus): Unit = {
    data = data + (system -> status)
  }

  private def getCurrentStatus: StatusCheckResponse = {
    val overall = data.values.forall(_.ok)
    StatusCheckResponse(overall, data)
  }

}

