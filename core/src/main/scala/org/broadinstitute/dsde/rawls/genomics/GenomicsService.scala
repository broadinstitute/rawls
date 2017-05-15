package org.broadinstitute.dsde.rawls.genomics

import akka.actor.{Actor, Props}
import akka.pattern._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.genomics.GenomicsService._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by davidan on 06/12/16.
 */
object GenomicsService {
  def props(userServiceConstructor: UserInfo => GenomicsService, userInfo: UserInfo): Props = {
    Props(userServiceConstructor(userInfo))
  }

  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, userDirectoryDAO: UserDirectoryDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new GenomicsService(userInfo, dataSource, googleServicesDAO, userDirectoryDAO)

  sealed trait GenomicsServiceMessage
  case class GetOperation(jobId: String) extends GenomicsServiceMessage

}

class GenomicsService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO, userDirectoryDAO: UserDirectoryDAO)(implicit protected val executionContext: ExecutionContext) extends Actor with RoleSupport with FutureSupport with UserWiths {

  override def receive = {
    case GetOperation(jobId) => getOperation(jobId) pipeTo sender
  }

  def getOperation(jobId: String): Future[PerRequestMessage] = {
    gcsDAO.getGenomicsOperation(jobId).map(RequestComplete.apply[JsObject])
  }
}

