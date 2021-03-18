package org.broadinstitute.dsde.rawls.genomics

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserWiths}
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import spray.json.{JsObject, PrettyPrinter}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by davidan on 06/12/16.
 */
object GenomicsService {
  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new GenomicsService(userInfo, dataSource, googleServicesDAO)

}

class GenomicsService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, protected val gcsDAO: GoogleServicesDAO)(implicit protected val executionContext: ExecutionContext) extends RoleSupport with FutureSupport with UserWiths {

  def getOperation(jobId: String): Future[Option[JsObject]] = {
    gcsDAO.getGenomicsOperation(jobId)
  }
}

