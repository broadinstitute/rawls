package org.broadinstitute.dsde.rawls.genomics

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserWiths}
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by davidan on 06/12/16.
 */
object GenomicsService {
  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO)(ctx: RawlsRequestContext)(implicit
    executionContext: ExecutionContext
  ) =
    new GenomicsService(ctx, dataSource, googleServicesDAO)

}

class GenomicsService(protected val ctx: RawlsRequestContext,
                      val dataSource: SlickDataSource,
                      protected val gcsDAO: GoogleServicesDAO
)(implicit protected val executionContext: ExecutionContext)
    extends RoleSupport
    with FutureSupport
    with UserWiths {

  def getOperation(jobId: String): Future[Option[JsObject]] =
    gcsDAO.getGenomicsOperation(jobId)
}
