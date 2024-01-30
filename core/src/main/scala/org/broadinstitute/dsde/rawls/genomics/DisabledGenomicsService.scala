package org.broadinstitute.dsde.rawls.genomics

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, UserWiths}
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}


object DisabledGenomicsService {
  def constructor(dataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO)(
    ctx: RawlsRequestContext)(implicit executionContext: ExecutionContext) =
    new GenomicsService(ctx, dataSource, googleServicesDAO)
}

class DisabledGenomicsService(protected val ctx: RawlsRequestContext,
                      val dataSource: SlickDataSource,
                      protected val gcsDAO: GoogleServicesDAO
                     )(implicit protected val executionContext: ExecutionContext)
  extends RoleSupport
    with FutureSupport
    with UserWiths {

  def getOperation(jobId: String): Future[Option[JsObject]] =
    throw new NotImplementedError("getOperation is not implemented for Azure.")
}

