package org.broadinstitute.dsde.rawls.genomics

import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import spray.json.JsObject

import scala.concurrent.Future

object DisabledGenomicsService {
  def constructor()(ctx: RawlsRequestContext) =
    new DisabledGenomicsService(ctx)

}
class DisabledGenomicsService(protected val ctx: RawlsRequestContext)
  extends GenomicsServiceRequest {
  def getOperation(jobId: String): Future[Option[JsObject]] =
    throw new NotImplementedError("getOperation is not implemented for Azure.")
}

