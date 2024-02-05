package org.broadinstitute.dsde.rawls.genomics

import spray.json.JsObject
import scala.concurrent.Future

trait GenomicsServiceRequest {
  def getOperation(jobId: String): Future[Option[JsObject]]
}
