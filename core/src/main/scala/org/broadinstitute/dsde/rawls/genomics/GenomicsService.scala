package org.broadinstitute.dsde.rawls.genomics

import spray.json.JsObject
import scala.concurrent.Future

trait GenomicsService {
  def getOperation(jobId: String): Future[Option[JsObject]]
}
