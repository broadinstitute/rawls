package org.broadinstitute.dsde.rawls.deltalayer

import akka.http.scaladsl.model.Uri
import org.broadinstitute.dsde.rawls.model.DeltaInsert

import scala.concurrent.Future

trait DeltaLayerWriter {

  /**
   * write the delta file to its destination
   * @param writeObject the delta file to be written
   */
  def writeFile(writeObject: DeltaInsert): Future[Uri]

}
