package org.broadinstitute.dsde.rawls.deltalayer

import org.broadinstitute.dsde.rawls.model.DeltaInsert

trait DeltaLayerWriter {

  /**
   * write the delta file to the cloud
   * @param writeObject
   */
  def writeFile(writeObject: DeltaInsert)

}
