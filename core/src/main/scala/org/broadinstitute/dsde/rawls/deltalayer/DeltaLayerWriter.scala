package org.broadinstitute.dsde.rawls.deltalayer

import org.broadinstitute.dsde.rawls.model.DeltaInsert

trait DeltaLayerWriter {

  /**
   * write the delta file to its destination
   * @param writeObject the delta file to be written
   */
  def writeFile(writeObject: DeltaInsert)

}
