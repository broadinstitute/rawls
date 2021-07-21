package org.broadinstitute.dsde.rawls.deltalayer

import java.util.UUID

object DeltaLayer {
  def generateDatasetName(datasetReferenceId: UUID) = {
    "deltalayer_" + datasetReferenceId.toString.replace('-', '_')
  }
}
