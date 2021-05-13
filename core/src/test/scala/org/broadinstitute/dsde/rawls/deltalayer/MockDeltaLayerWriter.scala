package org.broadinstitute.dsde.rawls.deltalayer
import org.broadinstitute.dsde.rawls.model.DeltaInsert

class MockDeltaLayerWriter extends DeltaLayerWriter {
  override def writeFile(writeObject: DeltaInsert): Unit = ???
}
