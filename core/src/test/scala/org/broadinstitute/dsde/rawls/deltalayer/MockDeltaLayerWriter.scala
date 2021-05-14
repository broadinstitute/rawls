package org.broadinstitute.dsde.rawls.deltalayer
import org.broadinstitute.dsde.rawls.model.DeltaInsert

class MockDeltaLayerWriter extends DeltaLayerWriter {
  // TODO: flesh out for unit tests, ideally using a mocked Google storage service
  override def writeFile(writeObject: DeltaInsert): Unit = ???
}
