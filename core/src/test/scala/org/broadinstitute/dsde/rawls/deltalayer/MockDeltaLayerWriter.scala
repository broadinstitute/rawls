package org.broadinstitute.dsde.rawls.deltalayer
import akka.http.scaladsl.model.Uri
import org.broadinstitute.dsde.rawls.model.deltalayer.v1.DeltaInsert

import scala.concurrent.Future

class MockDeltaLayerWriter extends DeltaLayerWriter {
  // TODO: flesh out for unit tests, ideally using a mocked Google storage service
  override def writeFile(writeObject: DeltaInsert): Future[Uri] = ???
}
