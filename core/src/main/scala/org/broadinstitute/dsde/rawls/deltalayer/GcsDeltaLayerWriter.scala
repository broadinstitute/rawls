package org.broadinstitute.dsde.rawls.deltalayer

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

class GcsDeltaLayerWriter(val storageService: GoogleStorageService[IO],
                          val sourceBucket: GcsBucketName)
  extends DeltaLayerWriter {


}
