package org.broadinstitute.dsde.rawls.snapshot

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class SnapshotServiceSpec extends AnyWordSpecLike with Matchers with TestDriverComponent {

  "SnapshotService" should {
    "foo" in {
      //Test that when we create a snapshot:
      // a) we call createDataset in Google (with non-empty labels)
      // b) we call setDatasetIam in Google
      // c) we call listPolicies for the workspace in Sam

      //Test that when we try to create a snapshot that does not exist:
      // a) we *do not* call createDataset in Google
      // b) we *do not* call setDatasetIam in Google
      // c) presumably we can verify a certain exception type was thrown?

    }
  }

}
