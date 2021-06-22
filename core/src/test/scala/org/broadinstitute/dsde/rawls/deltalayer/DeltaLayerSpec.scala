package org.broadinstitute.dsde.rawls.deltalayer

import org.broadinstitute.dsde.rawls.model.Workspace
import org.joda.time.DateTime
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeltaLayerSpec extends AnyFlatSpec with Matchers {

  behavior of "DeltaLayer object"

  it should "generate expected name of companion dataset for a workspace" in {
    val inputUuid = "123e4567-e89b-12d3-a456-426614174000"
    val expected = "deltalayer_forworkspace_123e4567_e89b_12d3_a456_426614174000"

    val dummyWorkspace = Workspace("namespace", "name", inputUuid, "bucketName", None, DateTime.now(), DateTime.now(), "createdBy", Map())
    val actual = DeltaLayer.generateDatasetNameForWorkspace(dummyWorkspace)

    assertResult(expected) { actual }
  }

}
