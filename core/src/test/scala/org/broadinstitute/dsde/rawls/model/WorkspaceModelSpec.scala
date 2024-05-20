package org.broadinstitute.dsde.rawls.model;

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WorkspaceModelSpec extends AnyFlatSpec with Matchers {
  "WorkspaceState" should "indicate whether it is deletable" in {
    WorkspaceState.Creating.isDeletable shouldBe false
    WorkspaceState.CreateFailed.isDeletable shouldBe true
    WorkspaceState.Cloning.isDeletable shouldBe false
    WorkspaceState.CloningContainer.isDeletable shouldBe false
    WorkspaceState.CloningFailed.isDeletable shouldBe true
    WorkspaceState.Ready.isDeletable shouldBe true
    WorkspaceState.Updating.isDeletable shouldBe false
    WorkspaceState.UpdateFailed.isDeletable shouldBe true
    WorkspaceState.Deleting.isDeletable shouldBe false
    WorkspaceState.DeleteFailed.isDeletable shouldBe true
  }
}
