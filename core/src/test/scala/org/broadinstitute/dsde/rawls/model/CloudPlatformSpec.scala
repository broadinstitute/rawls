package org.broadinstitute.dsde.rawls.model

import bio.terra.profile.model.{CloudPlatform => BPMCloudPlatform, ProfileModel}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class CloudPlatformSpec extends AnyFlatSpecLike with MockitoSugar with Matchers {

  behavior of "applying a CloudPlatform to a BPM ProfileModel"

  it should "be CloudPlatform.GCP when the model CloudPlatform is set to GCP" in {
    val model = new ProfileModel()
    model.setCloudPlatform(BPMCloudPlatform.GCP)
    CloudPlatform(model) shouldBe CloudPlatform.GCP
  }

  it should "be CloudPlatform.AZURE when the model CloudPlatform is set to AZURE" in {
    val model = new ProfileModel()
    model.setCloudPlatform(BPMCloudPlatform.AZURE)
    CloudPlatform(model) shouldBe CloudPlatform.AZURE
  }

  it should "be CloudPlatform.UNKNOWN when the BPM cloud platform is null" in {
    val model = new ProfileModel()
    model.getCloudPlatform shouldBe null
    model.getTenantId shouldBe null
    model.getSubscriptionId shouldBe null
    model.getManagedResourceGroupId shouldBe null
    CloudPlatform(model) shouldBe CloudPlatform.UNKNOWN
  }

}
