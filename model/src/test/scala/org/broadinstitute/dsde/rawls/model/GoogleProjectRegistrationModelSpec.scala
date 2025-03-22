package org.broadinstitute.dsde.rawls.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GoogleProjectRegistrationModelSpec extends AnyFlatSpec with Matchers {

  "UnRegisteredGoogleProjectRegistration" should "create an unregistered GoogleProjectRegistration" in {
    val googleProjectId = GoogleProjectId("test-project-id")
    val registration = UnRegisteredGoogleProjectRegistration(googleProjectId)

    registration.googleProjectId shouldEqual googleProjectId
    registration.billingAccount shouldBe None
    registration.message shouldBe None
    registration.billingProjectId.value shouldEqual "UNREGISTERED"
  }

  it should "unapply an unregistered GoogleProjectRegistration" in {
    val googleProjectId = GoogleProjectId("test-project-id")
    val registration = UnRegisteredGoogleProjectRegistration(googleProjectId)

    UnRegisteredGoogleProjectRegistration.unapply(registration) shouldEqual Some(googleProjectId)
  }

  it should "not unapply a registered GoogleProjectRegistration" in {
    val googleProjectId = GoogleProjectId("test-project-id")
    val registration = GoogleProjectRegistration(
      googleProjectId = googleProjectId,
      billingAccount = Some(RawlsBillingAccountName("billing-account")),
      message = Some("message"),
      billingProjectId = RawlsBillingProjectName("registered-project")
    )

    UnRegisteredGoogleProjectRegistration.unapply(registration) shouldEqual None
  }
}
