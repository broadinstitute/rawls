package org.broadinstitute.dsde.rawls.googleProject

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingProject, GoogleProjectRegistration}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class GoogleProjectRegistrationRepositorySpec extends AnyFlatSpec with TestDriverComponent {

  behavior of "registerGoogleProject"

  def makeGoogleProjectRegistration(billingProject: RawlsBillingProject) = GoogleProjectRegistration(
    GoogleProjectId(UUID.randomUUID().toString),
    billingProject.billingAccount,
    Some("fake message"),
    billingProject.projectName
  )

  it should "create a google project record" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg = makeGoogleProjectRegistration(testData.testProject1)

    val result = Await.result(repo.registerGoogleProject(googleProjectReg), Duration.Inf)

    assertResult(googleProjectReg) {
      result
    }
  }

}
