package org.broadinstitute.dsde.rawls.googleProject

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectRegistration, RawlsBillingProject}
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

    result match {
      case Some(project) => assertResult(googleProjectReg)(project)
      case None          => fail(s"Expected Some(${googleProjectReg}) but got None")
    }
  }

  it should "return None if record already exists" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg = makeGoogleProjectRegistration(testData.testProject1)

    Await.result(repo.registerGoogleProject(googleProjectReg), Duration.Inf)
    val result = Await.result(repo.registerGoogleProject(googleProjectReg), Duration.Inf)

    assertResult(None) {
      result
    }
  }

  it should "throw error if record exists with billing profile mismatch" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg1 = makeGoogleProjectRegistration(testData.testProject1)

    Await.result(repo.registerGoogleProject(googleProjectReg1), Duration.Inf)
    val googleProjectReg2 = googleProjectReg1.copy(billingProjectId = testData.testProject2.projectName)

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(repo.registerGoogleProject(googleProjectReg2), Duration.Inf)
    }
  }

}
