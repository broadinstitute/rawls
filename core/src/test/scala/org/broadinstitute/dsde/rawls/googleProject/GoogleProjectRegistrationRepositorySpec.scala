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

  it should "retrieve a google project registration by id" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg = makeGoogleProjectRegistration(testData.testProject1)

    Await.result(repo.registerGoogleProject(googleProjectReg), Duration.Inf)
    val result = Await.result(repo.getGoogleProjectRegistration(googleProjectReg.googleProjectId), Duration.Inf)

    result match {
      case Some(project) => assertResult(googleProjectReg)(project)
      case None          => fail(s"Expected Some(${googleProjectReg}) but got None")
    }
  }

  it should "return None if google project registration id does not exist" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val nonExistentId = GoogleProjectId(UUID.randomUUID().toString)

    val result = Await.result(repo.getGoogleProjectRegistration(nonExistentId), Duration.Inf)

    assertResult(None) {
      result
    }
  }

  it should "retrieve multiple google project registrations by ids" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg1 = makeGoogleProjectRegistration(testData.testProject1)
    val googleProjectReg2 = makeGoogleProjectRegistration(testData.testProject2)

    Await.result(repo.registerGoogleProject(googleProjectReg1), Duration.Inf)
    Await.result(repo.registerGoogleProject(googleProjectReg2), Duration.Inf)

    val result = Await.result(
      repo.getGoogleProjectRegistrations(Set(googleProjectReg1.googleProjectId, googleProjectReg2.googleProjectId)),
      Duration.Inf
    )

    assert(result.contains(googleProjectReg1))
    assert(result.contains(googleProjectReg2))
  }

  it should "return an empty sequence if none of the google project registration ids exist" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val nonExistentId1 = GoogleProjectId(UUID.randomUUID().toString)
    val nonExistentId2 = GoogleProjectId(UUID.randomUUID().toString)

    val result = Await.result(repo.getGoogleProjectRegistrations(Set(nonExistentId1, nonExistentId2)), Duration.Inf)

    assert(result.isEmpty)
  }
}
