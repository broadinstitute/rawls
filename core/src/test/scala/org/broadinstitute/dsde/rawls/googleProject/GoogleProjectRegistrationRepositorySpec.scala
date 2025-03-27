package org.broadinstitute.dsde.rawls.googleProject

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectRegistration, RawlsBillingProject}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class GoogleProjectRegistrationRepositorySpec extends AnyFlatSpec with TestDriverComponent with Matchers {

  behavior of "registerGoogleProject"

  private def makeGoogleProjectRegistration(billingProject: RawlsBillingProject) = GoogleProjectRegistration(
    GoogleProjectId(UUID.randomUUID().toString),
    billingProject.billingAccount,
    Some("fake message"),
    billingProject.projectName
  )

  it should "create a google project record" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg = makeGoogleProjectRegistration(testData.testProject1)

    val result = Await.result(repo.registerGoogleProject(googleProjectReg), Duration.Inf)

    result should contain(googleProjectReg)
  }

  it should "return None if record already exists" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg = makeGoogleProjectRegistration(testData.testProject1)

    Await.result(repo.registerGoogleProject(googleProjectReg), Duration.Inf)
    val result = Await.result(repo.registerGoogleProject(googleProjectReg), Duration.Inf)

    result shouldBe empty
  }

  it should "throw error if record exists with billing profile mismatch" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg1 = makeGoogleProjectRegistration(testData.testProject1)

    Await.result(repo.registerGoogleProject(googleProjectReg1), Duration.Inf)
    val googleProjectReg2 = googleProjectReg1.copy(billingProjectId = testData.testProject2.projectName)

    val result = intercept[RawlsExceptionWithErrorReport] {
      Await.result(repo.registerGoogleProject(googleProjectReg2), Duration.Inf)
    }

    result.getMessage should include("This google project id is already registered with a different billing project.")
  }

  behavior of "deleteGoogleProjectRegistration"

  it should "do nothing if trying to delete a record that does not exist" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg = makeGoogleProjectRegistration(testData.testProject1)

    val result =
      Await.result(repo.deleteGoogleProjectRegistration(googleProjectReg.googleProjectId), Duration.Inf)

    result shouldBe false
  }

  it should "delete the googleProjectRegistration record" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg = makeGoogleProjectRegistration(testData.testProject1)

    Await.result(repo.registerGoogleProject(googleProjectReg), Duration.Inf)

    val result =
      Await.result(repo.deleteGoogleProjectRegistration(googleProjectReg.googleProjectId), Duration.Inf)

    result shouldBe true
  }

  behavior of "getGoogleProjectRegistration"

  it should "retrieve a google project registration by id" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg = makeGoogleProjectRegistration(testData.testProject1)

    Await.result(repo.registerGoogleProject(googleProjectReg), Duration.Inf)
    val result = Await.result(repo.getGoogleProjectRegistration(googleProjectReg.googleProjectId), Duration.Inf)

    result should contain(googleProjectReg)
  }

  it should "return None if google project registration id does not exist" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val nonExistentId = GoogleProjectId(UUID.randomUUID().toString)

    val result = Await.result(repo.getGoogleProjectRegistration(nonExistentId), Duration.Inf)

    result shouldBe empty
  }

  behavior of "getGoogleProjectRegistrations"

  it should "retrieve multiple google project registrations by ids" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val googleProjectReg1 = makeGoogleProjectRegistration(testData.testProject1)
    val googleProjectReg2 = makeGoogleProjectRegistration(testData.testProject2)

    Await.result(repo.registerGoogleProject(googleProjectReg1), Duration.Inf)
    Await.result(repo.registerGoogleProject(googleProjectReg2), Duration.Inf)

    val result = Await.result(
      repo.getGoogleProjectRegistrations(Set(googleProjectReg1.googleProjectId, googleProjectReg2.googleProjectId),
                                         None,
                                         10,
                                         0
      ),
      Duration.Inf
    )

    result should contain theSameElementsAs List(googleProjectReg1, googleProjectReg2)
  }

  it should "return an empty sequence if none of the google project registration ids exist" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)
    val nonExistentId1 = GoogleProjectId(UUID.randomUUID().toString)
    val nonExistentId2 = GoogleProjectId(UUID.randomUUID().toString)

    val result =
      Await.result(repo.getGoogleProjectRegistrations(Set(nonExistentId1, nonExistentId2), None, 10, 0), Duration.Inf)

    result shouldBe empty
  }

  it should "return an empty sequence if empty google project registration ids set" in withDefaultTestDatabase {
    val repo = new GoogleProjectRegistrationRepository(slickDataSource)

    val result =
      Await.result(repo.getGoogleProjectRegistrations(Set.empty, None, 10, 0), Duration.Inf)

    result shouldBe empty
  }
}
