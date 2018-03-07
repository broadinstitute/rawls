package org.broadinstitute.dsde.firecloud.test.api.rawls

import java.util.UUID

import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls, Sam}
import org.broadinstitute.dsde.workbench.service.Sam.user.UserStatusDetails
import org.broadinstitute.dsde.workbench.auth.{AuthToken, ServiceAccountAuthTokenFromJson}
import org.broadinstitute.dsde.workbench.config.{Config, Credentials, UserPool}
import org.broadinstitute.dsde.workbench.dao.Google.googleIamDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.service.test.CleanUp
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccount}
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class RawlsApiSpec extends FreeSpec with Matchers with CleanUp with BillingFixtures {
  // We only want to see the users' workspaces so we can't be Project Owners
  val Seq(studentA, studentB) = UserPool.chooseStudents(2)
  val studentAToken: AuthToken = studentA.makeAuthToken()
  val studentBToken: AuthToken = studentB.makeAuthToken()

  val owner: Credentials = UserPool.chooseProjectOwner
  val ownerAuthToken: AuthToken = owner.makeAuthToken()

  def findPetInGoogle(project: String, userInfo: UserStatusDetails): Option[ServiceAccount] = {

    val find = googleIamDAO.findServiceAccount(GoogleProject(project), Sam.petName(userInfo))
    Await.result(find, 1.minute)
  }

  "Rawls" - {
    "pets should have same access as their owners" in {
      withCleanBillingProject(owner) { projectName =>

        //Create workspaces for Students

        Orchestration.billing.addUserToBillingProject(projectName, studentA.email, Orchestration.billing.BillingProjectRole.User)(ownerAuthToken)
        Orchestration.billing.addUserToBillingProject(projectName, studentB.email, Orchestration.billing.BillingProjectRole.User)(ownerAuthToken)

        val uuid = UUID.randomUUID().toString

        val workspaceNameA = "rawls_test_User_A_Workspace" + uuid
        Rawls.workspaces.create(projectName, workspaceNameA)(studentAToken)
        register cleanUp Rawls.workspaces.delete(projectName, workspaceNameA)(studentAToken)

        val workspaceNameB = "rawls_test_User_B_Workspace" + uuid
        Rawls.workspaces.create(projectName, workspaceNameB)(studentBToken)
        register cleanUp Rawls.workspaces.delete(projectName, workspaceNameB)(studentBToken)

        //Remove the pet SA for a clean test environment
        val userAStatus = Sam.user.status()(studentAToken).get
        Sam.removePet(projectName, userAStatus.userInfo)
        findPetInGoogle(projectName, userAStatus.userInfo) shouldBe None

        //Validate that the pet SA has been created
        val petAccountEmail = Sam.user.petServiceAccountEmail(projectName)(studentAToken)
        petAccountEmail.value should not be userAStatus.userInfo.userEmail
        findPetInGoogle(projectName, userAStatus.userInfo).map(_.email) shouldBe Some(petAccountEmail)

        val petAuthToken = ServiceAccountAuthTokenFromJson(Sam.user.petServiceAccountKey(projectName)(studentAToken))

        //TODO: Deserialize the json instead of checking for substring
        val petWorkspace = Rawls.workspaces.list()(petAuthToken)
        petWorkspace should include(workspaceNameA)
        petWorkspace should not include (workspaceNameB)

        val userAWorkspace = Rawls.workspaces.list()(studentAToken)
        userAWorkspace should include(workspaceNameA)
        userAWorkspace should not include (workspaceNameB)

        val userBWorkspace = Rawls.workspaces.list()(studentBToken)
        userBWorkspace should include(workspaceNameB)

        //TODO: this would be better as a cleanUp, but due to issues with ordering, the GPAlloc'd project
        //is released before the cleanUp functions are able to run, resulting in stranded users
        Orchestration.billing.removeUserFromBillingProject(projectName, studentA.email, Orchestration.billing.BillingProjectRole.User)(ownerAuthToken)
        Orchestration.billing.removeUserFromBillingProject(projectName, studentB.email, Orchestration.billing.BillingProjectRole.User)(ownerAuthToken)

        Sam.removePet(projectName, userAStatus.userInfo)
        findPetInGoogle(projectName, userAStatus.userInfo) shouldBe None
      }
    }
  }
}
