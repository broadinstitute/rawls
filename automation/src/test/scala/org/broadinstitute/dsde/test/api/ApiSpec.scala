package org.broadinstitute.dsde.rawls.test.api

import java.util.UUID

import org.broadinstitute.dsde.workbench.service.{Sam, Rawls}
import org.broadinstitute.dsde.workbench.service.Sam.user.UserStatusDetails
import org.broadinstitute.dsde.workbench.auth.{AuthToken, ServiceAccountAuthToken}
import org.broadinstitute.dsde.workbench.config.{Config, UserPool}
import org.broadinstitute.dsde.workbench.dao.Google.googleIamDAO
import org.broadinstitute.dsde.workbench.service.test.CleanUp
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccount}
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class RawlsApiSpec extends FreeSpec with Matchers with CleanUp {
  // We only want to see the users' workspaces so we can't be Project Owners
  val Seq(studentA, studentB) = UserPool.chooseStudents(2)
  val studentAToken: AuthToken = studentA.makeAuthToken()
  val studentBToken: AuthToken = studentB.makeAuthToken()

  val defaultProject:String = Config.Projects.default

  def findPetInGoogle(userInfo: UserStatusDetails): Option[ServiceAccount] = {

    val find = googleIamDAO.findServiceAccount(GoogleProject(defaultProject), Sam.petName(userInfo))
    Await.result(find, 1.minute)
  }

  "Rawls" - {
    "pets should have same access as their owners" in {

      //Create workspaces for Students

      val uuid = UUID.randomUUID().toString

      val workspaceNameA = "rawls_test_User_A_Workspace" + uuid
      Rawls.workspaces.create(defaultProject, workspaceNameA)(studentAToken)
      register cleanUp Rawls.workspaces.delete(defaultProject, workspaceNameA)(studentAToken)

      val workspaceNameB = "rawls_test_User_B_Workspace" + uuid
      Rawls.workspaces.create(defaultProject, workspaceNameB)(studentBToken)
      register cleanUp Rawls.workspaces.delete(defaultProject, workspaceNameB)(studentBToken)

      //Remove the pet SA for a clean test environment
      val userAStatus = Sam.user.status()(studentAToken).get
      Sam.removePet(userAStatus.userInfo)
      findPetInGoogle(userAStatus.userInfo) shouldBe None

      //Validate that the pet SA has been created
      val petAccountEmail = Sam.user.petServiceAccountEmail()(studentAToken)
      petAccountEmail.value should not be userAStatus.userInfo.userEmail
      findPetInGoogle(userAStatus.userInfo).map(_.email) shouldBe Some(petAccountEmail)

      val petAuthToken = ServiceAccountAuthToken(petAccountEmail)

      //TODO: Deserialize the json instead of checking for substring
      val petWorkspace = Rawls.workspaces.list()(petAuthToken)
      petWorkspace should include (workspaceNameA)
      petWorkspace should not include (workspaceNameB)

      val userAWorkspace = Rawls.workspaces.list()(studentAToken)
      userAWorkspace should include (workspaceNameA)
      userAWorkspace should not include (workspaceNameB)

      val userBWorkspace = Rawls.workspaces.list()(studentBToken)
      userBWorkspace should include (workspaceNameB)

      petAuthToken.removePrivateKey()
      Sam.removePet(userAStatus.userInfo)
      findPetInGoogle(userAStatus.userInfo) shouldBe None
    }
  }
}