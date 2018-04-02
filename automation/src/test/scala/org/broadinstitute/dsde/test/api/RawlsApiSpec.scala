package org.broadinstitute.dsde.test.api

import java.util.UUID

import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls, Sam}
import org.broadinstitute.dsde.workbench.auth.{AuthToken, ServiceAccountAuthTokenFromJson}
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.dao.Google.googleIamDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccount}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class RawlsApiSpec extends FreeSpec with Matchers with CleanUp with BillingFixtures with WorkspaceFixtures with RandomUtil with Eventually with TestMethods {
  // We only want to see the users' workspaces so we can't be Project Owners
  val Seq(studentA, studentB) = UserPool.chooseStudents(2)
  val studentAToken: AuthToken = studentA.makeAuthToken()
  val studentBToken: AuthToken = studentB.makeAuthToken()

  val owner: Credentials = UserPool.chooseProjectOwner
  val ownerAuthToken: AuthToken = owner.makeAuthToken()

  def findPetInGoogle(project: String, petEmail: WorkbenchEmail): Option[ServiceAccount] = {
    val find = googleIamDAO.findServiceAccount(GoogleProject(project), petEmail)
    Await.result(find, 1.minute)
  }

  def makeMethodPublic(method: Method)(implicit token: AuthToken): Unit = {
    Orchestration.methods.setMethodPermissions(method.methodNamespace, method.methodName, method.snapshotId, "public", "READER")
  }

  "Rawls" - {
    "pets should have same access as their owners" in {
      withCleanBillingProject(owner) { projectName =>
        withCleanUp {
          //Create workspaces for Students

          Orchestration.billing.addUserToBillingProject(projectName, studentA.email, Orchestration.billing.BillingProjectRole.User)(ownerAuthToken)
          register cleanUp Orchestration.billing.removeUserFromBillingProject(projectName, studentA.email, Orchestration.billing.BillingProjectRole.User)(ownerAuthToken)

          Orchestration.billing.addUserToBillingProject(projectName, studentB.email, Orchestration.billing.BillingProjectRole.User)(ownerAuthToken)
          register cleanUp Orchestration.billing.removeUserFromBillingProject(projectName, studentB.email, Orchestration.billing.BillingProjectRole.User)(ownerAuthToken)

          val uuid = UUID.randomUUID().toString

          val workspaceNameA = "rawls_test_User_A_Workspace" + uuid
          Rawls.workspaces.create(projectName, workspaceNameA)(studentAToken)
          register cleanUp Rawls.workspaces.delete(projectName, workspaceNameA)(studentAToken)

          val workspaceNameB = "rawls_test_User_B_Workspace" + uuid
          Rawls.workspaces.create(projectName, workspaceNameB)(studentBToken)
          register cleanUp Rawls.workspaces.delete(projectName, workspaceNameB)(studentBToken)

          //Remove the pet SA for a clean test environment
          val userAStatus = Sam.user.status()(studentAToken).get
          val petEmail = Sam.user.petServiceAccountEmail(projectName)(studentAToken)
          Sam.removePet(projectName, userAStatus.userInfo)
          findPetInGoogle(projectName, petEmail) shouldBe None

          //Validate that the pet SA has been created
          val petAccountEmail = Sam.user.petServiceAccountEmail(projectName)(studentAToken)
          petAccountEmail.value should not be userAStatus.userInfo.userEmail
          findPetInGoogle(projectName, petEmail).map(_.email) shouldBe Some(petAccountEmail)

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

          Sam.removePet(projectName, userAStatus.userInfo)
        }
      }
    }

    "should request sub-workflow metadata from Cromwell" in {
      implicit val token: AuthToken = studentAToken

      val testNamespace = makeRandomId()

      val childMethod = SubWorkflowChildMethod.copy(methodNamespace = testNamespace)
      val parentMethod = subWorkflowParentMethod(childMethod).copy(methodNamespace = testNamespace)

      val config = SubWorkflowConfig

      Orchestration.methods.createMethod(childMethod.creationAttributes)
      register cleanUp Orchestration.methods.redact(childMethod)

      makeMethodPublic(childMethod)

      Orchestration.methods.createMethod(parentMethod.creationAttributes)
      register cleanUp Orchestration.methods.redact(parentMethod)

      withCleanBillingProject(studentA) { projectName =>
        withWorkspace(projectName, "rawls-subworkflow") { workspaceName =>
          Orchestration.methodConfigurations.createMethodConfigInWorkspace(
            projectName, workspaceName,
            parentMethod,
            config.configNamespace, config.configName, config.snapshotId,
            config.inputs, config.outputs, config.rootEntityType)

          Orchestration.importMetaData(projectName, workspaceName, "entities", SingleParticipant.participantEntity)

          // it currently takes ~ 5 min for google bucket read permissions to propagate.
          // We can't launch a workflow until this happens.
          // See https://github.com/broadinstitute/workbench-libs/pull/61 and https://broadinstitute.atlassian.net/browse/GAWB-3327

          Orchestration.workspaces.waitForBucketReadAccess(projectName, workspaceName)

          val submissionId = Rawls.submissions.launchWorkflow(
            projectName, workspaceName,
            config.configNamespace, config.configName,
            "participant", SingleParticipant.entityId, "this", useCallCache = false)

          // wait for the first available queryable workflow ID

          val submissionPatience = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(20, Seconds)))
          implicit val patienceConfig: PatienceConfig = submissionPatience

          val workflowId = eventually {
            val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)

            withClue(s"Submission $projectName/$workspaceName/$submissionId: ") {
              status shouldBe "Submitted"
              workflows should not be (empty)
              workflows.head
            }
          }

          // wait for Cromwell to start processing sub-workflows

          eventually {
            val cromwellMetadata = Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, workflowId)
            withClue(s"Workflow $projectName/$workspaceName/$submissionId/$workflowId: ") {
              cromwellMetadata should include("subWorkflowMetadata")
            }
          }

          // clean up: Abort and wait for Aborted

          Rawls.submissions.abortSubmission(projectName, workspaceName, submissionId)

          eventually {
            val (status, _) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)

            withClue(s"Submission $projectName/$workspaceName/$submissionId: ") {
              status shouldBe "Aborted"
            }
          }
        }
      }
    }
  }
}
