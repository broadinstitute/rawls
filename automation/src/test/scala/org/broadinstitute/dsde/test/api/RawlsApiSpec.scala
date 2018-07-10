package org.broadinstitute.dsde.test.api

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls, Sam}
import org.broadinstitute.dsde.workbench.auth.{AuthToken, ServiceAccountAuthTokenFromJson}
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.dao.Google.googleIamDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccount}
import org.broadinstitute.dsde.workbench.util.Retry
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{FreeSpecLike, Matchers}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.Random

class RawlsApiSpec extends TestKit(ActorSystem("MySpec")) with FreeSpecLike with Matchers with Eventually with ScalaFutures
  with CleanUp with RandomUtil with Retry
  with BillingFixtures with WorkspaceFixtures with SubWorkflowFixtures {

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

  def parseSubWorkflowIdsFromMetadata(metadata: String): List[String] = {
    /*
    Workflow metadata has this structure:

    {
      "calls" : {
        "foo.bar" : [
          {
            "subWorkflowId" : "69581e76-2eb0-4179-99b9-958d210ebc4b", ...
          }, ...
        ], ...
      },  ...
    }
    */

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // "calls" is a top-level key of metadata, whose value is a JSON object.
    // Get that object's values.
    val calls: List[JsonNode] = mapper.readTree(metadata).get("calls").elements().asScala.toList

    // Call values are arrays of scatter shards.
    // Get the shards, which are JSON objects.
    val scatterShards: List[JsonNode] = calls flatMap { c =>
      c.elements().asScala.toList
    }

    // Return the values corresponding to each scatter shard's "subWorkflowId" key
    scatterShards map { s =>
      s.get("subWorkflowId").textValue()
    }
  }

  "Rawls" - {
    "should give pets the same access as their owners" in {
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
          implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(5, Seconds)))
          eventually(findPetInGoogle(projectName, petEmail) shouldBe None)

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

    "should retrieve sub-workflow metadata from Cromwell" in {
      implicit val token: AuthToken = studentAToken

      // this will run scatterCount^levels workflows, so be careful if increasing these values!
      val topLevelMethod: Method = methodTree(levels = 3, scatterCount = 3)

      withCleanBillingProject(studentA) { projectName =>
        withWorkspace(projectName, "rawls-subworkflow-metadata") { workspaceName =>
          Orchestration.methodConfigurations.createMethodConfigInWorkspace(
            projectName, workspaceName,
            topLevelMethod,
            topLevelMethodConfiguration.configNamespace, topLevelMethodConfiguration.configName, topLevelMethodConfiguration.snapshotId,
            topLevelMethodConfiguration.inputs(topLevelMethod), topLevelMethodConfiguration.outputs(topLevelMethod), topLevelMethodConfiguration.rootEntityType)

          Orchestration.importMetaData(projectName, workspaceName, "entities", SingleParticipant.participantEntity)

          // it currently takes ~ 5 min for google bucket read permissions to propagate.
          // We can't launch a workflow until this happens.
          // See https://github.com/broadinstitute/workbench-libs/pull/61 and https://broadinstitute.atlassian.net/browse/GAWB-3327

          Orchestration.workspaces.waitForBucketReadAccess(projectName, workspaceName)

          val submissionId = Rawls.submissions.launchWorkflow(
            projectName, workspaceName,
            topLevelMethodConfiguration.configNamespace, topLevelMethodConfiguration.configName,
            "participant", SingleParticipant.entityId, "this", useCallCache = false)

          // may need to wait for Cromwell to start processing workflows.  just take the first one we see.

          val submissionPatience = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(20, Seconds)))
          implicit val patienceConfig: PatienceConfig = submissionPatience

          val firstWorkflowId = eventually {
            val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)

            withClue(s"Submission $projectName/$workspaceName/$submissionId: ") {
              status shouldBe "Submitted"
              workflows should not be (empty)
              workflows.head
            }
          }

          // retrieve the workflow's metadata.  May need to wait for a subworkflow to appear.  Take the first one we see.

          val firstSubWorkflowId = eventually {
            val cromwellMetadata = Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, firstWorkflowId)
            val subIds = parseSubWorkflowIdsFromMetadata(cromwellMetadata)
            withClue(s"Workflow $projectName/$workspaceName/$submissionId/$firstWorkflowId: ") {
              subIds should not be (empty)
              subIds.head
            }
          }

          // can we also retrieve the subworkflow's metadata?  Get a sub-sub-workflow ID while we're doing this.

          val firstSubSubWorkflowId = eventually {
            val cromwellMetadata = Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, firstSubWorkflowId)
            val subSubIds = parseSubWorkflowIdsFromMetadata(cromwellMetadata)
            withClue(s"Workflow $projectName/$workspaceName/$submissionId/$firstSubWorkflowId: ") {
              subSubIds should not be (empty)
              subSubIds.head
            }
          }

          // verify that Rawls can retrieve the sub-sub-workflow's metadata without throwing an exception.

          eventually {
            Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, firstSubSubWorkflowId)
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

    "should retrieve metadata with widely scattered sub-workflows in a short time" in {
      implicit val token: AuthToken = studentAToken

      val scatterWidth = 500

      // this will run scatterCount^levels workflows, so be careful if increasing these values!
      val topLevelMethod: Method = methodTree(levels = 2, scatterCount = scatterWidth)

      withCleanBillingProject(studentA) { projectName =>
        withWorkspace(projectName, "rawls-subworkflow-metadata") { workspaceName =>
          Orchestration.methodConfigurations.createMethodConfigInWorkspace(
            projectName, workspaceName,
            topLevelMethod,
            topLevelMethodConfiguration.configNamespace, topLevelMethodConfiguration.configName, topLevelMethodConfiguration.snapshotId,
            topLevelMethodConfiguration.inputs(topLevelMethod), topLevelMethodConfiguration.outputs(topLevelMethod), topLevelMethodConfiguration.rootEntityType)

          Orchestration.importMetaData(projectName, workspaceName, "entities", SingleParticipant.participantEntity)

          // it currently takes ~ 5 min for google bucket read permissions to propagate.
          // We can't launch a workflow until this happens.
          // See https://github.com/broadinstitute/workbench-libs/pull/61 and https://broadinstitute.atlassian.net/browse/GAWB-3327

          Orchestration.workspaces.waitForBucketReadAccess(projectName, workspaceName)

          val submissionId = Rawls.submissions.launchWorkflow(
            projectName, workspaceName,
            topLevelMethodConfiguration.configNamespace, topLevelMethodConfiguration.configName,
            "participant", SingleParticipant.entityId, "this", useCallCache = false)

          // may need to wait for Cromwell to start processing workflows.  just take the first one we see.

          val submissionPatience = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(20, Seconds)))
          implicit val patienceConfig: PatienceConfig = submissionPatience

          val firstWorkflowId = eventually {
            val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)

            withClue(s"Submission $projectName/$workspaceName/$submissionId: ") {
              status shouldBe "Submitted"
              workflows should not be (empty)
              workflows.head
            }
          }

          // retrieve the workflow's metadata.
          // Orchestration times out in 1 minute, so we want to be well below that

          // we also need to check that it returns *at all* in under a minute
          // `eventually` won't cover this if the call itself is slow and synchronous

          val myTimeout = Timeout(scaled(Span(45, Seconds)))
          val myInterval = Interval(scaled(Span(10, Seconds)))

          implicit val ec: ExecutionContextExecutor = system.dispatcher

          def cromwellMetadata(wfId: String) = Future {
            Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, wfId)
          }.futureValue(timeout = myTimeout)

          val subworkflowIds = eventually(myTimeout, myInterval) {
            val subIds = parseSubWorkflowIdsFromMetadata(cromwellMetadata(firstWorkflowId))
            withClue(s"Workflow $projectName/$workspaceName/$submissionId/$firstWorkflowId: ") {
              subIds.size shouldBe scatterWidth
            }
            subIds
          }

          // can we also quickly retrieve metadata for a few of the subworkflows?

          Random.shuffle(subworkflowIds.take(10)).foreach { cromwellMetadata(_) }

          // clean up: Abort and wait for one minute or Aborted, whichever comes first
          // Timeout is OK here: just make a best effort

          Rawls.submissions.abortSubmission(projectName, workspaceName, submissionId)

          val abortOrGiveUp = retryUntilSuccessOrTimeout()(timeout = 1.minute, interval = 10.seconds) { () =>
            Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId) match {
              case (status, _) if status == "Aborted" => Future.successful(())
              case (status, _) => Future.failed(new Exception(s"Expected Aborted, saw $status"))
            }
          }

          // wait on the future's execution
          abortOrGiveUp.futureValue
        }
      }

    }
  }
}
