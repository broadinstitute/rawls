package org.broadinstitute.dsde.test.api

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.implicits.catsSyntaxOptionId
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.broadinstitute.dsde.test.api.tagannotation.rawls.{MethodsTest, WorkspacesTest}
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.auth.AuthTokenScopes.billingScopes
import org.broadinstitute.dsde.workbench.config.{Credentials, ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures.withTemporaryBillingProject
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName}
import org.broadinstitute.dsde.workbench.service.SamModel.{AccessPolicyMembership, AccessPolicyResponseEntry}
import org.broadinstitute.dsde.workbench.service._
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.broadinstitute.dsde.workbench.util.Retry
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Seconds, Span}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.Random

//noinspection ScalaUnnecessaryParentheses,JavaAccessorEmptyParenCall,ScalaUnusedSymbol
class RawlsApiSpec
  extends TestKit(ActorSystem("MySpec"))
    with AnyFreeSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with GroupFixtures
    with CleanUp
    with RandomUtil
    with Retry
    with WorkspaceFixtures
    with SubWorkflowFixtures
    with RawlsTestSuite
    with MethodFixtures {

  // We only want to see the users' workspaces so we can't be Project Owners
  val Seq(studentA, studentB) = UserPool.chooseStudents(2)

  val owner: Credentials = UserPool.chooseProjectOwner

  val billingAccountId: String = ServiceTestConfig.Projects.billingAccountId

  def parseCallsFromMetadata(metadata: String): List[JsonNode] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // "calls" is a top-level key of metadata, whose value is a JSON object mapping call names to shard lists.
    // Get that object's values.
    val calls: List[JsonNode] = mapper.readTree(metadata).get("calls").elements().asScala.toList

    // Call values are arrays of scatter shards.
    // Flatten this structure to get a single list containing all the shards.
    calls flatMap { c =>
      c.elements().asScala.toList
    }
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
    val scatterShards: List[JsonNode] = parseCallsFromMetadata(metadata)

    // Return the values corresponding to each scatter shard's "subWorkflowId" key
    scatterShards map { s =>
      s.get("subWorkflowId").textValue()
    }
  }

  def getWorkflowFieldFromMetadata(metadata: String, field: String): JsonNode = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readTree(metadata).get(field)
  }

  def parseWorkflowStatusFromMetadata(metadata: String): String = {
    getWorkflowFieldFromMetadata(metadata, "status").textValue()
  }

  def parseWorkflowOutputFromMetadata(metadata: String, outputField: String): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val metadataJson = mapper.readTree(metadata)
    val outputsJson = metadataJson.get("outputs")
    val outputFieldJson = Option(outputsJson.get(outputField))
      .getOrElse(fail(s"Unable to find metadata output $outputField"))
    outputFieldJson.asText()
  }

  def parseWorkflowOptionsFromMetadata(metadata: String): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val metadataJson = mapper.readTree(metadata)
    metadataJson.get("submittedFiles").get("options").asText()
  }

  def parseRuntimeAttributeKeyFromCallMetadata(callMetadata: JsonNode, attributeKey: String): String = {
    callMetadata.get("runtimeAttributes").get("zones").asText()
  }

  def parseWorkerAssignedExecEventsFromCallMetadata(callMetadata: JsonNode): List[String] = {
    val executionEvents = callMetadata.get("executionEvents").elements().asScala.toList
    val descriptions = executionEvents.map(_.get("description").asText())
    descriptions.filter(desc => desc.startsWith("Worker") && desc.contains("assigned"))
  }

  // if these prove useful anywhere else, they should move into workbench-libs
  def getSubmissionResponse(billingProject: String, workspaceName: String, submissionId: String)(implicit token: AuthToken): String = {
    Rawls.parseResponse(Rawls.getRequest(s"${Rawls.url}api/workspaces/$billingProject/$workspaceName/submissions/$submissionId"))
  }
  def getWorkflowResponse(billingProject: String, workspaceName: String, submissionId: String, workflowId: String)(implicit token: AuthToken): String = {
    Rawls.parseResponse(Rawls.getRequest(s"${Rawls.url}api/workspaces/$billingProject/$workspaceName/submissions/$submissionId/workflows/$workflowId"))
  }
  def getQueueStatus()(implicit token: AuthToken): String = {
    Rawls.parseResponse(Rawls.getRequest(s"${Rawls.url}api/submissions/queueStatus"))
  }

  "Rawls" - {
    "should retrieve sub-workflow metadata and outputs from Cromwell" taggedAs(MethodsTest) in {
      implicit val token: AuthToken = studentB.makeAuthToken()

      // this will run scatterCount^levels workflows, so be careful if increasing these values!
      val topLevelMethod: Method = methodTree(levels = 3, scatterCount = 3)

      withTemporaryBillingProject(billingAccountId, users = List(studentB.email).some) { projectName =>
        withWorkspace(projectName, "rawls-subworkflow-metadata") { workspaceName =>
          withCleanUp {
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
              projectName,
              workspaceName,
              topLevelMethodConfiguration.configNamespace,
              topLevelMethodConfiguration.configName,
              "participant",
              SingleParticipant.entityId,
              "this",
              useCallCache = false,
              deleteIntermediateOutputFiles = false,
              useReferenceDisks = false,
              memoryRetryMultiplier = 1.0,
              ignoreEmptyOutputs = false
            )
            // clean up: Abort submission
            register cleanUp Rawls.submissions.abortSubmission(projectName, workspaceName, submissionId)

            // may need to wait for Cromwell to start processing workflows.  just take the first one we see.

            val submissionPatience = PatienceConfig(timeout = scaled(Span(8, Minutes)), interval = scaled(Span(30, Seconds)))
            implicit val patienceConfig: PatienceConfig = submissionPatience

            val firstWorkflowId = eventually {
              val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)
              withClue(s"queue status: ${getQueueStatus()}, submission status: ${getSubmissionResponse(projectName, workspaceName, submissionId)}") {
                status should (be("Submitted") or be("Done")) // very unlikely it's already done, but let's handle that case.
                workflows should not be (empty)
                workflows.head
              }
            }

            // retrieve the workflow's metadata.  May need to wait for a subworkflow to appear.  Take the first one we see.

            val firstSubWorkflowId = eventually {
              val cromwellMetadata = Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, firstWorkflowId)
              val workflowOptions = parseWorkflowOptionsFromMetadata(cromwellMetadata)
              val subIds = parseSubWorkflowIdsFromMetadata(cromwellMetadata)
              withClue(getWorkflowResponse(projectName, workspaceName, submissionId, firstWorkflowId)) {
                workflowOptions should include ("us-central1-")

                subIds should not be (empty)
                subIds.head
              }
            }

            // can we also retrieve the subworkflow's metadata?  Get a sub-sub-workflow ID while we're doing this.

            val firstSubSubWorkflowId = eventually {
              val cromwellMetadata = Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, firstSubWorkflowId)
              val subSubIds = parseSubWorkflowIdsFromMetadata(cromwellMetadata)
              withClue(getWorkflowResponse(projectName, workspaceName, submissionId, firstSubWorkflowId)) {
                subSubIds should not be (empty)
                subSubIds.head
              }
            }

            // verify that Rawls can retrieve the sub-sub-workflow's metadata without throwing an exception.

            eventually {
              Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, firstSubSubWorkflowId)
            }

            // verify that Rawls can retrieve the workflows' outputs from Cromwell without error
            // https://github.com/DataBiosphere/firecloud-app/issues/157

            val outputsTimeout = Timeout(scaled(Span(10, Seconds)))
            eventually(outputsTimeout) {
              Rawls.submissions.getWorkflowOutputs(projectName, workspaceName, submissionId, firstWorkflowId)
              // nope https://github.com/DataBiosphere/firecloud-app/issues/160
              //Rawls.submissions.getWorkflowOutputs(projectName, workspaceName, submissionId, firstSubWorkflowId)
              //Rawls.submissions.getWorkflowOutputs(projectName, workspaceName, submissionId, firstSubSubWorkflowId)
            }
          }
        }
      }(owner.makeAuthToken(billingScopes))
    }

    "should be able to create workspace and run sub-workflow tasks in non-US regions" taggedAs(MethodsTest) in {
      implicit val token: AuthToken = studentB.makeAuthToken()

      // this will create a method with a workflow containing 3 sub-workflows
      val topLevelMethod: Method = methodTree(levels = 2, scatterCount = 3)

      val europeWest1ZonesPrefix = "europe-west1-"

      withTemporaryBillingProject(billingAccountId, users = List(studentB.email).some) { projectName =>
        withWorkspace(projectName, "rawls-subworkflows-in-regions", bucketLocation = Option("europe-west1")) { workspaceName =>
          withCleanUp {
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

            val start = System.currentTimeMillis()

            val submissionId = Rawls.submissions.launchWorkflow(
              projectName,
              workspaceName,
              topLevelMethodConfiguration.configNamespace,
              topLevelMethodConfiguration.configName,
              "participant",
              SingleParticipant.entityId,
              "this",
              useCallCache = false,
              deleteIntermediateOutputFiles = false,
              useReferenceDisks = false,
              memoryRetryMultiplier = 1.0,
              ignoreEmptyOutputs = false
            )

            logger.info(s"Submission in $projectName/$workspaceName returned submission ID: $submissionId")

            // Clean up: Abort submission. This doesn't abort the submission immediately, but only registers it to be aborted
            // as part of clean up process. Once the test finishes (or times out), an abort request is sent to Cromwell to undo any
            // side effects of the test. If the workflow has reached terminal state, Cromwell will return 404 for the abort request
            register cleanUp Rawls.submissions.abortSubmission(projectName, workspaceName, submissionId)

            // may need to wait for Cromwell to start processing workflows
            val submissionPatience = PatienceConfig(timeout = scaled(Span(30, Minutes)), interval = scaled(Span(30, Seconds)))
            implicit val patienceConfig: PatienceConfig = submissionPatience

            // Get workflow ID from submission details
            val rootWorkflowId: String = eventually {
              val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)
              withClue(s"queue status: ${getQueueStatus()}, submission status: ${getSubmissionResponse(projectName, workspaceName, submissionId)}") {
                status should (be("Submitted") or be("Done")) // very unlikely it's already done, but let's handle that case.
                workflows should not be (empty)
                workflows.head
              }
            }

            // Get sub-workflow ids and check the `zones` in workflow options belong to `europe-west1` region
            val subWorkflowIds: List[String] = eventually {
              val rootWorkflowMetadata = Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, rootWorkflowId)
              val workflowOptions = parseWorkflowOptionsFromMetadata(rootWorkflowMetadata)
              val subWorkflowIds = parseSubWorkflowIdsFromMetadata(rootWorkflowMetadata)

              withClue(getWorkflowResponse(projectName, workspaceName, submissionId, rootWorkflowId)) {
                workflowOptions should include (europeWest1ZonesPrefix)

                subWorkflowIds should not be (empty)
                subWorkflowIds.length shouldBe(3)

                subWorkflowIds
              }
            }

            logger.info(s"Submission in $projectName/$workspaceName/$submissionId returned root workflow ID: $rootWorkflowId " +
              s"and sub-workflow IDs: ${subWorkflowIds.mkString(",")}")

            // Get the sub-workflows call metadata once they finish running
            val subWorkflowCallMetadata: List[JsonNode] = eventually {
              val subWorkflowsMetadata = subWorkflowIds map { Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, _) }
              val subWorkflowCallMetadata = subWorkflowsMetadata flatMap parseCallsFromMetadata

              subWorkflowsMetadata foreach { x => x should include (""""executionStatus":"Done"""") }

              subWorkflowCallMetadata
            }

            val finish = System.currentTimeMillis()
            logger.info(s"All sub-workflows in submission $projectName/$workspaceName/$submissionId have finished execution after ${finish - start} milliseconds")

            // For each call in the sub-workflows, check
            //   - the zones for each job that were determined by Cromwell and
            //   - the worker assigned for the tasks
            // belong to `europe-west1`
            val callZones = subWorkflowCallMetadata map { parseRuntimeAttributeKeyFromCallMetadata(_, "zones") }
            val workerAssignedExecEvents = subWorkflowCallMetadata flatMap { parseWorkerAssignedExecEventsFromCallMetadata }

            callZones foreach { _.split(",") foreach { zone => zone should startWith (europeWest1ZonesPrefix) } }

            workerAssignedExecEvents should not be (empty)
            workerAssignedExecEvents foreach { event => event should include (europeWest1ZonesPrefix) }
          }
        }
      }(owner.makeAuthToken(billingScopes))
    }

    "should be able to run sub-workflow tasks in a cloned workspace in non-US regions" taggedAs(MethodsTest) in {
      implicit val token: AuthToken = studentB.makeAuthToken()

      // this will create a method with a workflow containing 3 sub-workflows
      val topLevelMethod: Method = methodTree(levels = 2, scatterCount = 3)

      val europeWest1ZonesPrefix = "europe-west1-"

      withTemporaryBillingProject(billingAccountId, users = List(studentB.email).some) { projectName =>
        // `withClonedWorkspace()` will create a new workspace, clone it and run the workflow in the cloned workspace
        withClonedWorkspace(projectName, "rawls-subworkflows-in-regions", bucketLocation = Option("europe-west1")) { workspaceName =>
          withCleanUp {
            // `withClonedWorkspace()` appends `_clone` to the original workspace. Check that workspace returned is actually a clone
            workspaceName should include ("_clone")

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

            val start = System.currentTimeMillis()

            val submissionId = Rawls.submissions.launchWorkflow(
              projectName,
              workspaceName,
              topLevelMethodConfiguration.configNamespace,
              topLevelMethodConfiguration.configName,
              "participant",
              SingleParticipant.entityId,
              "this",
              useCallCache = false,
              deleteIntermediateOutputFiles = false,
              useReferenceDisks = false,
              memoryRetryMultiplier = 1.0,
              ignoreEmptyOutputs = false
            )

            logger.info(s"Submission in $projectName/$workspaceName returned submission ID: $submissionId")

            // Clean up: Abort submission. This doesn't abort the submission immediately, but only registers it to be aborted
            // as part of clean up process. Once the test finishes (or times out), an abort request is sent to Cromwell to undo any
            // side effects of the test. If the workflow has reached terminal state, Cromwell will return 404 for the abort request
            register cleanUp Rawls.submissions.abortSubmission(projectName, workspaceName, submissionId)

            // may need to wait for Cromwell to start processing workflows
            val submissionPatience = PatienceConfig(timeout = scaled(Span(30, Minutes)), interval = scaled(Span(30, Seconds)))
            implicit val patienceConfig: PatienceConfig = submissionPatience

            // Get workflow ID from submission details
            val rootWorkflowId: String = eventually {
              val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)
              withClue(s"queue status: ${getQueueStatus()}, submission status: ${getSubmissionResponse(projectName, workspaceName, submissionId)}") {
                status should (be("Submitted") or be("Done")) // very unlikely it's already done, but let's handle that case.
                workflows should not be (empty)
                workflows.head
              }
            }

            // Get sub-workflow ids and check the `zones` in workflow options belong to `europe-west1` region
            val subWorkflowIds: List[String] = eventually {
              val rootWorkflowMetadata = Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, rootWorkflowId)
              val workflowOptions = parseWorkflowOptionsFromMetadata(rootWorkflowMetadata)
              val subWorkflowIds = parseSubWorkflowIdsFromMetadata(rootWorkflowMetadata)

              withClue(getWorkflowResponse(projectName, workspaceName, submissionId, rootWorkflowId)) {
                workflowOptions should include (europeWest1ZonesPrefix)

                subWorkflowIds should not be (empty)
                subWorkflowIds.length shouldBe(3)

                subWorkflowIds
              }
            }

            logger.info(s"Submission in $projectName/$workspaceName/$submissionId returned root workflow ID: $rootWorkflowId " +
              s"and sub-workflow IDs: ${subWorkflowIds.mkString(",")}")

            // Get the sub-workflows call metadata once they finish running
            val subWorkflowCallMetadata = eventually {
              val subWorkflowsMetadata = subWorkflowIds map { Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, _) }
              val subWorkflowCallMetadata = subWorkflowsMetadata flatMap parseCallsFromMetadata

              subWorkflowsMetadata foreach { x => x should include (""""executionStatus":"Done"""") }

              subWorkflowCallMetadata
            }

            val finish = System.currentTimeMillis()
            logger.info(s"All sub-workflows in submission $projectName/$workspaceName/$submissionId have finished execution after ${finish - start} milliseconds")

            // For each call in the sub-workflows, check
            //   - the zones for each job that were determined by Cromwell and
            //   - the worker assigned for the tasks
            // belong to `europe-west1`
            val callZones = subWorkflowCallMetadata map { parseRuntimeAttributeKeyFromCallMetadata(_, "zones") }
            val workerAssignedExecEvents = subWorkflowCallMetadata flatMap { parseWorkerAssignedExecEventsFromCallMetadata }

            callZones foreach { _.split(",") foreach { zone => zone should startWith (europeWest1ZonesPrefix) } }

            workerAssignedExecEvents should not be (empty)
            workerAssignedExecEvents foreach { event => event should include (europeWest1ZonesPrefix) }
          }
        }
      }(owner.makeAuthToken(billingScopes))
    }

//    Disabling this test until we decide what to do with it. See AP-177

    "should retrieve metadata with widely scattered sub-workflows in a short time" taggedAs(MethodsTest) ignore {
      implicit val token: AuthToken = studentA.makeAuthToken()

      val scatterWidth = 500

      // this will run scatterCount^levels workflows, so be careful if increasing these values!
      val topLevelMethod: Method = methodTree(levels = 2, scatterCount = scatterWidth)

      withTemporaryBillingProject(billingAccountId, users = List(studentA.email).some) { projectName =>
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
            "participant", SingleParticipant.entityId, "this", useCallCache = false,
            deleteIntermediateOutputFiles = true,
            useReferenceDisks = false,
            memoryRetryMultiplier = 1.2,
            ignoreEmptyOutputs = false
          )

          // may need to wait for Cromwell to start processing workflows.  just take the first one we see.

          val submissionPatience = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(20, Seconds)))
          implicit val patienceConfig: PatienceConfig = submissionPatience

          val firstWorkflowId = eventually {
            val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)

            withClue(s"Submission $projectName/$workspaceName/$submissionId: ") {
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

          Random.shuffle(subworkflowIds.take(10)).foreach {
            cromwellMetadata(_)
          }

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
      }(owner.makeAuthToken(billingScopes))
    }

    "should label low security bucket" taggedAs(WorkspacesTest) in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)
      implicit val token: AuthToken = studentA.makeAuthToken()

      withTemporaryBillingProject(billingAccountId, users = List(studentA.email).some) { projectName =>
        withWorkspace(projectName, "rawls-bucket-test") { workspaceName =>
          val bucketName = Rawls.workspaces.getBucketName(projectName, workspaceName)
          val bucket = googleStorageDAO.getBucket(GcsBucketName(bucketName)).futureValue

          bucket.getLabels.asScala should contain theSameElementsAs Map("security" -> "low")
        }
      }(owner.makeAuthToken(billingScopes))
    }

    "should label high security bucket" taggedAs(WorkspacesTest) in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)
      implicit val token: AuthToken = studentA.makeAuthToken()

      withGroup("ad") { realmGroup =>
        withGroup("ad2") { realmGroup2 =>
          withTemporaryBillingProject(billingAccountId, users = List(studentA.email).some) { projectName =>
            withWorkspace(projectName, "rawls-bucket-test", Set(realmGroup, realmGroup2)) { workspaceName =>
              val bucketName = Rawls.workspaces.getBucketName(projectName, workspaceName)
              val bucket = googleStorageDAO.getBucket(GcsBucketName(bucketName)).futureValue

              bucketName should startWith("fc-secure-")
              bucket.getLabels.asScala should contain theSameElementsAs Map("security" -> "high", "ad-" + realmGroup.toLowerCase -> "", "ad-" + realmGroup2.toLowerCase -> "")
            }
          }(owner.makeAuthToken(billingScopes))
        }
      }
    }

    // bucket IAM roles for sam policies as described in comments in updateBucketIam function in HttpGoogleServicesDAO
    // Note that the role in this map is just the suffix, as the org ID will vary depending on which context this test is run
    // We will check against just the suffix instead of the entire string
    val policyToBucketRole = Map("project-owner" -> "terraBucketWriter", "owner" -> "terraBucketWriter", "writer" -> "terraBucketWriter", "reader" -> "terraBucketReader")

    "should have correct policies in Sam and IAM roles in Google when an unconstrained workspace is created" taggedAs(WorkspacesTest) in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)
      implicit val token: AuthToken = owner.makeAuthToken()

      withTemporaryBillingProject(billingAccountId) { projectName =>
        withWorkspace(projectName, s"unconstrained-workspace") { workspaceName =>
          val workspaceId = getWorkspaceId(projectName, workspaceName)
          val samPolicies = verifySamPolicies(workspaceId)
          val bucketName = GcsBucketName(Rawls.workspaces.getBucketName(projectName, workspaceName))

          // check bucket acls. bucket policy only is enabled for workspace buckets so we do not need to look at object acls
          val actualBucketRolesWithEmails = getBucketRolesWithEmails(bucketName)

          val expectedBucketRolesWithEmails = samPolicies.collect {
            case AccessPolicyResponseEntry("project-owner", AccessPolicyMembership(emails, _, _), _) => ("terraBucketWriter", emails.head)
            case AccessPolicyResponseEntry(policyName, _, email) if policyToBucketRole.contains(policyName) => (policyToBucketRole(policyName), email.value)
          }.groupBy{ case (policy, _) => policy}.map{ case (policy, policyRolePairs) => policy -> policyRolePairs.map(_._2)}
          actualBucketRolesWithEmails should contain theSameElementsAs expectedBucketRolesWithEmails
        }
      }(owner.makeAuthToken(billingScopes))
    }

    "should have correct policies in Sam and IAM roles in Google when a constrained workspace is created" taggedAs(WorkspacesTest) in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)
      implicit val token: AuthToken = owner.makeAuthToken()

      withTemporaryBillingProject(billingAccountId) { projectName =>
        withGroup("authDomain", List(owner.email)) { authDomain =>
          withWorkspace(projectName, s"constrained-workspace", Set(authDomain)) { workspaceName =>
            val workspaceId = getWorkspaceId(projectName, workspaceName)
            val samPolicies = verifySamPolicies(workspaceId)
            val bucketName = GcsBucketName(Rawls.workspaces.getBucketName(projectName, workspaceName))

            // check bucket acls. bucket policy only is enabled for workspace buckets so we do not need to look at object acls
            val actualBucketRolesWithEmails = getBucketRolesWithEmails(bucketName)
            val expectedBucketRolesWithEmails = samPolicies.collect {
              case AccessPolicyResponseEntry(policyName, _, email) if policyToBucketRole.contains(policyName) => (policyToBucketRole(policyName), email.value)
            }.groupBy{ case (policy, _) => policy}.map{ case (policy, policyRolePairs) => policy -> policyRolePairs.map(_._2)}
            actualBucketRolesWithEmails.toMap should contain theSameElementsAs expectedBucketRolesWithEmails
          }
        }
      }(owner.makeAuthToken(billingScopes))
    }

    "should clone a workspace and only copy files in the specified path" taggedAs(WorkspacesTest) in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 5 minutes)
      implicit val token: AuthToken = studentA.makeAuthToken()

      withTemporaryBillingProject(billingAccountId, users = List(studentA.email).some) { projectName =>
        withWorkspace(projectName, "test-copy-files", Set.empty) { workspaceName =>
          withCleanUp {
            val bucketName = Rawls.workspaces.getBucketName(projectName, workspaceName)

            val fileToCopy = GcsObjectName("/pleasecopythis/foo.txt")
            val fileToLeave = GcsObjectName("/dontcopythis/bar.txt")

            googleStorageDAO.storeObject(GcsBucketName(bucketName), fileToCopy, "foo", "text/plain").futureValue
            googleStorageDAO.storeObject(GcsBucketName(bucketName), fileToLeave, "bar", "text/plain").futureValue

            val initialFiles = googleStorageDAO.listObjectsWithPrefix(GcsBucketName(bucketName), "").futureValue.map(_.value)

            initialFiles.size shouldBe 2
            initialFiles should contain(fileToCopy.value)
            initialFiles should contain(fileToLeave.value)

            val destWorkspaceName = workspaceName + "_clone"
            Rawls.workspaces.clone(projectName, workspaceName, projectName, destWorkspaceName, Set.empty, Some("/pleasecopythis"))
            register cleanUp Rawls.workspaces.delete(projectName, destWorkspaceName)
            val cloneBucketName = Rawls.workspaces.getBucketName(projectName, destWorkspaceName)

            val start = System.currentTimeMillis()
            eventually {
              googleStorageDAO.listObjectsWithPrefix(GcsBucketName(cloneBucketName), "").futureValue.size shouldBe 1
            }
            val finish = System.currentTimeMillis()

            googleStorageDAO.listObjectsWithPrefix(GcsBucketName(cloneBucketName), "").futureValue.map(_.value) should contain only fileToCopy.value

            logger.info(s"Copied bucket files visible after ${finish - start} milliseconds")
          }
        }
      }(owner.makeAuthToken(billingScopes))
    }

    "should support running workflows with private docker images" taggedAs(MethodsTest) in {
      implicit val token: AuthToken = owner.makeAuthToken()

      val privateMethod: Method = MethodData.SimpleMethod.copy(
        methodName = s"${UUID.randomUUID().toString()}-private_test_method",
        payload = "task hello {\n  String? name\n\n  command {\n    echo 'hello ${name}!'\n  }\n  output {\n    File response = stdout()\n  }\n  runtime {\n    docker: \"mtalbott/mtalbott-papi-v2\"\n  }\n}\n\nworkflow test {\n  call hello\n}"
      )

      withTemporaryBillingProject(billingAccountId) { projectName =>
        withWorkspace(projectName, "rawls-private-image") { workspaceName =>
          withCleanUp {
            Orchestration.methods.createMethod(privateMethod.creationAttributes)
            register cleanUp Orchestration.methods.redact(privateMethod)

            Orchestration.methodConfigurations.createMethodConfigInWorkspace(
              projectName, workspaceName,
              privateMethod,
              SimpleMethodConfig.configNamespace, SimpleMethodConfig.configName, SimpleMethodConfig.snapshotId,
              SimpleMethodConfig.inputs, SimpleMethodConfig.outputs, SimpleMethodConfig.rootEntityType)

            Orchestration.importMetaData(projectName, workspaceName, "entities", SingleParticipant.participantEntity)

            // it currently takes ~ 5 min for google bucket read permissions to propagate.
            // We can't launch a workflow until this happens.
            // See https://github.com/broadinstitute/workbench-libs/pull/61 and https://broadinstitute.atlassian.net/browse/GAWB-3327

            Orchestration.workspaces.waitForBucketReadAccess(projectName, workspaceName)

            val submissionId = Rawls.submissions.launchWorkflow(
              projectName,
              workspaceName,
              SimpleMethodConfig.configNamespace,
              SimpleMethodConfig.configName,
              "participant",
              SingleParticipant.entityId,
              "this",
              useCallCache = false,
              deleteIntermediateOutputFiles = false,
              useReferenceDisks = false,
              memoryRetryMultiplier = 1.0,
              ignoreEmptyOutputs = false
            )
            // clean up: Abort submission
            register cleanUp Rawls.submissions.abortSubmission(projectName, workspaceName, submissionId)

            val submissionPatience = PatienceConfig(timeout = scaled(Span(16, Minutes)), interval = scaled(Span(30, Seconds)))
            implicit val patienceConfig: PatienceConfig = submissionPatience

            val workflowId = eventually {
              val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)
              withClue(s"queue status: ${getQueueStatus()}, submission status: ${getSubmissionResponse(projectName, workspaceName, submissionId)}") {
                workflows should not be (empty)
                workflows.head
              }
            }

            eventually {
              parseWorkflowStatusFromMetadata(Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, workflowId)) should be("Succeeded")
            }
          }
        }
      }(owner.makeAuthToken(billingScopes))
    }

    "should support running workflows with wdl structs" taggedAs(MethodsTest) in {
      implicit val token: AuthToken = owner.makeAuthToken()

      val privateMethod: Method = MethodData.SimpleMethod.copy(
        methodName = s"${UUID.randomUUID()}-wdl_struct_test_method",
        payload =
          """version 1.0
            |
            |struct IndexedVcf {
            |    File vcf
            |    File vcf_idx
            |}
            |
            |workflow test_count_variants {
            |    input {
            |        IndexedVcf indexedVcf
            |        String interval
            |    }
            |
            |    call count_variants {
            |        input: indexedVcf = indexedVcf, interval = interval
            |    }
            |
            |    output {
            |        Int count = count_variants.count
            |    }
            |}
            |
            |task count_variants {
            |    input {
            |        IndexedVcf indexedVcf
            |        String interval
            |    }
            |
            |    command {
            |        ln -s '~{indexedVcf.vcf}' '~{basename(indexedVcf.vcf)}'
            |        ln -s '~{indexedVcf.vcf_idx}' '~{basename(indexedVcf.vcf_idx)}'
            |
            |        gatk \
            |            CountVariants \
            |            --intervals '~{interval}' \
            |            --variant '~{basename(indexedVcf.vcf)}' | \
            |        tail -n 1 > count.txt
            |    }
            |
            |    output {
            |        Int count = read_int("count.txt")
            |    }
            |
            |    runtime {
            |        docker: "broadinstitute/gatk:4.1.7.0"
            |    }
            |}
            |""".stripMargin
      )

      withTemporaryBillingProject(billingAccountId) { projectName =>
        withWorkspace(projectName, "rawls-wdl-struct") { workspaceName =>
          withCleanUp {
            Orchestration.methods.createMethod(privateMethod.creationAttributes)
            register cleanUp Orchestration.methods.redact(privateMethod)

            /*
            HapMap is an old and very public project: https://en.wikipedia.org/wiki/International_HapMap_Project

            The links below are using the GCP Cloud Life Sciences public datasets:
            https://cloud.google.com/life-sciences/docs/resources/public-datasets

            If someone at GCP pulls this copy of these reference files, there should be other public copies.
            One example is the Broad's copy: gs://gcp-public-data--broad-references/hg38/v0/hapmap_3.3.hg38.vcf.gz

            Also if/when we switch to multi-cloud, this file should be publicly available on other clouds, such as
            https://registry.opendata.aws/broad-references/
             */
            val entityType = "participant"
            val entityId = "hapmap_vcf"
            val vcfPath =  "gs://genomics-public-data/resources/broad/hg38/v0/hapmap_3.3.hg38.vcf.gz"
            val vcfIndexPath =  "gs://genomics-public-data/resources/broad/hg38/v0/hapmap_3.3.hg38.vcf.gz.tbi"

            val entityTsv =
              s"""|entity:${entityType}_id\tvcf_path\tvcf_index_path
                  |$entityId\t$vcfPath\t$vcfIndexPath
                  |""".stripMargin

            val inputs = Map(
              "test_count_variants.indexedVcf" ->
                """{
                  |  "vcf": this.vcf_path,
                  |  "vcf_idx": this.vcf_index_path
                  |}
                  |""".stripMargin,
              "test_count_variants.interval" -> """"chr20"""",
            )

            val outputs = Map(
              "test_count_variants.count" -> "this.chr20_vcf_count",
            )

            Orchestration.methodConfigurations.createMethodConfigInWorkspace(
              wsNs = projectName,
              wsName = workspaceName,
              method = privateMethod,
              configNamespace = SimpleMethodConfig.configNamespace,
              configName = SimpleMethodConfig.configName,
              methodConfigVersion = SimpleMethodConfig.snapshotId,
              inputs = inputs,
              outputs = outputs,
              rootEntityType = entityType,
            )

            Orchestration.importMetaData(
              ns = projectName,
              wsName = workspaceName,
              fileName = "entities",
              fileContent = entityTsv,
            )

            // It currently takes ~ 5 min for google bucket read permissions to propagate.
            // We can't launch a workflow until this happens.
            // See https://github.com/broadinstitute/workbench-libs/pull/61
            // and https://broadinstitute.atlassian.net/browse/GAWB-3327

            Orchestration.workspaces.waitForBucketReadAccess(projectName, workspaceName)

            val submissionId = Rawls.submissions.launchWorkflow(
              billingProject = projectName,
              workspaceName = workspaceName,
              methodConfigurationNamespace = SimpleMethodConfig.configNamespace,
              methodConfigurationName = SimpleMethodConfig.configName,
              entityType = entityType,
              entityName = entityId,
              expression = "this",
              useCallCache = false,
              deleteIntermediateOutputFiles = false,
              useReferenceDisks = false,
              memoryRetryMultiplier = 1.0,
              ignoreEmptyOutputs = false
            )

            register cleanUp Rawls.submissions.abortSubmission(projectName, workspaceName, submissionId)

            val submissionPatience = PatienceConfig(
              timeout = scaled(Span(36, Minutes)),
              interval = scaled(Span(30, Seconds)),
            )
            implicit val patienceConfig: PatienceConfig = submissionPatience

            val workflowId = eventually {
              val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)
              val clue =
                s"queue status: ${getQueueStatus()}, " +
                s"submission status: ${getSubmissionResponse(projectName, workspaceName, submissionId)}"
              withClue(clue) {
                workflows should not be (empty)
                workflows.head
              }
            }

            val notRunningMetadata = eventually {
              val metadata = Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, workflowId)
              val status = parseWorkflowStatusFromMetadata(metadata)
              status should not be("Submitted")
              status should not be("Running")
              metadata
            }
            withClue(Option(getWorkflowFieldFromMetadata(notRunningMetadata, "failures")).getOrElse("No failures in metadata")) {
              parseWorkflowStatusFromMetadata(notRunningMetadata) should be("Succeeded")
            }
            parseWorkflowOutputFromMetadata(notRunningMetadata, "test_count_variants.count") should be("123997")
          }
        }
      }(owner.makeAuthToken(billingScopes))
    }

    "should fail to launch a submission with a reserved output attribute" taggedAs(MethodsTest) in {
      implicit val token: AuthToken = owner.makeAuthToken()

      withTemporaryBillingProject(billingAccountId) { projectName =>
        withWorkspace(projectName, "rawls-wdl-struct") { workspaceName =>
          withMethod("RawlsApiSpec_invalidOutputs", MethodData.SimpleMethod) { methodName =>
            withCleanUp {
              val invalidOutputs = SimpleMethodConfig.outputs map {
                case (key, _) => key -> s"this.${SimpleMethodConfig.rootEntityType}_id"
              }

              Orchestration.methodConfigurations.createMethodConfigInWorkspace(
                wsNs = projectName,
                wsName = workspaceName,
                method = MethodData.SimpleMethod.copy(methodName = methodName),
                configNamespace = SimpleMethodConfig.configNamespace,
                configName = SimpleMethodConfig.configName,
                methodConfigVersion = SimpleMethodConfig.snapshotId,
                inputs = SimpleMethodConfig.inputs,
                outputs = invalidOutputs,
                rootEntityType = SimpleMethodConfig.rootEntityType,
              )

              Orchestration.importMetaData(
                ns = projectName,
                wsName = workspaceName,
                fileName = "entities",
                fileContent = SingleParticipant.participantEntity,
              )

              // It currently takes ~ 5 min for google bucket read permissions to propagate.
              // We can't launch a workflow until this happens.
              // See https://github.com/broadinstitute/workbench-libs/pull/61
              // and https://broadinstitute.atlassian.net/browse/GAWB-3327

              Orchestration.workspaces.waitForBucketReadAccess(projectName, workspaceName)

              val exception = intercept[RestException](
                Rawls.submissions.launchWorkflow(
                  billingProject = projectName,
                  workspaceName = workspaceName,
                  methodConfigurationNamespace = SimpleMethodConfig.configNamespace,
                  methodConfigurationName = SimpleMethodConfig.configName,
                  entityType = SimpleMethodConfig.rootEntityType,
                  entityName = SingleParticipant.entityId,
                  expression = "this",
                  useCallCache = false,
                  deleteIntermediateOutputFiles = false,
                  useReferenceDisks = false,
                  memoryRetryMultiplier = 1.0,
                  ignoreEmptyOutputs = false
                )
              )

              val exceptionObject = exception.message.parseJson.asJsObject
              exceptionObject.fields("message").convertTo[String] should be(
                "Validation errors: Invalid outputs: " +
                  "test.hello.response -> Attribute name participant_id is reserved and cannot be overwritten"
              )
            }
          }
        }
      }(owner.makeAuthToken(billingScopes))
    }
  }

  private def getWorkspaceId(projectName: String, workspaceName: String)(implicit token: AuthToken): String = {
    import DefaultJsonProtocol._
    Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName).parseJson.asJsObject.getFields("workspace").flatMap { workspace =>
      workspace.asJsObject.getFields("workspaceId")
    }.head.convertTo[String]
  }

  // Retrieves policies for workspace from Sam and verifies they are correct
  private def verifySamPolicies(workspaceId: String)(implicit token: AuthToken): Set[AccessPolicyResponseEntry] = {
    val workspacePolicyNames = Set("can-compute", "project-owner", "writer", "reader", "share-writer", "can-catalog", "owner", "share-reader")
    val samPolicies = Sam.user.listResourcePolicies("workspace", workspaceId)

    val samPolicyNames = samPolicies.map(_.policyName)
    samPolicyNames should contain theSameElementsAs workspacePolicyNames
    samPolicies
  }

  // Retrieves roles with policy emails for bucket acls and checks that service account is set up correctly
  private def getBucketRolesWithEmails(bucketName: GcsBucketName)(implicit patienceConfig: PatienceConfig): List[(String, Set[String])] = {
    import cats.effect.IO
    import cats.effect.unsafe.implicits.global

    GoogleStorageService.resource[IO](
      RawlsConfig.pathToQAJson
    ).use {
      storage =>
        for {
         policy <- storage.getIamPolicy(bucketName, None).compile.lastOrError
       } yield {
          policy.getBindings.asScala.collect {
            case binding if(binding._1.toString.contains("terraBucket")) => (binding._1.toString.split("/")(3), binding._2.asScala.map(_.getValue).toSet)
          }
       }.toList
    }.unsafeRunSync()
  }
}
