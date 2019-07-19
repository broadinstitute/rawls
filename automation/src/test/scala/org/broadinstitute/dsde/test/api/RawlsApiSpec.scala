package org.broadinstitute.dsde.test.api

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.fixture.{MethodData, SimpleMethodConfig, _}
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GoogleProject, ServiceAccount}
import org.broadinstitute.dsde.workbench.service.SamModel.{AccessPolicyMembership, AccessPolicyResponseEntry}
import org.broadinstitute.dsde.workbench.service._
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.broadinstitute.dsde.workbench.util.Retry
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{FreeSpecLike, Matchers}
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class RawlsApiSpec extends TestKit(ActorSystem("MySpec")) with FreeSpecLike with Matchers with Eventually with ScalaFutures with GroupFixtures
  with CleanUp with RandomUtil with Retry
  with BillingFixtures with WorkspaceFixtures with SubWorkflowFixtures with RawlsTestSuite {

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

  def parseWorkflowStatusFromMetadata(metadata: String): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    mapper.readTree(metadata).get("status").textValue()
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
    "should retrieve sub-workflow metadata and outputs from Cromwell" in {
      implicit val token: AuthToken = studentBToken

      // this will run scatterCount^levels workflows, so be careful if increasing these values!
      val topLevelMethod: Method = methodTree(levels = 3, scatterCount = 3)

      withCleanBillingProject(studentB) { projectName =>
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
              projectName, workspaceName,
              topLevelMethodConfiguration.configNamespace, topLevelMethodConfiguration.configName,
              "participant", SingleParticipant.entityId, "this", useCallCache = false)
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
              val subIds = parseSubWorkflowIdsFromMetadata(cromwellMetadata)
              withClue(getWorkflowResponse(projectName, workspaceName, submissionId, firstWorkflowId)) {
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
      }

    }

//    Disabling this test until we decide what to do with it. See AP-177

//    "should retrieve metadata with widely scattered sub-workflows in a short time" in {
//      implicit val token: AuthToken = studentAToken
//
//      val scatterWidth = 500
//
//      // this will run scatterCount^levels workflows, so be careful if increasing these values!
//      val topLevelMethod: Method = methodTree(levels = 2, scatterCount = scatterWidth)
//
//      withCleanBillingProject(studentA) { projectName =>
//        withWorkspace(projectName, "rawls-subworkflow-metadata") { workspaceName =>
//          Orchestration.methodConfigurations.createMethodConfigInWorkspace(
//            projectName, workspaceName,
//            topLevelMethod,
//            topLevelMethodConfiguration.configNamespace, topLevelMethodConfiguration.configName, topLevelMethodConfiguration.snapshotId,
//            topLevelMethodConfiguration.inputs(topLevelMethod), topLevelMethodConfiguration.outputs(topLevelMethod), topLevelMethodConfiguration.rootEntityType)
//
//          Orchestration.importMetaData(projectName, workspaceName, "entities", SingleParticipant.participantEntity)
//
//          // it currently takes ~ 5 min for google bucket read permissions to propagate.
//          // We can't launch a workflow until this happens.
//          // See https://github.com/broadinstitute/workbench-libs/pull/61 and https://broadinstitute.atlassian.net/browse/GAWB-3327
//
//          Orchestration.workspaces.waitForBucketReadAccess(projectName, workspaceName)
//
//          val submissionId = Rawls.submissions.launchWorkflow(
//            projectName, workspaceName,
//            topLevelMethodConfiguration.configNamespace, topLevelMethodConfiguration.configName,
//            "participant", SingleParticipant.entityId, "this", useCallCache = false)
//
//          // may need to wait for Cromwell to start processing workflows.  just take the first one we see.
//
//          val submissionPatience = PatienceConfig(timeout = scaled(Span(5, Minutes)), interval = scaled(Span(20, Seconds)))
//          implicit val patienceConfig: PatienceConfig = submissionPatience
//
//          val firstWorkflowId = eventually {
//            val (status, workflows) = Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId)
//
//            withClue(s"Submission $projectName/$workspaceName/$submissionId: ") {
//              workflows should not be (empty)
//              workflows.head
//            }
//          }
//
//          // retrieve the workflow's metadata.
//          // Orchestration times out in 1 minute, so we want to be well below that
//
//          // we also need to check that it returns *at all* in under a minute
//          // `eventually` won't cover this if the call itself is slow and synchronous
//
//          val myTimeout = Timeout(scaled(Span(45, Seconds)))
//          val myInterval = Interval(scaled(Span(10, Seconds)))
//
//          implicit val ec: ExecutionContextExecutor = system.dispatcher
//
//          def cromwellMetadata(wfId: String) = Future {
//            Rawls.submissions.getWorkflowMetadata(projectName, workspaceName, submissionId, wfId)
//          }.futureValue(timeout = myTimeout)
//
//          val subworkflowIds = eventually(myTimeout, myInterval) {
//            val subIds = parseSubWorkflowIdsFromMetadata(cromwellMetadata(firstWorkflowId))
//            withClue(s"Workflow $projectName/$workspaceName/$submissionId/$firstWorkflowId: ") {
//              subIds.size shouldBe scatterWidth
//            }
//            subIds
//          }
//
//          // can we also quickly retrieve metadata for a few of the subworkflows?
//
//          Random.shuffle(subworkflowIds.take(10)).foreach {
//            cromwellMetadata(_)
//          }
//
//          // clean up: Abort and wait for one minute or Aborted, whichever comes first
//          // Timeout is OK here: just make a best effort
//
//          Rawls.submissions.abortSubmission(projectName, workspaceName, submissionId)
//
//          val abortOrGiveUp = retryUntilSuccessOrTimeout()(timeout = 1.minute, interval = 10.seconds) { () =>
//            Rawls.submissions.getSubmissionStatus(projectName, workspaceName, submissionId) match {
//              case (status, _) if status == "Aborted" => Future.successful(())
//              case (status, _) => Future.failed(new Exception(s"Expected Aborted, saw $status"))
//            }
//          }
//
//          // wait on the future's execution
//          abortOrGiveUp.futureValue
//        }
//      }
//
//    }

    "should label low security bucket" in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)
      implicit val token: AuthToken = studentAToken

      withCleanBillingProject(studentA) { projectName =>
        withWorkspace(projectName, "rawls-bucket-test") { workspaceName =>
          val bucketName = Rawls.workspaces.getBucketName(projectName, workspaceName)
          val bucket = googleStorageDAO.getBucket(GcsBucketName(bucketName)).futureValue

          bucket.getLabels.asScala should contain theSameElementsAs Map("security" -> "low")
        }
      }
    }

    "should label high security bucket" in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)
      implicit val token: AuthToken = studentAToken

      withGroup("ad") { realmGroup =>
        withGroup("ad2") { realmGroup2 =>
          withCleanBillingProject(studentA) { projectName =>
            withWorkspace(projectName, "rawls-bucket-test", Set(realmGroup, realmGroup2)) { workspaceName =>
              val bucketName = Rawls.workspaces.getBucketName(projectName, workspaceName)
              val bucket = googleStorageDAO.getBucket(GcsBucketName(bucketName)).futureValue

              bucketName should startWith("fc-secure-")
              bucket.getLabels.asScala should contain theSameElementsAs Map("security" -> "high", "ad-" + realmGroup.toLowerCase -> "", "ad-" + realmGroup2.toLowerCase -> "")
            }
          }
        }
      }
    }

    // bucket IAM roles for sam policies as described in comments in updateBucketIam function in HttpGoogleServicesDAO
    // Note that the role in this map is just the suffix, as the org ID will vary depending on which context this test is run
    // We will check against just the suffix instead of the entire string
    val policyToBucketRole = Map("project-owner" -> "terraBucketWriter", "owner" -> "terraBucketWriter", "writer" -> "terraBucketWriter", "reader" -> "terraBucketReader")

    "should have correct policies in Sam and IAM roles in Google when an unconstrained workspace is created" in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)
      implicit val token: AuthToken = ownerAuthToken

      withCleanBillingProject(owner) { projectName =>
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
      }
    }

    "should have correct policies in Sam and IAM roles in Google when a constrained workspace is created" in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)
      implicit val token: AuthToken = ownerAuthToken

      withCleanBillingProject(owner) { projectName =>
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
      }
    }

    "should clone a workspace and only copy files in the specified path" in {
      implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 20 seconds)
      implicit val token: AuthToken = studentAToken

      withCleanBillingProject(studentA) { projectName =>
        withWorkspace(projectName, "test-copy-files", Set.empty) { workspaceName =>
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
          val cloneBucketName = Rawls.workspaces.getBucketName(projectName, destWorkspaceName)

          val start = System.currentTimeMillis()
          eventually {
            googleStorageDAO.listObjectsWithPrefix(GcsBucketName(cloneBucketName), "").futureValue.size shouldBe 1
          }
          val finish = System.currentTimeMillis()

          googleStorageDAO.listObjectsWithPrefix(GcsBucketName(cloneBucketName), "").futureValue.map(_.value) should contain only fileToCopy.value

          logger.info(s"Copied bucket files visible after ${finish-start} milliseconds")
        }
      }
    }

    "should support running workflows with private docker images" in {
      implicit val token: AuthToken = ownerAuthToken

      val privateMethod: Method = MethodData.SimpleMethod.copy(
        methodName = s"${UUID.randomUUID().toString()}-private_test_method",
        payload = "task hello {\n  String? name\n\n  command {\n    echo 'hello ${name}!'\n  }\n  output {\n    File response = stdout()\n  }\n  runtime {\n    docker: \"mtalbott/mtalbott-papi-v2\"\n  }\n}\n\nworkflow test {\n  call hello\n}"
      )

      withCleanBillingProject(owner) { projectName =>
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
              projectName, workspaceName,
              SimpleMethodConfig.configNamespace, SimpleMethodConfig.configName,
              "participant", SingleParticipant.entityId, "this", useCallCache = false)
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
      }
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

  private val serviceAccountEmailDomain = ".gserviceaccount.com"

  // Retrieves roles with policy emails for bucket acls and checks that service account is set up correctly
  private def getBucketRolesWithEmails(bucketName: GcsBucketName)(implicit patienceConfig: PatienceConfig): List[(String, Set[String])] = {
    GoogleStorageService.resource(RawlsConfig.pathToQAJson).use {
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
