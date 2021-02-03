package org.broadinstitute.dsde.test.api

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import bio.terra.datarepo.api.RepositoryApi
import bio.terra.datarepo.client.ApiClient
import bio.terra.datarepo.model.{EnumerateSnapshotModel, SnapshotModel}
import bio.terra.workspace.model.{DataReferenceList, ReferenceTypeEnum}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import spray.json._
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.ServiceTestConfig.FireCloud
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.fixture.{BillingFixtures, WorkspaceFixtures}
import org.broadinstitute.dsde.workbench.service.Rawls
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


class SnapshotAPISpec extends AnyFreeSpecLike with Matchers with BeforeAndAfterAll
  with WorkspaceFixtures with BillingFixtures
  with SprayJsonSupport with LazyLogging
  with Eventually {

  private val dataRepoBaseUrl = FireCloud.dataRepoApiUrl

  override protected def beforeAll(): Unit = {
    assert(Try(Uri.parseAbsolute(dataRepoBaseUrl)).isSuccess,
      s"---> Aborting! Tests in this suite would fail because [$dataRepoBaseUrl] is not a valid url for data repo." +
        s" This is a problem in test config, not in the runtime code. <---")
  }

  "TDR Snapshot integration" - {
    //as of this writing, hermione.owner is the user with access to snapshots

    "should be able to contact Data Repo" taggedAs(Tags.AlphaTest, Tags.ExcludeInFiab) in {
      // status API is unauthenticated, but all our utility methods expect a token.
      // so, we'll send a token to the unauthenticated API to make this code path easier.
      val owner = UserPool.userConfig.Owners.getUserCredential("hermione")
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()
      val statusRequest = Rawls.getRequest(dataRepoBaseUrl + "status")

      withClue(s"Data Repo status API returned ${statusRequest.status.intValue()} ${statusRequest.status.reason()}!") {
        statusRequest.status shouldBe StatusCodes.OK // compare
      }
    }

    "should allow snapshot references to be added to workspaces" taggedAs(Tags.AlphaTest, Tags.ExcludeInFiab) in {
      val owner = UserPool.userConfig.Owners.getUserCredential("hermione")

      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()

      withCleanBillingProject(owner) { projectName =>
        withWorkspace(projectName, s"${UUID.randomUUID().toString}-snapshot references") { workspaceName =>

          val drSnapshots = listDataRepoSnapshots(2, owner)(ownerAuthToken)

          val dataRepoSnapshotId = drSnapshots.getItems.get(0).getId
          val anotherDataRepoSnapshotId = drSnapshots.getItems.get(1).getId

          // add snapshot reference to the workspace. Under the covers, this creates the workspace in WSM and adds the ref
          createSnapshotReference(projectName, workspaceName, dataRepoSnapshotId, "firstSnapshot")

          // validate the snapshot was added correctly: list snapshots in Rawls, should return 1, which we just added.
          // if we can successfully list snapshot references, it means WSM created its copy of the workspace
          val firstListResponse = listSnapshotReferences(projectName, workspaceName)
          val firstResources = Rawls.parseResponseAs[DataReferenceList](firstListResponse).getResources.asScala
          firstResources.size shouldBe 1
          firstResources.head.getName shouldBe "firstSnapshot"
          firstResources.head.getReferenceType shouldBe ReferenceTypeEnum.DATA_REPO_SNAPSHOT
          firstResources.head.getReference.getSnapshot shouldBe dataRepoSnapshotId

          // add a second snapshot reference to the workspace. Under the covers, this recognizes the workspace
          // already exists in WSM, so it just adds the ref
          createSnapshotReference(projectName, workspaceName, anotherDataRepoSnapshotId, "secondSnapshot")

          // validate the second snapshot was added correctly: list snapshots in Rawls, should return 2, which we just added
          val secondListResponse = listSnapshotReferences(projectName, workspaceName)
          // sort by reference name for easy predictability inside this test: "firstSnapshot" is before "secondSnapshot"
          val secondResources = Rawls.parseResponseAs[DataReferenceList](secondListResponse)
            .getResources.asScala.sortBy(_.getName)
          secondResources.size shouldBe 2
          secondResources.head.getName shouldBe "firstSnapshot"
          secondResources.head.getReference.getSnapshot shouldBe dataRepoSnapshotId
          secondResources.head.getReferenceType shouldBe ReferenceTypeEnum.DATA_REPO_SNAPSHOT
          secondResources(1).getName shouldBe "secondSnapshot"
          secondResources(1).getReference.getSnapshot shouldBe anotherDataRepoSnapshotId
          secondResources(1).getReferenceType shouldBe ReferenceTypeEnum.DATA_REPO_SNAPSHOT
        }
      }
    }

    "should report the same tables/columns via metadata API as TDR reports" taggedAs(Tags.AlphaTest, Tags.ExcludeInFiab) in {
      val numSnapshotsToVerify = 2

      val owner = UserPool.userConfig.Owners.getUserCredential("hermione")

      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()

      // get N snapshots from TDR
      val drSnapshots = listDataRepoSnapshots(numSnapshotsToVerify, owner)(ownerAuthToken)

      withCleanBillingProject(owner) { projectName =>
        withWorkspace(projectName, s"${UUID.randomUUID().toString}-snapshot references") { workspaceName =>
          // loop through each snapshot, and:
          drSnapshots.getItems.asScala.foreach { snapSummary =>
            info(s"for snapshot ${snapSummary.getId}: ${snapSummary.getName}")

            // workspace manager enforces validation on reference names:
            // "Name must be 1 to 63 alphanumeric characters or underscores, and cannot start with an underscore."
            // so, we replace all dashes (in the uuid) with underscores
            val referenceName = s"refname_${snapSummary.getId}".replaceAll("-", "_")

            // register the snapshot in Rawls
            createSnapshotReference(projectName, workspaceName, snapSummary.getId, referenceName)

            // call TDR to describe the snapshot; this includes table metadata
            val tdrModel = describeDataRepoSnapshot(snapSummary.getId, owner)(ownerAuthToken)

            // call Rawls' entity-type-metadata API for this snapshot reference; this includes table metadata
            val rawlsModel: Map[String, EntityTypeMetadata] = getEntityTypeMetadata(projectName, workspaceName, referenceName)

            // assert the two versions of metadata have the same tables
            val tdrTables = tdrModel.getTables.asScala
            val tdrTableNames = tdrTables.map(_.getName).toSet
            val rawlsTableNames = rawlsModel.keySet

            withClue(s"Rawls and TDR did not describe the same snapshot tables for reference $referenceName:") {
              rawlsTableNames should contain theSameElementsAs(tdrTableNames)
            }

            // for each table, assert the two versions of metadata have the same column names
            tdrTables.foreach { table =>
              val tdrColumnNames = table.getColumns.asScala.map(_.getName).toSet
              val rawlsColumnNames = rawlsModel(table.getName).attributeNames.toSet
              withClue(s"Rawls and TDR did not describe the same column names for table '${table.getName}' in reference $referenceName:") {
                rawlsColumnNames should contain theSameElementsAs(tdrColumnNames)
              }
            }
          }
        }
      }

    }

    "should be able to run analysis on a snapshot" taggedAs(Tags.AlphaTest, Tags.ExcludeInFiab) in {
      val owner = UserPool.userConfig.Owners.getUserCredential("hermione")

      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken()

      withCleanBillingProject(owner) { projectName =>
        withWorkspace(projectName, s"${UUID.randomUUID().toString}-snapshot references") { workspaceName =>

          val drSnapshot = listDataRepoSnapshots(1, owner)(ownerAuthToken)
          val dataRepoSnapshotId = drSnapshot.getItems.get(0).getId

          val snapshotName = "snapshotReferenceForAnalysis"
          // add snapshot reference to the workspace. Under the covers, this creates the workspace in WSM and adds the ref
          createSnapshotReference(projectName, workspaceName, dataRepoSnapshotId, snapshotName)

          // validate the snapshot was added correctly: list snapshots in Rawls, should return 1, which we just added.
          // if we can successfully list snapshot references, it means WSM created its copy of the workspace
          val listResponse = listSnapshotReferences(projectName, workspaceName)

          val resources = Rawls.parseResponseAs[DataReferenceList](listResponse).getResources.asScala
          resources.size shouldBe 1
          resources.head.getName shouldBe snapshotName
          resources.head.getReferenceType shouldBe ReferenceTypeEnum.DATA_REPO_SNAPSHOT
          resources.head.getReference.getSnapshot shouldBe dataRepoSnapshotId

          // create method config in a workspace
          val createMethodConfigUrl  = Uri(Rawls.url).withPath(Path(s"/api/workspaces/$projectName/$workspaceName/methodconfigs"))
          // TODO: consider using MethodConfiguration case class when AS-623 is done.
          val methodRepoMethod = Map(
            "methodUri" -> "agora://gatk/echo_to_file/9",
            "methodName" -> "echo_to_file",
            "methodNamespace" -> "gatk",
            "methodVersion" -> 9
          )
          val createMethodConfigPayload = Map(
            "methodRepoMethod" -> methodRepoMethod,
            "name" -> "echo_to_file-configured",
            "namespace" -> "gatk",
            "rootEntityType" -> "vcf_file",
            "prerequisites" -> Map(),
            "inputs" -> Map("echo_strings.echo_to_file.input1" -> "this.VCF_File_Name"),
            "outputs" -> Map("echo_strings.echo_to_file.out" -> "workspace.output"),
            "methodConfigVersion" -> 1,
            "deleted" -> false,
            "dataReferenceName" -> snapshotName
          )
          Rawls.postRequest(
            uri = createMethodConfigUrl.toString(),
            content = createMethodConfigPayload)

          // run analysis on the snapshot
          val createSubmissionUrl  = Uri(Rawls.url).withPath(Path(s"/api/workspaces/$projectName/$workspaceName/submissions"))
          // TODO: consider using 'SubmissionRequest' case class when AS-623 is done
          val createSubmissionPayload = Map(
            "useCallCache" -> true,
            "deleteIntermediateOutputFiles" -> false,
            "methodConfigurationNamespace" -> "gatk",
            "methodConfigurationName" -> "echo_to_file-configured"
          )
          val response = Rawls.postRequest(
            uri = createSubmissionUrl.toString(),
            content = createSubmissionPayload)

          // use spray-json here to parse into SubmissionReport. Jackson has trouble parsing the 'status' field
          // into SubmissionStatus (which is contained in SubmissionReport) object.
          val submissionId = response.parseJson.convertTo[SubmissionReport].submissionId

          // wait for submission to complete
          Submission.waitUntilSubmissionComplete(projectName, workspaceName, submissionId)

          // verify submission status is done
          val expectedSubmissionStatus = "Done"
          val actualSubmissionStatus = Submission.getSubmissionStatus(projectName, workspaceName, submissionId)
          withClue(s"Submission $projectName/$workspaceName/$submissionId status should be $expectedSubmissionStatus") {
            actualSubmissionStatus shouldBe expectedSubmissionStatus
          }

          // verify workflows succeeded
          val getSubmissionUrl  = Uri(Rawls.url).withPath(Path(s"/api/workspaces/$projectName/$workspaceName/submissions/$submissionId"))
          val submissionResponse = Rawls.parseResponse(Rawls.getRequest(uri = getSubmissionUrl.toString))

          // use spray-json here to parse into Submission. Jackson has trouble parsing the 'status' field
          // into SubmissionStatus (which is contained in Submission) object.
          val workflows: Seq[Workflow] = submissionResponse.parseJson.convertTo[Submission].workflows
          workflows.foreach { workflow =>
            val expectedWorkflowStatus = "Succeeded"
            val actualWorkflowStatus = workflow.status.toString
            withClue(s"Unexpected status: '${actualWorkflowStatus}'") {
              actualWorkflowStatus shouldBe expectedWorkflowStatus
            }
          }

          }

      }
    }

  }

  // ==================== Rawls helpers ====================
  private def listSnapshotReferences(projectName: String, workspaceName: String, offset: Int = 0, limit: Int = 10)(implicit authToken: AuthToken) = {
    val targetRawlsUrl  = Uri(Rawls.url)
      .withPath(Path(s"/api/workspaces/$projectName/$workspaceName/snapshots"))
      .withQuery(Query(Map("offset" -> offset.toString, "limit" ->  limit.toString)))
    Rawls.getRequest(uri = targetRawlsUrl.toString)
  }

  private def createSnapshotReference(projectName: String, workspaceName: String, snapshotId: String, snapshotName: String)(implicit authToken: AuthToken) = {
    val targetRawlsUrl  = Uri(Rawls.url).withPath(Path(s"/api/workspaces/$projectName/$workspaceName/snapshots"))
    val payload = Map("snapshotId" -> snapshotId, "name" -> snapshotName)
    Rawls.postRequest(
      uri = targetRawlsUrl.toString(),
      content = payload)
  }

  private def getEntityTypeMetadata(projectName: String, workspaceName: String, snapRefName: String)(implicit authToken: AuthToken): Map[String, EntityTypeMetadata] = {
    val targetRawlsUrl  = Uri(Rawls.url)
      .withPath(Path(s"/api/workspaces/$projectName/$workspaceName/entities"))
      .withQuery(Query(Map("dataReference" -> snapRefName)))
    val response = Rawls.getRequest(uri = targetRawlsUrl.toString)

    // workbench-libs' serviceTest code - e.g. Rawls.parseResponseAs - fails to correctly parse Maps from json.
    // I don't know why, and I am reluctant to change that code just to make this test work. We should fix it
    // at the source at some point.
    val respString = Rawls.parseResponse(response)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val metadataResponseObject = mapper.readValue[Map[String, EntityTypeMetadata]](respString)

    metadataResponseObject
  }

  // ==================== Data Repo helpers ====================
  private def listDataRepoSnapshots(numSnapshots: Int, credentials: Credentials)(implicit authToken: AuthToken): EnumerateSnapshotModel = {
    // call data repo to list snapshots
    // this gets the most recent snapshots in TDR (to which we have read access). This can cause tests to change
    // over time, if the snapshots keep changing. It's here for convenience - we can always add/remove snapshots from
    // TDR and this code will always pick up the latest. But if it causes flakiness we could change to sorting by
    // created_date ASC - that should be more stable.
    val dataRepoApi = new TestDataRepoDAO("terra", dataRepoBaseUrl).getRepositoryApi(authToken)

    logger.info(s"calling data repo at $dataRepoBaseUrl as user ${credentials.email} ... ")
    val drSnapshots = Try(dataRepoApi.enumerateSnapshots(
      0, numSnapshots, "created_date", "desc", "")) match {
      case Success(s) => s
      case Failure(ex) =>
        logger.error(s"data repo call as user ${credentials.email} failed: ${ex.getMessage}", ex)
        throw ex
    }
    assume(drSnapshots.getItems.size() == numSnapshots,
      s"---> TDR at $dataRepoBaseUrl did not have $numSnapshots snapshots for this test to use!" +
        s" This is likely a problem in environment setup, but has a chance of being a problem in runtime code. <---")

    logger.info(s"found ${drSnapshots.getItems.size()} snapshot(s) from $dataRepoBaseUrl as user ${credentials.email}: " +
      s"${drSnapshots.getItems.asScala.map(_.getId).mkString(", ")}")

    drSnapshots
  }

  private def describeDataRepoSnapshot(snapshotId: String, credentials: Credentials)(implicit authToken: AuthToken): SnapshotModel = {
    val dataRepoApi = new TestDataRepoDAO("terra", dataRepoBaseUrl).getRepositoryApi(authToken)

    Try(dataRepoApi.retrieveSnapshot(snapshotId)) match {
      case Success(s) => s
      case Failure(ex) =>
        logger.error(s"data repo call as user ${credentials.email} failed: ${ex.getMessage}", ex)
        throw ex
    }
  }


  // a bastardized version of the HttpDataRepoDAO in the main rawls codebase
  class TestDataRepoDAO(dataRepoInstanceName: String, dataRepoInstanceBasePath: String) {

    private def getApiClient(accessToken: String): ApiClient = {
      val client: ApiClient = new ApiClient()
      client.setBasePath(dataRepoInstanceBasePath)
      client.setAccessToken(accessToken)

      client
    }

    def getRepositoryApi(accessToken: AuthToken): RepositoryApi = {
      new RepositoryApi(getApiClient(accessToken.value))
    }
  }

}
