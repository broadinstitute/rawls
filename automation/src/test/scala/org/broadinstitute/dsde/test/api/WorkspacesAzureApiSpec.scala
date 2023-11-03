package org.broadinstitute.dsde.test.api

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit.awaitCond
import akka.util.ByteString
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.ProjectOwner
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{
  WorkspaceCloudPlatform,
  WorkspaceListResponse,
  WorkspaceResponse,
  WorkspaceType
}
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.service.test.CleanUp
import org.broadinstitute.dsde.workbench.service.{Orchestration, Rawls, RestException, WorkspaceAccessLevel}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import org.broadinstitute.dsde.test.pipeline._

@WorkspacesAzureTest
class WorkspacesAzureApiSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with CleanUp {
  // The values of the following vars are injected from the pipeline.
  var billingProject: String = _
  var ownerAuthToken: ProxyAuthToken = _
  var nonOwnerAuthToken: ProxyAuthToken = _

  private val wsmUrl = RawlsConfig.wsmUrl
  private val leoUrl = RawlsConfig.leoUrl

  implicit val system = ActorSystem()

  override def beforeAll(): Unit = {
    val bee = PipelineInjector(PipelineInjector.e2eEnv())
    billingProject = bee.billingProject
    bee.Owners.getUserCredential("hermione") match {
      case Some(owner) =>
        ownerAuthToken = owner.makeAuthToken
      case _ => ()
    }
    bee.chooseStudent match {
      case Some(student) =>
        nonOwnerAuthToken = student.makeAuthToken
      case _ => ()
    }
  }

  "Other Terra services" should "include Leonardo" in {
    implicit val token = ownerAuthToken
    val statusRequest = Rawls.getRequest(leoUrl + "status")

    withClue(s"Leo status API returned ${statusRequest.status.intValue()} ${statusRequest.status.reason()}!") {
      statusRequest.status shouldBe StatusCodes.OK
    }
  }

  it should "include WorkspaceManager" in {
    implicit val token = ownerAuthToken
    val statusRequest = Rawls.getRequest(wsmUrl + "status")

    withClue(s"WSM status API returned ${statusRequest.status.intValue()} ${statusRequest.status.reason()}!") {
      statusRequest.status shouldBe StatusCodes.OK
    }
  }

  "Rawls" should "allow creation and deletion of an Azure workspace without WDS" in {
    implicit val token = ownerAuthToken
    val projectName = billingProject
    val workspaceName = generateWorkspaceName()
    Rawls.workspaces.create(
      projectName,
      workspaceName,
      Set.empty,
      Map("disableAutomaticAppCreation" -> "true")
    )
    try {
      val response = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName))
      response.workspace.name should be(workspaceName)
      response.workspace.cloudPlatform should be(Some(WorkspaceCloudPlatform.Azure))
      response.workspace.workspaceType should be(Some(WorkspaceType.McWorkspace))
      response.accessLevel should be(Some(ProjectOwner))
    } finally {
      Rawls.workspaces.delete(projectName, workspaceName)
      assertNoAccessToWorkspace(projectName, workspaceName)
    }
  }

  it should "allow cloning of azure workspaces" in {
    implicit val token = ownerAuthToken
    val projectName = billingProject
    val workspaceName = generateWorkspaceName()
    val workspaceCloneName = generateWorkspaceName()

    val analysesDir = "analyses"
    val analysesFilename = analysesDir + "/testFile.txt"
    val analysesContents = "hello world"

    val nonAnalysesFilename = "willNotClone.txt"
    val nonAnalysesContents = "user upload content"

    Rawls.workspaces.create(
      projectName,
      workspaceName,
      Set.empty,
      Map("disableAutomaticAppCreation" -> "true")
    )
    try {
      val sasUrl = getSasUrl(projectName, workspaceName, token)

      // Upload the blob that will be cloned
      uploadBlob(sasUrl, analysesFilename, analysesContents)
      val downloadContents = downloadBlob(sasUrl, analysesFilename)
      withClue(s"testing uploaded blob ${analysesFilename}") {
        downloadContents shouldBe analysesContents
      }

      // Upload the blob that should not be cloned
      uploadBlob(sasUrl, nonAnalysesFilename, nonAnalysesContents)
      val downloadNonAnalysesContents = downloadBlob(sasUrl, nonAnalysesFilename)
      withClue(s"testing uploaded blob ${nonAnalysesFilename}") {
        downloadNonAnalysesContents shouldBe nonAnalysesContents
      }

      Rawls.workspaces.clone(
        projectName,
        workspaceName,
        projectName,
        workspaceCloneName,
        Set.empty,
        Some(analysesDir),
        Map("disableAutomaticAppCreation" -> "true")
      )
      try {
        val clonedResponse = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceCloneName))
        clonedResponse.workspace.name should equal(workspaceCloneName)
        clonedResponse.workspace.cloudPlatform should be(Some(WorkspaceCloudPlatform.Azure))
        clonedResponse.workspace.workspaceType should be(Some(WorkspaceType.McWorkspace))
        clonedResponse.accessLevel should be(Some(ProjectOwner))

        withClue(s"Verifying container cloning has completed") {
          awaitCond(
            isCloneCompleted(projectName, workspaceCloneName),
            60 seconds,
            2 seconds
          )
        }

        val cloneSasUrl = getSasUrl(projectName, workspaceCloneName, token)
        val downloadCloneContents = downloadBlob(cloneSasUrl, analysesFilename)
        withClue(s"testing blob ${analysesFilename} cloned") {
          downloadCloneContents shouldBe analysesContents
        }
        withClue(s"testing blob ${nonAnalysesFilename} did not clone") {
          verifyBlobNotCloned(cloneSasUrl, nonAnalysesFilename)
        }
      } finally
        withClue(s"deleting the cloned workspace ${workspaceCloneName} failed") {
          Rawls.workspaces.delete(projectName, workspaceCloneName)
          assertNoAccessToWorkspace(projectName, workspaceCloneName)
        }
    } finally
      withClue(s"deleting the original workspace ${workspaceName} failed") {
        Rawls.workspaces.delete(projectName, workspaceName)
        assertNoAccessToWorkspace(projectName, workspaceName)
      }
  }

  it should "allow listing workspaces" in {
    implicit val token = ownerAuthToken
    val projectName = billingProject
    val workspaceName1 = generateWorkspaceName()
    val workspaceName2 = generateWorkspaceName()

    try {
      Rawls.workspaces.create(
        projectName,
        workspaceName1,
        Set.empty,
        Map("disableAutomaticAppCreation" -> "true")
      )
      Rawls.workspaces.create(
        projectName,
        workspaceName2,
        Set.empty,
        Map("disableAutomaticAppCreation" -> "true")
      )

      val workspaces = Rawls.workspaces.list().parseJson.convertTo[Seq[WorkspaceListResponse]]

      workspaces.length shouldBe 2
      workspaces.map(_.workspace.name).toSet shouldBe Set(workspaceName1, workspaceName2)
    } finally {
      Rawls.workspaces.delete(projectName, workspaceName1)
      assertNoAccessToWorkspace(projectName, workspaceName1)

      Rawls.workspaces.delete(projectName, workspaceName2)
      assertNoAccessToWorkspace(projectName, workspaceName2)
    }
  }

  it should "allow sharing a workspace" in {
    implicit val token = ownerAuthToken
    val projectName = billingProject
    val workspaceName = generateWorkspaceName()
    Rawls.workspaces.create(
      projectName,
      workspaceName,
      Set.empty,
      Map("disableAutomaticAppCreation" -> "true")
    )
    try {
      val response = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName))
      response.workspace.name should be(workspaceName)
      response.workspace.cloudPlatform should be(Some(WorkspaceCloudPlatform.Azure))

      val userToken = nonOwnerAuthToken
      eventually {
        intercept[Exception] {
          getSasUrl(projectName, workspaceName, userToken)
        }
      }

      // Make nonOwner a writer
      Orchestration.workspaces.updateAcl(
        projectName,
        workspaceName,
        nonOwnerAuthToken.userData.email,
        WorkspaceAccessLevel.Writer,
        Some(false),
        Some(false)
      )
      // Verify can get a Sas URL to write to workspace
      getSasUrl(projectName, workspaceName, userToken)

      // Remove write access
      Orchestration.workspaces.updateAcl(
        projectName,
        workspaceName,
        nonOwnerAuthToken.userData.email,
        WorkspaceAccessLevel.NoAccess,
        Some(false),
        Some(false)
      )
      eventually {
        intercept[Exception] {
          getSasUrl(projectName, workspaceName, userToken)
        }
      }
    } finally {
      Rawls.workspaces.delete(projectName, workspaceName)
      assertNoAccessToWorkspace(projectName, workspaceName)
    }
  }

  it should "allow creation and deletion of an Azure workspace with WDS" in {
    implicit val token = ownerAuthToken
    val projectName = billingProject
    val workspaceName = generateWorkspaceName()
    Rawls.workspaces.create(projectName, workspaceName)

    try {
      val response = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName))
      response.workspace.name should be(workspaceName)
      response.workspace.cloudPlatform should be(Some(WorkspaceCloudPlatform.Azure))
      val workspaceId = response.workspace.workspaceId
      val creationTimeout = 600

      withClue(s"WDS did not become deletable within the timeout period of ${creationTimeout} seconds") {
        awaitCond(
          isWdsDeletable(workspaceId, token),
          creationTimeout seconds,
          20 seconds
        )
      }
    } finally {
      Rawls.workspaces.delete(projectName, workspaceName, 600)
      assertNoAccessToWorkspace(projectName, workspaceName)
    }
  }

  private def generateWorkspaceName(): String =
    s"${UUID.randomUUID().toString()}-azure-test-workspace"

  private def assertExceptionStatusCode(exception: RestException, statusCode: Int): Unit =
    exception.message.parseJson.asJsObject.fields("statusCode").convertTo[Int] should be(statusCode)

  private def assertNoAccessToWorkspace(projectName: String, workspaceName: String)(implicit token: AuthToken): Unit =
    eventually {
      val exception = intercept[RestException](Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(token))
      assertExceptionStatusCode(exception, 404)
    }

  private def workspaceResponse(response: String): WorkspaceResponse = response.parseJson.convertTo[WorkspaceResponse]

  private def getWorkspaceId(projectName: String, workspaceName: String)(implicit token: AuthToken): String =
    Rawls.workspaces
      .getWorkspaceDetails(projectName, workspaceName)
      .parseJson
      .asJsObject
      .getFields("workspace")
      .flatMap { workspace =>
        workspace.asJsObject.getFields("workspaceId")
      }
      .head
      .convertTo[String]

  private def isCloneCompleted(projectName: String, workspaceName: String)(implicit token: AuthToken): Boolean = {
    val cloneTransferComplete = Rawls.workspaces
      .getWorkspaceDetails(projectName, workspaceName)
      .parseJson
      .asJsObject
      .getFields("workspace")
      .flatMap { workspace =>
        workspace.asJsObject.getFields("completedCloneWorkspaceFileTransfer")
      }
    cloneTransferComplete.headOption.isDefined
  }

  private def uploadBlob(containerUrl: String, blobName: String, contents: String): Unit = {
    val urlParts = containerUrl.split("\\?")
    val fullBlobUrl = urlParts.head + s"/${blobName}?" + urlParts.tail.head
    val headers = List(RawHeader("x-ms-blob-type", "BlockBlob"))

    val uploadRequest =
      HttpRequest(HttpMethods.PUT, fullBlobUrl, headers, HttpEntity(ContentTypes.`text/plain(UTF-8)`, contents))
    val uploadResponse = Await.result(Http().singleRequest(uploadRequest), 2.minutes)

    withClue(s"Upload blob ${blobName}") {
      uploadResponse.status shouldBe StatusCodes.Created
    }
  }

  private def downloadBlob(containerUrl: String, blobName: String): String = {
    val downloadResponse = getBlobResponse(containerUrl, blobName)
    withClue(s"Download blob ${blobName}") {
      downloadResponse.status shouldBe StatusCodes.OK
    }
    val byteStringSink: Sink[ByteString, Future[ByteString]] = Sink.fold(ByteString("")) { (z, i) =>
      z.concat(i)
    }
    val entityFuture = downloadResponse.entity.dataBytes.runWith(byteStringSink)
    Await.result(entityFuture, 1.second).decodeString("UTF-8")
  }

  private def verifyBlobNotCloned(containerUrl: String, blobName: String): Unit = {
    val downloadResponse = getBlobResponse(containerUrl, blobName)
    withClue(s"Check blob ${blobName} does not exist") {
      downloadResponse.status shouldBe StatusCodes.NotFound
    }
  }

  private def getBlobResponse(containerUrl: String, blobName: String): HttpResponse = {
    val urlParts = containerUrl.split("\\?")
    val fullBlobUrl = urlParts.head + s"/${blobName}?" + urlParts.tail.head
    val downloadRequest = HttpRequest(HttpMethods.GET, fullBlobUrl)
    Await.result(Http().singleRequest(downloadRequest), 2.minutes)
  }

  private def getSasUrl(projectName: String, workspaceName: String, authToken: AuthToken) = {
    implicit val token = authToken
    val workspaceId = getWorkspaceId(projectName, workspaceName)
    val resourceResponse = Rawls.parseResponse(
      Rawls.getRequest(wsmUrl + s"api/workspaces/v1/${workspaceId}/resources?stewardship=CONTROLLED&limit=1000")
    )
    val containerId = resourceResponse.parseJson.asJsObject
      .getFields("resources")
      .head
      .asInstanceOf[JsArray]
      .elements
      .head
      .asJsObject
      .getFields("metadata")
      .head
      .asJsObject
      .getFields("resourceId")
      .head
      .convertTo[String]

    val sasResponse = Rawls.postRequest(
      wsmUrl + s"api/workspaces/v1/${workspaceId}/resources/controlled/azure/storageContainer/${containerId}/getSasToken"
    )
    sasResponse.parseJson.asJsObject.getFields("url").head.convertTo[String]
  }

  private def isWdsDeletable(workspaceId: String, authToken: AuthToken) = {
    implicit val token = authToken

    val appResponse = Rawls.parseResponse(Rawls.getRequest(leoUrl + s"api/apps/v2/${workspaceId}/wds-${workspaceId}"))
    val wdsStatus = appResponse.parseJson.asJsObject.getFields("status").head.convertTo[String]
    logger.info(s"WDS is in status ${wdsStatus}")
    wdsStatus.toLowerCase match {
      case "running" => true
      case "error"   => true
      case _         => false
    }
  }
}
