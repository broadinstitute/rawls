package org.broadinstitute.dsde.test.api

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit.awaitCond
import akka.util.ByteString
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  WorkspaceCloudPlatform,
  WorkspaceResponse,
  WorkspaceType
}
import org.broadinstitute.dsde.rawls.dataaccess.LeonardoDAO
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.{Credentials, UserPool}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures.withTemporaryAzureBillingProject
import org.broadinstitute.dsde.workbench.service.test.CleanUp
import org.broadinstitute.dsde.workbench.service.{Rawls, RestException}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

@WorkspacesAzureTest
class AzureWorkspacesSpec extends AnyFlatSpec with Matchers with CleanUp {
  val owner: Credentials = UserPool.userConfig.Owners.getUserCredential("hermione")
  val leonardoConfig = LeonardoConfig(conf.getConfig("leonardo"))
  val leonardoDAO: LeonardoDAO =
    new HttpLeonardoDAO(leonardoConfig);

  private val azureManagedAppCoordinates = AzureManagedAppCoordinates(
    UUID.fromString("fad90753-2022-4456-9b0a-c7e5b934e408"),
    UUID.fromString("f557c728-871d-408c-a28b-eb6b2141a087"),
    "staticTestingMrg",
    Some(UUID.fromString("f41c1a97-179b-4a18-9615-5214d79ba600"))
  )

  private val wsmUrl = RawlsConfig.wsmUrl

  implicit val system = ActorSystem()

  "Rawls" should "allow creation and deletion of azure workspaces" in {
    implicit val token = owner.makeAuthToken()
    withTemporaryAzureBillingProject(azureManagedAppCoordinates) { projectName =>
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
      } finally {
        Rawls.workspaces.delete(projectName, workspaceName)
        assertNoAccessToWorkspace(projectName, workspaceName)
      }
    }
  }

  it should "allow creation and deletion of azure workspaces with wds" in {
    implicit val token = owner.makeAuthToken()
    withTemporaryAzureBillingProject(azureManagedAppCoordinates) { projectName =>
      val workspaceName = generateWorkspaceName()
      Rawls.workspaces.create(
        projectName,
        workspaceName,
        Set.empty,
        Map("disableAutomaticAppCreation" -> "false")
      )
      try {
        val response = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName))
        response.workspace.name should be(workspaceName)
        response.workspace.cloudPlatform should be(Some(WorkspaceCloudPlatform.Azure))
        response.workspace.workspaceType should be(Some(WorkspaceType.McWorkspace))
        withClue(s"Waiting for WDS app to be created") {
          awaitCond(
            isWDSCreated(response.workspace.workspaceId),
            60 seconds,
            2 seconds
          )
        }
        val appName = leonardoDAO.listAppsV2(token, response.workspace.workspaceId).getAppName
        //TODO: Check that database is responding and instance exists??

      } finally {
        leonardoDAO.deleteAppV2(token, response.workspace.workspaceId, appName)
        Rawls.workspaces.delete(projectName, workspaceName)
        assertNoAccessToWorkspace(projectName, workspaceName)
      }
    }
  }

  it should "allow access to WorkspaceManager API" in {
    implicit val token = owner.makeAuthToken()
    val statusRequest = Rawls.getRequest(wsmUrl + "status")

    withClue(s"WSM status API returned ${statusRequest.status.intValue()} ${statusRequest.status.reason()}!") {
      statusRequest.status shouldBe StatusCodes.OK
    }
  }

  it should "allow cloning of azure workspaces" in {
    implicit val token = owner.makeAuthToken()
    withTemporaryAzureBillingProject(azureManagedAppCoordinates) { projectName =>
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
        val sasUrl = getSasUrl(projectName, workspaceName)

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

          withClue(s"Verifying container cloning has completed") {
            awaitCond(
              isCloneCompleted(projectName, workspaceCloneName),
              60 seconds,
              2 seconds
            )
          }

          val cloneSasUrl = getSasUrl(projectName, workspaceCloneName)
          val downloadCloneContents = downloadBlob(cloneSasUrl, analysesFilename)
          withClue(s"testing blob ${analysesFilename} cloned") {
            downloadCloneContents shouldBe analysesContents
          }
          withClue(s"testing blob ${nonAnalysesFilename} did not clone") {
            verifyBlobNotCloned(cloneSasUrl, nonAnalysesFilename)
          }
        } finally {
          Rawls.workspaces.delete(projectName, workspaceCloneName)
          assertNoAccessToWorkspace(projectName, workspaceCloneName)
        }
      } finally {
        Rawls.workspaces.delete(projectName, workspaceName)
        assertNoAccessToWorkspace(projectName, workspaceName)
      }
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

  private def getSasUrl(projectName: String, workspaceName: String)(implicit token: AuthToken) = {
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

  private def isWDSCreated(workspaceId: String)(implicit token: AuthToken): Boolean = {
    val createdApps = leonardoDAO.listAppsV2(token, response.workspace.workspaceId)
    createdApps.getAppType == AppType.WDS && createdApps.getStatus == RUNNING
  }
}
