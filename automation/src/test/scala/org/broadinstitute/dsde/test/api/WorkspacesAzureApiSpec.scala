package org.broadinstitute.dsde.test.api

import com.typesafe.scalalogging.LazyLogging
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit.awaitCond
import akka.util.ByteString
import com.google.api.client.auth.oauth2.TokenResponse
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import org.apache.commons.lang3.StringUtils
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.ProjectOwner
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{WorkspaceCloudPlatform, WorkspaceResponse, WorkspaceType}
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

import io.circe._
import io.circe.parser._
import io.circe.generic.semiauto._

/**
  * Enum-like sealed trait representing the user type.
  */
sealed trait UserType { def title: String }

/**
  * Enum-like user type for owners.
  */
case object Owner extends UserType { def title = "owner" }

/**
  * Enum-like user type for regular users.
  */
case object Regular extends UserType { def title = "regular" }

/**
  * Companion object containing some useful methods for UserType.
  */
object UserType {
  implicit val userTypeDecoder: Decoder[UserType] = Decoder.decodeString.emap {
    case "owner" => Right(Owner)
    case "regular" => Right(Regular)
    case other => Left(s"Unknown user type: $other")
  }
}

/**
  * Represents metadata associated with a user.
  *
  * @param email  The email address associated with the user.
  * @param type   An instance of UserType (e.g., "Owner or Regular).
  * @param bearer The Bearer token to assert authorization.
  */
case class UserMetadata(email: String, `type`: UserType, bearer: String)

/**
  * Companion object containing some useful methods for UserMetadata.
  */
object UserMetadata {
  implicit val userMetadataDecoder: Decoder[UserMetadata] = deriveDecoder[UserMetadata]
}

/**
  * A proxy authentication token that represents a user. This class extends the base
  * `AuthToken` and includes additional user-related information such as user metadata
  * and the associated Google credential.
  *
  * @param userData    The user metadata associated with the authentication token.
  * @param credential The Google credential associated with the authentication token.
  */
case class MockAuthToken(userData: UserMetadata, credential: GoogleCredential) extends AuthToken {
  //private def userAuth(userData: UserMetadata): Map[String, String] =
  //  Map("email" -> System.getProperty(userType.id + "Email"),
  //      "bearerToken" -> System.getProperty(userType.id + "BearerToken")
  //  )

  override def buildCredential(): GoogleCredential = {
    logger.info("MockAuthToken.buildCredential() called...")
    credential.setAccessToken(userData.bearer)
    logger.info("Access token: " + credential.getAccessToken)
    credential
  }

  // lazy val email: String = userAuth(userType).getOrElse("email", StringUtils.EMPTY)

  // lazy val bearerToken = userAuth(userType).getOrElse("bearerToken", StringUtils.EMPTY)

  // lazy val bearerToken = userData.bearer
}

@WorkspacesAzureTest
class AzureWorkspacesSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with LazyLogging with CleanUp {
  var ownerAuthToken: MockAuthToken = _
  var nonOwnerAuthToken: MockAuthToken = _
  var billingProject: String = _

  private val wsmUrl = RawlsConfig.wsmUrl

  implicit val system = ActorSystem()

  val jsonString =
    """
      |[
      |  {
      |    "email": "hermione.owner@quality.firecloud.org",
      |    "type": "owner",
      |    "bearer": "yada yada"
      |  },
      |  {
      |    "email": "harry.potter@quality.firecloud.org",
      |    "type": "regular",
      |    "bearer": "yada yada"
      |  },
      |  {
      |    "email": "ron.weasley@quality.firecloud.org",
      |    "type": "regular",
      |    "bearer": "yada yada"
      |  }
      |]
  """.stripMargin

  var usersMetadata: Seq[UserMetadata] = _

  logger.info("Default usersMetadata")
  println(usersMetadata)

  override def beforeAll(): Unit = {
    //ownerAuthToken = MockAuthToken(Owner, (new MockGoogleCredential.Builder()).build())
    //nonOwnerAuthToken = MockAuthToken(NonOwner, (new MockGoogleCredential.Builder()).build())
    //nonOwnerAuthToken.buildCredential().refreshToken()
    billingProject = sys.env.getOrElse("BILLING_PROJECT", "")
    logger.info("billingProject: " + billingProject)

    usersMetadata = decode[Seq[UserMetadata]](jsonString).getOrElse(Seq())
    // println(usersMetadata)

    sys.env.get("USERS_METADATA_JSON") match {
      case Some(s) =>
        val decoded = decode[Seq[UserMetadata]](s)
        decoded match {
          case Right(u) =>
            usersMetadata = u
          case Left(error) => ()
        }
      case _ => ()
    }

    // println("usersMetadata: " + usersMetadata)

    ownerAuthToken = MockAuthToken(
      usersMetadata.filter(_.`type` == Owner).head,
      (new MockGoogleCredential.Builder()).build())
    // ownerAuthToken.buildCredential()

    // logger.info("ownerAuthToken: " + ownerAuthToken.credential.getAccessToken)

    nonOwnerAuthToken = MockAuthToken(
      usersMetadata.filter(_.`type` == Regular).head,
      (new MockGoogleCredential.Builder()).build())
    // nonOwnerAuthToken.buildCredential()

    // logger.info("nonOwnerAuthToken: " + nonOwnerAuthToken.credential.getAccessToken)
  }

  "Rawls" should "allow creation and deletion of azure workspaces" in {
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

  it should "allow access to WorkspaceManager API" in {
    // implicit val token = owner.makeAuthToken()
    implicit val token = ownerAuthToken
    val statusRequest = Rawls.getRequest(wsmUrl + "status")

    withClue(s"WSM status API returned ${statusRequest.status.intValue()} ${statusRequest.status.reason()}!") {
      statusRequest.status shouldBe StatusCodes.OK
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
      } finally {
        Rawls.workspaces.delete(projectName, workspaceCloneName)
        assertNoAccessToWorkspace(projectName, workspaceCloneName)
      }
    } finally {
      Rawls.workspaces.delete(projectName, workspaceName)
      assertNoAccessToWorkspace(projectName, workspaceName)
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

      // nonOwner is not a member of the workspace, should not be able to write
      // val userToken = nonOwner.makeAuthToken()
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
}
