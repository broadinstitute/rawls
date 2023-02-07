package org.broadinstitute.dsde.test.api

import akka.actor.ActorSystem
import akka.testkit.TestKit
import bio.terra.profile.api.ProfileApi
import bio.terra.workspace.api.LandingZonesApi
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.client.ApiException
import bio.terra.profile.client.{ApiClient => BpmApiClient}
import bio.terra.profile.model.{CloudPlatform, CreateProfileRequest}
import bio.terra.workspace.model.{CreateAzureLandingZoneRequestBody, JobControl}
import org.apache.http.HttpStatus
import org.broadinstitute.dsde.rawls.model.Attributable.{AttributeMap, workspaceIdAttribute}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.auth.AuthTokenScopes.billingScopes
import org.broadinstitute.dsde.workbench.auth.{AuthToken, AuthTokenScopes}
import org.broadinstitute.dsde.workbench.config.{Credentials, ServiceTestConfig, UserPool}
import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures.withTemporaryBillingProject
import org.broadinstitute.dsde.workbench.fixture._
import org.broadinstitute.dsde.workbench.google.{GoogleCredentialModes, HttpGoogleIamDAO, HttpGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.service._
import org.broadinstitute.dsde.workbench.service.test.{CleanUp, RandomUtil}
import org.broadinstitute.dsde.workbench.util.Retry
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Seconds, Span}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

//noinspection JavaAccessorEmptyParenCall,TypeAnnotation
// reenable test in Jenkins
@WorkspacesTest
class WorkspaceApiSpec
  extends TestKit(ActorSystem("MySpec"))
    with AnyFreeSpecLike
    with Matchers
    with Eventually
    with CleanUp
    with RandomUtil
    with Retry
    with ScalaFutures
    with WorkspaceFixtures
    with MethodFixtures {

  val Seq(studentA, studentB) = UserPool.chooseStudents(2)
  val studentAToken: AuthToken = studentA.makeAuthToken()
  val studentBToken: AuthToken = studentB.makeAuthToken()

  val owner: Credentials = UserPool.chooseProjectOwner
  val ownerAuthToken: AuthToken = owner.makeAuthToken()

  val operations = Array(Map("op" -> "AddUpdateAttribute", "attributeName" -> "participant1", "addUpdateAttribute" -> "testparticipant"))
  val entity: Array[Map[String, Any]] = Array(Map("name" -> "participant1", "entityType" -> "participant", "operations" -> operations))
  val billingAccountId: String = ServiceTestConfig.Projects.billingAccountId

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(1, Minutes)), interval = scaled(Span(20, Seconds)))


  "Rawls" - {


    def getWsmClient(token: String) = {
      val client: ApiClient = new ApiClient()
      client.setBasePath("http://localhost:8080")
      client.setAccessToken(token)

      client
    }

    def getBpmClient(token: String) = {
      val client: BpmApiClient = new BpmApiClient()
      client.setBasePath("http://localhost:8081")
      client.setAccessToken(token)

      client
    }


    "should do stuff" in {
      val owner: Credentials = UserPool.chooseStudent
      implicit val ownerAuthToken: AuthToken = owner.makeAuthToken(AuthTokenScopes.userLoginScopes ++ Seq("https://www.googleapis.com/auth/cloud-platform"))
      val bpmClient = getBpmClient(ownerAuthToken.value)
      val profileApi = new ProfileApi(bpmClient)


      val createProfileRequest = new CreateProfileRequest()
      val bpId = UUID.fromString("4e274601-00f5-42ba-bbe5-3b969f2fb46a") //UUID.randomUUID()

      createProfileRequest.setId(bpId)
      createProfileRequest.setBiller("direct")
      createProfileRequest.setTenantId(UUID.fromString("fad90753-2022-4456-9b0a-c7e5b934e408"))
      createProfileRequest.setSubscriptionId(UUID.fromString("df547342-9cfd-44ef-a6dd-df0ede32f1e3"))
      createProfileRequest.setManagedResourceGroupId("terraIntegrationTesting3")
      createProfileRequest.setDisplayName("terraIntegrationTesting3")
      createProfileRequest.setCloudPlatform(CloudPlatform.AZURE)
      profileApi.createProfile(
        createProfileRequest
      )
      val wsmClient = getWsmClient(ownerAuthToken.value)
      val lzApi = new LandingZonesApi(wsmClient)
      val createLandingZoneRequest = new CreateAzureLandingZoneRequestBody()
      createLandingZoneRequest.setVersion("v1")
      createLandingZoneRequest.setDefinition("CromwellBaseResourcesFactory")

      createLandingZoneRequest.setBillingProfileId(bpId)

      val jobId = new JobControl
      jobId.setId(UUID.randomUUID().toString)

      createLandingZoneRequest.setJobControl(jobId)

      val lzId = UUID.fromString("cb93c510-c46f-464a-8bd3-2ed019a1f857")
      try {
        lzApi.attachAzureLandingZone(
          createLandingZoneRequest, lzId
        )
      } catch {
        case e: ApiException =>
          if (!e.getResponseBody.contains("already exists")) {
            throw e;
          } else {
            logger.info(s"LZ ${lzId} already exists, skipping creation.")
          }
      }

      /**
        * TODO
        * 1. Extract a context helper withAzureBillingProject() that won't tear down the LZ but *will* delete the BP
        * (likely requires internal rawls changes)
        * 2. Extract a context helper withAzureWorkspace() that creates a workspace and tears it down
        * (ensure WSM is running with janitor => ON)
        * 3. Wire up LZ attachment
        */
      val billingProjectName = s"azureBp-${UUID.randomUUID()}".substring(0, 29)
      val result = Rawls.billingV2.createBillingProject(billingProjectName, "", None)
      val workspaceName = s"azure-workspace-tmp-${UUID.randomUUID().toString}"
      Rawls.workspaces.create(billingProjectName, workspaceName)
      val createdWorkspaceResponse = workspaceResponse(Rawls.workspaces.getWorkspaceDetails(billingProjectName, workspaceName))
      assert(createdWorkspaceResponse.workspace.name.equals(workspaceName))
    }
  }

  private def workspaceResponse(response: String): WorkspaceResponse = response.parseJson.convertTo[WorkspaceResponse]

  private def assertNoAccessToWorkspace(projectName: String, workspaceName: String)(implicit token: AuthToken): Unit = {
    eventually {
      val exception = intercept[RestException](Rawls.workspaces.getWorkspaceDetails(projectName, workspaceName)(token))
      assertExceptionStatusCode(exception, 404)
    }
  }

  private def assertExceptionStatusCode(exception: RestException, statusCode: Int): Unit = {
    exception.message.parseJson.asJsObject.fields("statusCode").convertTo[Int] should be(statusCode)
  }

  private def prependUUID(suffix: String): String = {
    s"${UUID.randomUUID().toString()}-$suffix"
  }
}
