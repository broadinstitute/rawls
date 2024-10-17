package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{StatusCodes, _}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import bio.terra.workspace.model.{ErrorReport => _}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceService}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import spray.json.DefaultJsonProtocol._
import spray.json.{enrichAny, JsObject}

import java.util.UUID
import scala.concurrent.Future

class WorkspaceApiServiceSpec
    extends AnyFlatSpec
    with TableDrivenPropertyChecks
    with Matchers
    with MockitoTestUtils
    with ScalatestRouteTest
    with SprayJsonSupport {
  implicit val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  object testData {
    val userInfo: UserInfo = UserInfo(RawlsUserEmail("owner-access"),
                                      OAuth2BearerToken("token"),
                                      123,
                                      RawlsUserSubjectId("123456789876543212345")
    )
    val testContext: RawlsRequestContext = RawlsRequestContext(userInfo)

    def currentTime() = new DateTime()

    val testDate: DateTime = currentTime()

    val wsName: WorkspaceName = WorkspaceName("myNamespace", "myWorkspace")

    val wsAttrs: AttributeMap = Map(
      AttributeName.withDefaultNS("string") -> AttributeString("yep, it's a string"),
      AttributeName.withDefaultNS("number") -> AttributeNumber(10),
      AttributeName.withDefaultNS("empty") -> AttributeValueEmptyList,
      AttributeName.withDefaultNS("values") -> AttributeValueList(
        Seq(AttributeString("another string"), AttributeString("true"))
      )
    )

    val workspace: Workspace = Workspace(
      wsName.namespace,
      wsName.name,
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      wsAttrs,
      false,
      WorkspaceVersions.V2,
      GoogleProjectId(UUID.randomUUID().toString),
      Option(GoogleProjectNumber(UUID.randomUUID().toString)),
      Option(RawlsBillingAccountName("fakeBillingAcct")),
      None,
      Option(currentTime()),
      WorkspaceType.RawlsWorkspace,
      WorkspaceState.Ready
    )

  }

  behavior of "WorkspaceApiService"

  it should "call workspaceService.listWorkspaces" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspace = testData.workspace
    val details = WorkspaceDetails(workspace, Set())
    val responseWorkspace = WorkspaceListResponse(WorkspaceAccessLevels.Read, None, None, details, None, false, None)
    val response = Seq(responseWorkspace).toJson
    val workspaceService = mock[WorkspaceService]
    when(workspaceService.listWorkspaces(any(), any())).thenReturn(Future.successful(response))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get("/workspaces") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[Seq[WorkspaceListResponse]]
        resp.head shouldBe responseWorkspace
      }
    verify(workspaceService).listWorkspaces(WorkspaceFieldSpecs(), -1) // empty seq and -1 are the default values
  }

  it should "call MCWorkspaceService.createMultiCloudOrRawlsWorkspace to create a workspace" in {
    val workspaceService = mock[WorkspaceService]
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspace = testData.workspace
    val newWorkspace = WorkspaceRequest(
      namespace = workspace.namespace,
      name = workspace.name,
      Map.empty
    )
    val details = WorkspaceDetails(workspace, Set())
    when(mcWorkspaceService.createMultiCloudOrRawlsWorkspace(newWorkspace, workspaceService))
      .thenReturn(Future.successful(details))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Post("/workspaces", newWorkspace.toJson) ~>
      service.testRoutes ~>
      check {
        status should be(StatusCodes.Created)
        val response = responseAs[WorkspaceDetails]
        response.name shouldBe workspace.name
      }
    verify(mcWorkspaceService).createMultiCloudOrRawlsWorkspace(newWorkspace, workspaceService, null)
  }

  private val tagsTestParameters = Table[String, Option[String], Option[Int]](
    ("queryString", "userQuery", "limit"),
    ("q=query&limit=5", Some("query"), Some(5)),
    ("/workspaces/tags?limit=&q=", Some(""), None),
    ("", None, None)
  )

  it should "call the workspace service to get tags with user query and limit" in {
    forAll(tagsTestParameters) { (queryString: String, userQuery: Option[String], limit: Option[Int]) =>
      val workspaceService = mock[WorkspaceService]
      val mcWorkspaceService = mock[MultiCloudWorkspaceService]
      val service = new MockApiService(
        workspaceServiceConstructor = _ => workspaceService,
        multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
      )
      val tag = WorkspaceTag("test-tag", 1)
      when(workspaceService.getTags(any, any)).thenReturn(Future.successful(Seq(tag)))

      Get("/workspaces/tags?" + queryString) ~>
        service.testRoutes ~>
        check { _: RouteTestResult =>
          status shouldBe StatusCodes.OK
          val resp = responseAs[Seq[WorkspaceTag]]
          resp.head shouldBe tag
        }
      verify(workspaceService).getTags(userQuery, limit)
    }
  }

  it should "get a workspace by id from the workspace service" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspace = testData.workspace
    val details = WorkspaceDetails(workspace, Set())
    val responseWorkspace = WorkspaceResponse(None, None, None, None, details, None, None, None, None, None)
    val response: JsObject = responseWorkspace.toJson.asJsObject
    val workspaceService = mock[WorkspaceService]
    val params = WorkspaceFieldSpecs(Some(Set("a", "b", "c")))
    when(workspaceService.getWorkspaceById(workspace.workspaceId, params)).thenReturn(Future.successful(response))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get(s"/workspaces/id/${workspace.workspaceId}?fields=a,b,c") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkspaceResponse]
        resp shouldBe responseWorkspace
      }
    verify(workspaceService).getWorkspaceById(workspace.workspaceId, params)
  }

  it should "get a workspace by name and namespace from the workspace service" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspace = testData.workspace
    val details = WorkspaceDetails(workspace, Set())
    val responseWorkspace = WorkspaceResponse(None, None, None, None, details, None, None, None, None, None)
    val response: JsObject = responseWorkspace.toJson.asJsObject
    val workspaceService = mock[WorkspaceService]
    when(workspaceService.getWorkspace(workspace.toWorkspaceName, WorkspaceFieldSpecs(None)))
      .thenReturn(Future.successful(response))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkspaceResponse]
        resp shouldBe responseWorkspace
      }
    verify(workspaceService).getWorkspace(workspace.toWorkspaceName, WorkspaceFieldSpecs(None))
  }

  it should "pass the fields parameter when getting a workspace by name and namespace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspace = testData.workspace
    val details = WorkspaceDetails(workspace, Set())
    val responseWorkspace = WorkspaceResponse(None, None, None, None, details, None, None, None, None, None)
    val response: JsObject = responseWorkspace.toJson.asJsObject
    val workspaceService = mock[WorkspaceService]
    val params = WorkspaceFieldSpecs(Some(Set("a", "b", "c")))
    when(workspaceService.getWorkspace(workspace.toWorkspaceName, params)).thenReturn(Future.successful(response))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get(s"/workspaces/${workspace.namespace}/${workspace.name}?fields=a,b,c") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkspaceResponse]
        resp shouldBe responseWorkspace
      }
    verify(workspaceService).getWorkspace(workspace.toWorkspaceName, params)
  }

  it should "update the workspace by name and namespace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceName = WorkspaceName("ns", "n")
    val update =
      Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation)
    val details = WorkspaceDetails(testData.workspace, Set())
    val workspaceService = mock[WorkspaceService]
    when(workspaceService.updateWorkspace(any, any)).thenReturn(Future.successful(details))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Patch(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}", update.toJson) ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkspaceDetails]
        resp shouldBe details
      }
    verify(workspaceService).updateWorkspace(workspaceName, update)
  }

  it should "delete the workspace by name and namespace" in {
    forAll(
      Table(
        ("bucketResult", "message"),
        (None, "Your workspace has been deleted."),
        (Some("BucketName"), s"Your Google bucket BucketName will be deleted within 24h.")
      )
    ) { (bucketResult, message) =>
      val mcWorkspaceService = mock[MultiCloudWorkspaceService]
      val workspaceName = WorkspaceName("ns", "n")
      val workspaceService = mock[WorkspaceService]
      when(mcWorkspaceService.deleteMultiCloudOrRawlsWorkspace(workspaceName, workspaceService))
        .thenReturn(Future.successful(bucketResult))
      val service = new MockApiService(
        workspaceServiceConstructor = _ => workspaceService,
        multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
      )
      Delete(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}") ~>
        service.testRoutes ~>
        check {
          status shouldBe StatusCodes.Accepted
          responseAs[String] shouldBe message

        }
      verify(mcWorkspaceService).deleteMultiCloudOrRawlsWorkspace(workspaceName, workspaceService)
    }
  }

  it should "get accessInstructions by name and namespace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceName = WorkspaceName("ns", "n")
    val workspaceService = mock[WorkspaceService]
    val serviceResponse = Seq[ManagedGroupAccessInstructions]()
    when(workspaceService.getAccessInstructions(workspaceName)).thenReturn(Future.successful(serviceResponse))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/accessInstructions") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[Seq[ManagedGroupAccessInstructions]]
        resp shouldBe serviceResponse
      }

    verify(workspaceService).getAccessInstructions(workspaceName)
  }

  it should "get bucketOptions by name and namespace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceName = WorkspaceName("ns", "n")
    val workspaceService = mock[WorkspaceService]
    val serviceResponse = WorkspaceBucketOptions(requesterPays = true)
    when(workspaceService.getBucketOptions(workspaceName)).thenReturn(Future.successful(serviceResponse))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/bucketOptions") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkspaceBucketOptions]
        resp shouldBe serviceResponse
      }

    verify(workspaceService).getBucketOptions(workspaceName)
  }

  it should "clone a workspace using the multicloud workspace service" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspace = testData.workspace
    val workspaceName = workspace.toWorkspaceName
    val workspaceService = mock[WorkspaceService]
    val cloneWorkspace = WorkspaceRequest(
      namespace = "cloneNamespace",
      name = "cloneName",
      Map.empty
    )
    val details =
      WorkspaceDetails(workspace.copy(namespace = cloneWorkspace.namespace, name = cloneWorkspace.name), Set())
    when(mcWorkspaceService.createMultiCloudOrRawlsWorkspace(any, any, any))
      .thenReturn(Future.successful(details))

    when(mcWorkspaceService.cloneMultiCloudWorkspace(workspaceService, workspaceName, cloneWorkspace))
      .thenReturn(Future.successful(details))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Post(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/clone", cloneWorkspace.toJson) ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.Created
        val resp = responseAs[WorkspaceDetails]
        resp shouldBe details
      }

    verify(mcWorkspaceService).cloneMultiCloudWorkspace(workspaceService, workspaceName, cloneWorkspace)
  }

  it should "get the workspace ACL by name and namespace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceName = WorkspaceName("ns", "n")
    val workspaceService = mock[WorkspaceService]
    val serviceResponse = WorkspaceACL(acl = Map("a" -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false)))
    when(workspaceService.getACL(workspaceName)).thenReturn(Future.successful(serviceResponse))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/acl") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkspaceACL]
        resp shouldBe serviceResponse
      }

    verify(workspaceService).getACL(workspaceName)
  }

  it should "update the workspace ACL for the patch operation" in {
    forAll(
      Table(
        ("queryString", "inviteMissingUsersValue"),
        ("?inviteUsersNotFound=true", true),
        ("?inviteUsersNotFound=false", false),
        ("?inviteUsersNotFound=", false),
        ("?", false),
        ("", false)
      )
    ) { (queryString, inviteMissingUsersValue) =>
      val mcWorkspaceService = mock[MultiCloudWorkspaceService]
      val workspaceName = WorkspaceName("ns", "n")
      val workspaceService = mock[WorkspaceService]
      val update: Set[WorkspaceACLUpdate] = Set(
        WorkspaceACLUpdate("email1@test.com", WorkspaceAccessLevels.Read, None, None),
        WorkspaceACLUpdate("email2@test.com", WorkspaceAccessLevels.NoAccess),
        WorkspaceACLUpdate("email3@test.com", WorkspaceAccessLevels.Owner, Some(true), Some(false))
      )
      val serviceResponse = WorkspaceACLUpdateResponseList(
        Set(WorkspaceACLUpdate("email1@test.com", WorkspaceAccessLevels.Read, None, None)),
        Set(WorkspaceACLUpdate("email2@test.com", WorkspaceAccessLevels.NoAccess)),
        Set(WorkspaceACLUpdate("email3@test.com", WorkspaceAccessLevels.Owner, Some(true), Some(false)))
      )
      when(workspaceService.updateACL(workspaceName, update, inviteMissingUsersValue))
        .thenReturn(Future.successful(serviceResponse))
      val service = new MockApiService(
        workspaceServiceConstructor = _ => workspaceService,
        multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
      )
      Patch(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/acl" + queryString, update.toJson) ~>
        service.testRoutes ~>
        check {
          status shouldBe StatusCodes.OK
          val resp = responseAs[WorkspaceACLUpdateResponseList]
          resp shouldBe serviceResponse
        }

      verify(workspaceService).updateACL(workspaceName, update, inviteMissingUsersValue)
    }
  }

  it should "update the workspace library attributes" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspace = testData.workspace
    val workspaceName = workspace.toWorkspaceName
    val details = WorkspaceDetails(workspace, Set())
    val update: Seq[AttributeUpdateOperation] = Seq(
      AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation
    )
    when(workspaceService.updateLibraryAttributes(workspaceName, update)).thenReturn(Future.successful(details))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Patch(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/library", update.toJson) ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkspaceDetails]
        resp shouldBe details
      }

    verify(workspaceService).updateLibraryAttributes(workspaceName, update)
  }

  it should "get the workspace catalog" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspaceName = testData.workspace.toWorkspaceName
    val response = Set(
      WorkspaceCatalog("email1@test.com", true),
      WorkspaceCatalog("email2@test.com", false)
    )
    when(workspaceService.getCatalog(workspaceName)).thenReturn(Future.successful(response))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/catalog") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[Set[WorkspaceCatalog]]
        resp shouldBe response
      }

    verify(workspaceService).getCatalog(workspaceName)
  }

  it should "update the workspace catalog" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspaceName = testData.workspace.toWorkspaceName
    val update: Seq[WorkspaceCatalog] = Seq(
      WorkspaceCatalog("email@test.com", true)
    )
    val response = WorkspaceCatalogUpdateResponseList(
      Seq(WorkspaceCatalogResponse(UUID.randomUUID().toString, true)),
      Seq("email1@test.com")
    )
    when(workspaceService.updateCatalog(workspaceName, update)).thenReturn(Future.successful(response))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Patch(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/catalog", update.toJson) ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkspaceCatalogUpdateResponseList]
        resp shouldBe response
      }

    verify(workspaceService).updateCatalog(workspaceName, update)
  }

  it should "check the bucket read access" in {
    forAll(
      Table[Option[RawlsException], StatusCode](
        ("exception", "statusCode"),
        (None, StatusCodes.OK),
        (Some(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "message"))), StatusCodes.Forbidden),
        (Some(new RawlsException("user has workspace read-only access yet ...")), StatusCodes.InternalServerError)
      )
    ) { (exception: Option[Throwable], statusCode: StatusCode) =>
      val mcWorkspaceService = mock[MultiCloudWorkspaceService]
      val workspaceService = mock[WorkspaceService]
      val workspaceName = testData.workspace.toWorkspaceName
      val serviceResponse: Future[Unit] = exception.map(e => Future.failed(e)).getOrElse(Future.successful())

      when(workspaceService.checkWorkspaceCloudPermissions(workspaceName)).thenReturn(serviceResponse)

      val service = new MockApiService(
        workspaceServiceConstructor = _ => workspaceService,
        multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
      )
      Get(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/checkBucketReadAccess") ~>
        service.testRoutes ~>
        check {
          status shouldBe statusCode
        }
      verify(workspaceService).checkWorkspaceCloudPermissions(workspaceName)
    }
  }

  it should "check the permission with checkIamActionWithLock on the workspace" in {
    forAll(
      Table(
        ("accessResult", "statusCode"),
        (true, StatusCodes.NoContent),
        (false, StatusCodes.Forbidden)
      )
    ) { (accessResult, statusCode) =>
      val mcWorkspaceService = mock[MultiCloudWorkspaceService]
      val workspaceService = mock[WorkspaceService]
      val workspaceName = testData.workspace.toWorkspaceName

      when(workspaceService.checkSamActionWithLock(workspaceName, SamWorkspaceActions.read))
        .thenReturn(Future.successful(accessResult))
      val service = new MockApiService(
        workspaceServiceConstructor = _ => workspaceService,
        multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
      )
      Get(
        s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/checkIamActionWithLock/${SamWorkspaceActions.read.value}"
      ) ~>
        service.testRoutes ~>
        check {
          status shouldBe statusCode
        }

      verify(workspaceService).checkSamActionWithLock(workspaceName, SamWorkspaceActions.read)
    }
  }

  it should "get the file transfers for the workspace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspaceName = testData.workspace.toWorkspaceName

    val response = Seq(
      PendingCloneWorkspaceFileTransfer(
        UUID.randomUUID(),
        "source",
        "dest",
        "prefix",
        GoogleProjectId("projectId"),
        testData.currentTime(),
        None,
        None
      )
    )
    when(workspaceService.listPendingFileTransfersForWorkspace(workspaceName)).thenReturn(Future.successful(response))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/fileTransfers") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[Seq[PendingCloneWorkspaceFileTransfer]]
        resp shouldBe response
      }

    verify(workspaceService).listPendingFileTransfersForWorkspace(workspaceName)
  }

  it should "lock the workspace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspaceName = testData.workspace.toWorkspaceName

    when(workspaceService.lockWorkspace(workspaceName)).thenReturn(Future.successful(true))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Put(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/lock") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.NoContent
      }

    verify(workspaceService).lockWorkspace(workspaceName)
  }

  it should "unlock the workspace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspaceName = testData.workspace.toWorkspaceName

    when(workspaceService.unlockWorkspace(workspaceName)).thenReturn(Future.successful(true))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Put(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/unlock") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.NoContent
      }

    verify(workspaceService).unlockWorkspace(workspaceName)
  }

  it should "get the bucket usage for the workspace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspaceName = testData.workspace.toWorkspaceName
    val response = BucketUsageResponse(BigInt(83729), None)
    when(workspaceService.getBucketUsage(workspaceName)).thenReturn(Future.successful(response))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Get(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/bucketUsage") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[BucketUsageResponse]
        resp shouldBe response
      }

    verify(workspaceService).getBucketUsage(workspaceName)
  }

  it should "call sendChangeNotifications on the workspace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspaceName = testData.workspace.toWorkspaceName
    val response = "12"
    when(workspaceService.sendChangeNotifications(workspaceName)).thenReturn(Future.successful(response))
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Post(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/sendChangeNotification") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[String]
        resp shouldBe response
      }

    verify(workspaceService).sendChangeNotifications(workspaceName)
  }

  it should "call enableRequesterPaysForLinkedSAs for the workspace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspaceName = testData.workspace.toWorkspaceName
    when(workspaceService.enableRequesterPaysForLinkedSAs(workspaceName)).thenReturn(Future.successful())
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Put(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/enableRequesterPaysForLinkedServiceAccounts") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.NoContent
      }

    verify(workspaceService).enableRequesterPaysForLinkedSAs(workspaceName)
  }

  it should "call disableRequesterPaysForLinkedSAs on the workspace" in {
    val mcWorkspaceService = mock[MultiCloudWorkspaceService]
    val workspaceService = mock[WorkspaceService]
    val workspaceName = testData.workspace.toWorkspaceName
    when(workspaceService.disableRequesterPaysForLinkedSAs(workspaceName)).thenReturn(Future.successful())
    val service = new MockApiService(
      workspaceServiceConstructor = _ => workspaceService,
      multiCloudWorkspaceServiceConstructor = _ => mcWorkspaceService
    )
    Put(s"/workspaces/${workspaceName.namespace}/${workspaceName.name}/disableRequesterPaysForLinkedServiceAccounts") ~>
      service.testRoutes ~>
      check {
        status shouldBe StatusCodes.NoContent
      }

    verify(workspaceService).disableRequesterPaysForLinkedSAs(workspaceName)
  }
}
