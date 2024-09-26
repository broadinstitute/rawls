package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model.{
  AzureContext,
  IamRole,
  RoleBinding,
  RoleBindingList,
  WorkspaceDescription,
  WorkspaceStageModel
}
import com.google.api.client.googleapis.json.{GoogleJsonError, GoogleJsonResponseException}
import com.google.api.client.http.{HttpHeaders, HttpResponseException}
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.fastpass.{FastPassService, FastPassServiceImpl}
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model.WorkspaceType.WorkspaceType
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferServiceImpl
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterServiceImpl
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport, UserDisabledException}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchGroupName}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.dsl.MatcherWords.not.contain
import org.scalatest.matchers.must.Matchers.{include, not}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.prop.TableDrivenPropertyChecks
import spray.json.{JsArray, JsObject}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

/**
  * Unit tests kept separate from WorkspaceServiceSpec to separate true unit tests from tests requiring external resources
  */
class WorkspaceServiceUnitTests
    extends AnyFlatSpec
    with OptionValues
    with MockitoTestUtils
    with SprayJsonSupport
    with TableDrivenPropertyChecks {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  val workspace: Workspace = Workspace(
    "test-namespace",
    "test-name",
    UUID.randomUUID().toString,
    "aBucket",
    Some("workflow-collection"),
    new DateTime(),
    new DateTime(),
    "test",
    Map.empty
  )

  // This is just for convenience, so we only need to specify mocks we care about
  def workspaceServiceConstructor(
    executionServiceCluster: ExecutionServiceCluster = mock[ExecutionServiceCluster](RETURNS_SMART_NULLS),
    workspaceManagerDAO: WorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
    leonardoService: LeonardoService = mock[LeonardoService](RETURNS_SMART_NULLS),
    gcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
    samDAO: SamDAO = mock[SamDAO],
    notificationDAO: NotificationDAO = mock[NotificationDAO](RETURNS_SMART_NULLS),
    userServiceConstructor: RawlsRequestContext => UserService = _ => mock[UserService](RETURNS_SMART_NULLS),
    workbenchMetricBaseName: String = "",
    config: WorkspaceServiceConfig = mock[WorkspaceServiceConfig](RETURNS_SMART_NULLS),
    requesterPaysSetupService: RequesterPaysSetupServiceImpl = mock[RequesterPaysSetupServiceImpl](RETURNS_SMART_NULLS),
    resourceBufferService: ResourceBufferServiceImpl = mock[ResourceBufferServiceImpl](RETURNS_SMART_NULLS),
    servicePerimeterService: ServicePerimeterServiceImpl = mock[ServicePerimeterServiceImpl](RETURNS_SMART_NULLS),
    googleIamDao: GoogleIamDAO = mock[GoogleIamDAO](RETURNS_SMART_NULLS),
    terraBillingProjectOwnerRole: String = "",
    terraWorkspaceCanComputeRole: String = "",
    terraWorkspaceNextflowRole: String = "",
    terraBucketReaderRole: String = "",
    terraBucketWriterRole: String = "",
    billingProfileManagerDAO: BillingProfileManagerDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS),
    aclManagerDatasource: SlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS),
    fastPassServiceConstructor: RawlsRequestContext => FastPassService = _ =>
      mock[FastPassService](RETURNS_SMART_NULLS),
    workspaceRepository: WorkspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS),
    billingRepository: BillingRepository = mock[BillingRepository](RETURNS_SMART_NULLS)
  ): RawlsRequestContext => WorkspaceService = info =>
    new WorkspaceService(
      info,
      mock[SlickDataSource](RETURNS_SMART_NULLS),
      executionServiceCluster,
      workspaceManagerDAO,
      leonardoService,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      workbenchMetricBaseName,
      config,
      requesterPaysSetupService,
      resourceBufferService,
      servicePerimeterService,
      googleIamDao,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole,
      new RawlsWorkspaceAclManager(samDAO),
      new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, aclManagerDatasource),
      fastPassServiceConstructor,
      workspaceRepository,
      billingRepository
    )(scala.concurrent.ExecutionContext.global)

  behavior of "getWorkspaceById"

  it should "return the workspace on success" in {
    val sam = mock[SamDAO]
    when(sam.getUserStatus(defaultRequestContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      sam.userHasAction(SamResourceTypeNames.workspace,
                        workspace.workspaceId,
                        SamWorkspaceActions.read,
                        defaultRequestContext
      )
    ).thenReturn(Future(true))
    when(sam.getResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(Seq()))
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.workspaceIdAsUUID, Some(WorkspaceAttributeSpecs(false))))
      .thenReturn(Future(Some(workspace)))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any))
      .thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceRepository = repository,
      workspaceManagerDAO = wsm
    )(defaultRequestContext)

    val result = Await.result(
      service.getWorkspaceById(workspace.workspaceId, WorkspaceFieldSpecs(Some(Set("workspace")))),
      Duration.Inf
    )

    val fields = result.fields.get("workspace").get.asJsObject.getFields("name", "namespace")
    fields should contain(workspace.name)
    fields should contain(workspace.namespace)
  }

  it should "return an exception without the workspace name when the user can't read the workspace" in {
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.workspaceIdAsUUID, Some(WorkspaceAttributeSpecs(true))))
      .thenReturn(Future(Some(workspace)))
    val sam = mock[SamDAO]
    when(sam.getUserStatus(defaultRequestContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      sam.userHasAction(SamResourceTypeNames.workspace,
                        workspace.workspaceId,
                        SamWorkspaceActions.read,
                        defaultRequestContext
      )
    ).thenReturn(Future(false))

    val service = workspaceServiceConstructor(samDAO = sam, workspaceRepository = repository)(defaultRequestContext)

    val exception = intercept[NoSuchWorkspaceException] {
      Await.result(service.getWorkspaceById(workspace.workspaceId, WorkspaceFieldSpecs()), Duration.Inf)
    }

    exception.workspace shouldBe workspace.workspaceId
    exception.getMessage should (not include workspace.name)
    exception.getMessage should (not include workspace.namespace)
    verify(sam).userHasAction(SamResourceTypeNames.workspace,
                              workspace.workspaceId,
                              SamWorkspaceActions.read,
                              defaultRequestContext
    )
  }

  it should "return an exception with the workspaceId when no workspace is found" in {
    val sam = mock[SamDAO]
    when(sam.getUserStatus(defaultRequestContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.workspaceIdAsUUID, Some(WorkspaceAttributeSpecs(true))))
      .thenReturn(Future(None))

    val exception = intercept[NoSuchWorkspaceException] {
      val service = workspaceServiceConstructor(samDAO = sam, workspaceRepository = repository)(defaultRequestContext)
      Await.result(service.getWorkspaceById(workspace.workspaceId, WorkspaceFieldSpecs()), Duration.Inf)
    }

    exception.workspace shouldEqual workspace.workspaceId
  }

  behavior of "getWorkspace"

  it should "return the workspace on success" in {
    val sam = mock[SamDAO]
    when(sam.getUserStatus(defaultRequestContext)).thenReturn(Future(Some(SamUserStatusResponse("", "", true))))
    when(
      sam.userHasAction(SamResourceTypeNames.workspace,
                        workspace.workspaceId,
                        SamWorkspaceActions.read,
                        defaultRequestContext
      )
    ).thenReturn(Future(true))
    when(sam.getResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(Seq()))
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.toWorkspaceName, Some(WorkspaceAttributeSpecs(false))))
      .thenReturn(Future(Some(workspace)))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any)).thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceRepository = repository,
      workspaceManagerDAO = wsm
    )(defaultRequestContext)

    val result = Await.result(
      service.getWorkspace(workspace.toWorkspaceName, WorkspaceFieldSpecs(Some(Set("workspace")))),
      Duration.Inf
    )
    val fields = result.fields("workspace").asJsObject.getFields("name", "namespace")
    fields should contain(workspace.name)
    fields should contain(workspace.namespace)
  }

  it should "throw an exception when invalid fields are requested" in {
    val service = workspaceServiceConstructor()(defaultRequestContext)
    val invalidField = "thisFieldIsInvalid"
    val fields = WorkspaceFieldSpecs(Some(Set(invalidField)))

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getWorkspace(workspace.toWorkspaceName, fields), Duration.Inf)
    }

    exception.errorReport.message should include(invalidField)
  }

  it should "return an unauthorized error if the user is disabled" in {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    val samUserStatus = SamUserStatusResponse("sub", "email", enabled = false)
    when(samDAO.getUserStatus(ArgumentMatchers.eq(defaultRequestContext))).thenReturn(
      Future.successful(Some(samUserStatus))
    )

    val exception = intercept[UserDisabledException] {
      val service = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)
      Await.result(service.getWorkspace(WorkspaceName("fake_namespace", "fake_name"), WorkspaceFieldSpecs()),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Unauthorized)
  }

  behavior of "getWorkspaceDetails"

  it should "not preform operations for fields that are not requested" in {
    val options = WorkspaceService.QueryOptions(Set(), WorkspaceAttributeSpecs(false))
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any, any))
      .thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsmDao)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.canCompute shouldBe None
    result.catalog shouldBe None
    result.canShare shouldBe None
  }

  it should "check for the catalog permission in sam the field is requested" in {
    val options = WorkspaceService.QueryOptions(Set("catalog"), WorkspaceAttributeSpecs(false))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any)).thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val sam = mock[SamDAO]
    when(
      sam.userHasAction(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspaceActions.catalog,
        defaultRequestContext
      )
    ).thenReturn(Future(true))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsm, samDAO = sam)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.catalog shouldBe Some(true)
    verify(sam).userHasAction(SamResourceTypeNames.workspace,
                              workspace.workspaceId,
                              SamWorkspaceActions.catalog,
                              defaultRequestContext
    )
  }

  it should "return the highest access level in accessLevel" in {
    val options = WorkspaceService.QueryOptions(Set("accessLevel"), WorkspaceAttributeSpecs(false))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any))
      .thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(Set(SamResourceRole("READER"), SamResourceRole("OWNER"))))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsm, samDAO = sam)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.accessLevel shouldBe Some(WorkspaceAccessLevels.Owner)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext)
  }

  it should "return noaccess for accessLevel when sam return no roles for the user" in {
    // this isn't realistic, since the user should have at least read access to get here,
    // but it's the default specified
    val options = WorkspaceService.QueryOptions(Set("accessLevel"), WorkspaceAttributeSpecs(false))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any)).thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(Set()))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsm, samDAO = sam)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.accessLevel shouldBe Some(WorkspaceAccessLevels.NoAccess)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext)
  }

  it should "return true for canCompute if the user is an owner" in {
    val options = WorkspaceService.QueryOptions(Set("canCompute"), WorkspaceAttributeSpecs(false))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any))
      .thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(Set(SamResourceRole("OWNER"))))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsm, samDAO = sam)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.workspace.name shouldBe workspace.name
    result.workspace.namespace shouldBe workspace.namespace
    result.canCompute shouldBe Some(true)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext)
  }

  // TODO: finish with azure codepath
  ignore should "return true for canCompute if the user is a writer on an azure workspace" in {
    val workspace = this.workspace.copy(workspaceType = WorkspaceType.McWorkspace)
    val options = WorkspaceService.QueryOptions(Set("canCompute"), WorkspaceAttributeSpecs(false))
    val wsmDao = mock[WorkspaceManagerDAO]
    // val aggregatedWorkspace = new AggregatedWorkspace()
    when(wsmDao.getWorkspace(any, any))
    // .thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(Set(SamResourceRole("OWNER"))))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsmDao, samDAO = sam)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.workspace.name shouldBe workspace.name
    result.workspace.namespace shouldBe workspace.namespace
    result.canCompute shouldBe Some(true)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext)
  }

  it should "query sam for canCompute if the user is not an owner on a gcp workspace" in {
    val options = WorkspaceService.QueryOptions(Set("canCompute"), WorkspaceAttributeSpecs(false))
    val wsmDao = mock[WorkspaceManagerDAO]
    when(wsmDao.getWorkspace(any, any))
      .thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(Set(SamResourceRole("WRITER"))))
    when(
      sam.userHasAction(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspaceActions.compute,
        defaultRequestContext
      )
    ).thenReturn(Future(true))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsmDao, samDAO = sam)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.workspace.name shouldBe workspace.name
    result.workspace.namespace shouldBe workspace.namespace
    result.canCompute shouldBe Some(true)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext)
    verify(sam).userHasAction(
      SamResourceTypeNames.workspace,
      workspace.workspaceId,
      SamWorkspaceActions.compute,
      defaultRequestContext
    )
  }

  it should "return true for canShare if the user is a workspace or project owner" in {
    forAll(Table("role", "OWNER", "PROJECT_OWNER")) { (role: String) =>
      val options = WorkspaceService.QueryOptions(Set("canShare"), WorkspaceAttributeSpecs(false))
      val wsm = mock[WorkspaceManagerDAO]
      when(wsm.getWorkspace(any, any))
        .thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
      val sam = mock[SamDAO]
      when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
        .thenReturn(Future(Set(SamResourceRole(role))))
      val service = workspaceServiceConstructor(workspaceManagerDAO = wsm, samDAO = sam)(defaultRequestContext)

      val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

      result.workspace.name shouldBe workspace.name
      result.workspace.namespace shouldBe workspace.namespace
      result.canShare shouldBe Some(true)
      verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext)
    }
  }

  it should "query sam for canShare if the user is not an owner" in {
    forAll(
      Table(
        ("role", "samAnswer"),
        ("WRITER", true),
        ("READER", true),
        ("NO ACCESS", false)
      )
    ) { (role: String, samAnswer: Boolean) =>
      val options = WorkspaceService.QueryOptions(Set("canShare"), WorkspaceAttributeSpecs(false))
      val wsmDao = mock[WorkspaceManagerDAO]
      when(wsmDao.getWorkspace(any, any))
        .thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
      val sam = mock[SamDAO]
      when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
        .thenReturn(Future(Set(SamResourceRole(role))))
      when(
        sam.userHasAction(
          SamResourceTypeNames.workspace,
          workspace.workspaceId,
          SamWorkspaceActions.sharePolicy(role.toLowerCase),
          defaultRequestContext
        )
      ).thenReturn(Future(samAnswer))
      val service = workspaceServiceConstructor(workspaceManagerDAO = wsmDao, samDAO = sam)(defaultRequestContext)

      val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

      result.workspace.name shouldBe workspace.name
      result.workspace.namespace shouldBe workspace.namespace
      result.canShare shouldBe Some(samAnswer)
      verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext)
      verify(sam).userHasAction(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspaceActions.sharePolicy(role.toLowerCase),
        defaultRequestContext
      )
    }
  }

  it should "get the bucket options from gcs when requested" in {
    val options = WorkspaceService.QueryOptions(Set("bucketOptions"), WorkspaceAttributeSpecs(false))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any)).thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val gcs = mock[GoogleServicesDAO]
    val bucketDetails = WorkspaceBucketOptions(true)
    when(gcs.getBucketDetails(workspace.bucketName, workspace.googleProjectId)).thenReturn(Future(bucketDetails))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsm, gcsDAO = gcs)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.bucketOptions shouldBe Some(bucketDetails)
    verify(gcs).getBucketDetails(workspace.bucketName, workspace.googleProjectId)
  }

  it should "get the owner emails using the policy from sam when requested" in {
    val options = WorkspaceService.QueryOptions(Set("owners"), WorkspaceAttributeSpecs(false))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any)).thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val sam = mock[SamDAO]
    val ownerEmails = Set("user1@test.com", "user2@test.com")
    val owners = SamPolicy(ownerEmails.map(WorkbenchEmail), Set(), Set())
    when(
      sam.getPolicy(SamResourceTypeNames.workspace,
                    workspace.workspaceId,
                    SamWorkspacePolicyNames.owner,
                    defaultRequestContext
      )
    )
      .thenReturn(Future(owners))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsm, samDAO = sam)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.owners shouldBe Some(ownerEmails)
  }

  it should "get the auth domain from sam when requested" in {
    val options = WorkspaceService.QueryOptions(Set("workspace"), WorkspaceAttributeSpecs(false))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any)).thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val sam = mock[SamDAO]
    val authDomains = Seq("some-auth-domain")
    when(sam.getResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(authDomains))
    val service = workspaceServiceConstructor(workspaceManagerDAO = wsm, samDAO = sam)(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    val expectedAuthDomains = authDomains.map(authDomainName => ManagedGroupRef(RawlsGroupName(authDomainName))).toSet
    result.workspace.authorizationDomain shouldEqual Some(expectedAuthDomains)
  }

  it should "get the submissionSummaryStats when requested" in {
    val options = WorkspaceService.QueryOptions(Set("workspaceSubmissionStats"), WorkspaceAttributeSpecs(false))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.getWorkspace(any, any)).thenAnswer(_ => throw new AggregateWorkspaceNotFoundException(ErrorReport("")))
    val stats = WorkspaceSubmissionStats(None, None, 3)
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getSubmissionSummaryStats(workspace.workspaceIdAsUUID)).thenReturn(Future(Some(stats)))
    val service = workspaceServiceConstructor(
      workspaceManagerDAO = wsm,
      workspaceRepository = workspaceRepository
    )(defaultRequestContext)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.workspaceSubmissionStats shouldBe Some(stats)
  }

  behavior of "listWorkspaces"
  it should "return an empty response when the user has no workspaces" in {
    val sam = mock[SamDAO]
    when(sam.listUserResources(SamResourceTypeNames.workspace, defaultRequestContext))
      .thenReturn(Future(Seq()))
    val wsRepo = mock[WorkspaceRepository]
    when(wsRepo.listWorkspacesByIds(ArgumentMatchers.eq(Seq()), any)).thenReturn(Future(Seq()))
    when(wsRepo.listSubmissionSummaryStats(Seq())).thenReturn(Future(Map()))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.listWorkspaces(defaultRequestContext)).thenReturn(List())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceManagerDAO = wsm,
      workspaceRepository = wsRepo
    )(defaultRequestContext)

    val result = Await.result(service.listWorkspaces(WorkspaceFieldSpecs(), -1), Duration.Inf)

    result shouldBe JsArray.empty
  }

  it should "return workspaces the user has an access level role on" in {
    forAll(
      Table(
        ("role", "hasAccess"),
        (SamWorkspaceRoles.projectOwner, true),
        (SamWorkspaceRoles.owner, true),
        (SamWorkspaceRoles.writer, true),
        (SamWorkspaceRoles.reader, true),
        (SamWorkspaceRoles.canCatalog, false),
        (SamWorkspaceRoles.canCompute, false),
        (SamWorkspaceRoles.shareWriter, false),
        (SamWorkspaceRoles.shareReader, false)
      )
    ) { (role: SamResourceRole, hasAccess: Boolean) =>
      val workspaceSamResource = SamUserResource(
        workspace.workspaceId,
        SamRolesAndActions(Set(role), Set()),
        SamRolesAndActions(Set(), Set()),
        SamRolesAndActions(Set(), Set()),
        Set(),
        Set()
      )
      val sam = mock[SamDAO]
      when(sam.listUserResources(SamResourceTypeNames.workspace, defaultRequestContext))
        .thenReturn(Future(Seq(workspaceSamResource)))
      val wsRepo = mock[WorkspaceRepository]
      if (hasAccess) {
        when(wsRepo.listWorkspacesByIds(ArgumentMatchers.eq(Seq(workspace.workspaceIdAsUUID)), any))
          .thenReturn(Future(Seq(workspace)))
      } else {
        when(wsRepo.listWorkspacesByIds(ArgumentMatchers.eq(Seq()), any)).thenReturn(Future(Seq()))
      }
      val wsm = mock[WorkspaceManagerDAO]
      when(wsm.listWorkspaces(defaultRequestContext)).thenReturn(List())
      val service = workspaceServiceConstructor(
        samDAO = sam,
        workspaceManagerDAO = wsm,
        workspaceRepository = wsRepo
      )(defaultRequestContext)

      val params = WorkspaceFieldSpecs(fields = Some(Set("workspace", "accessLevel", "public")))
      val result = Await.result(service.listWorkspaces(params, -1), Duration.Inf)

      val resultWorkspace = result match {
        case jsa: JsArray =>
          jsa.elements.headOption.map {
            _.convertTo[WorkspaceListResponse]
          }
        case _ => None
      }
      hasAccess match {
        case false => resultWorkspace shouldBe None
        case true =>
          val details =
            WorkspaceDetails.fromWorkspaceAndOptions(workspace, Some(Set()), false, Some(WorkspaceCloudPlatform.Gcp))
          resultWorkspace.get.workspace shouldBe details
      }
    }
  }

  it should "return the highest access level" in {
    val workspaceSamResource = SamUserResource(
      workspace.workspaceId,
      SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set()),
      SamRolesAndActions(Set(SamWorkspaceRoles.reader), Set()),
      SamRolesAndActions(Set(), Set(SamWorkspaceActions.compute)),
      Set(),
      Set()
    )
    val sam = mock[SamDAO]
    when(sam.listUserResources(SamResourceTypeNames.workspace, defaultRequestContext))
      .thenReturn(Future(Seq(workspaceSamResource)))
    val wsRepo = mock[WorkspaceRepository]
    when(wsRepo.listWorkspacesByIds(ArgumentMatchers.eq(Seq(workspace.workspaceIdAsUUID)), any))
      .thenReturn(Future(Seq(workspace)))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.listWorkspaces(defaultRequestContext)).thenReturn(List())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceManagerDAO = wsm,
      workspaceRepository = wsRepo
    )(defaultRequestContext)

    val params = WorkspaceFieldSpecs(fields = Some(Set("workspace", "accessLevel", "public")))
    val result = Await.result(service.listWorkspaces(params, -1), Duration.Inf)

    val resultWorkspace = result match {
      case jsa: JsArray => jsa.elements.headOption.map(_.convertTo[WorkspaceListResponse])
      case _            => None
    }
    resultWorkspace.get.accessLevel shouldBe WorkspaceAccessLevels.Owner
  }

  it should "return the matching value of canShare" in {
    forAll(
      Table(
        ("highestAccessLevel", "additionalRoles", "canShareResult"),
        (SamWorkspaceRoles.owner, Set[SamResourceRole](), true),
        (SamWorkspaceRoles.projectOwner, Set[SamResourceRole](), true),
        (SamWorkspaceRoles.reader, Set[SamResourceRole](), false),
        (SamWorkspaceRoles.reader, Set[SamResourceRole](SamWorkspaceRoles.shareReader), true),
        (SamWorkspaceRoles.writer, Set[SamResourceRole](), false),
        (SamWorkspaceRoles.writer, Set[SamResourceRole](SamWorkspaceRoles.shareWriter), true)
      )
    ) { (highestAccessLevel: SamResourceRole, additionalRoles: Set[SamResourceRole], canShareResult: Boolean) =>
      val workspaceSamResource = SamUserResource(
        workspace.workspaceId,
        SamRolesAndActions(Set(highestAccessLevel), Set()),
        SamRolesAndActions(additionalRoles, Set()),
        SamRolesAndActions(Set(), Set(SamWorkspaceActions.compute)),
        Set(),
        Set()
      )
      val sam = mock[SamDAO]
      when(sam.listUserResources(SamResourceTypeNames.workspace, defaultRequestContext))
        .thenReturn(Future(Seq(workspaceSamResource)))
      val wsRepo = mock[WorkspaceRepository]
      when(wsRepo.listWorkspacesByIds(ArgumentMatchers.eq(Seq(workspace.workspaceIdAsUUID)), any))
        .thenReturn(Future(Seq(workspace)))
      val wsm = mock[WorkspaceManagerDAO]
      when(wsm.listWorkspaces(defaultRequestContext)).thenReturn(List())
      val service = workspaceServiceConstructor(
        samDAO = sam,
        workspaceManagerDAO = wsm,
        workspaceRepository = wsRepo
      )(defaultRequestContext)

      val params = WorkspaceFieldSpecs(fields = Some(Set("workspace", "public", "accessLevel", "canShare")))
      val result = Await.result(service.listWorkspaces(params, -1), Duration.Inf)

      val resultWorkspace = result match {
        case jsa: JsArray =>
          jsa.elements.headOption.map {
            _.convertTo[WorkspaceListResponse]
          }
        case _ => None
      }
      resultWorkspace.get.canShare.get shouldBe canShareResult
    }
  }

  it should "return the matching value of canCompute" in {
    forAll(
      Table(
        ("cloudPlatform", "highestAccessLevel", "additionalRoles", "canComputeResult"),
        (WorkspaceCloudPlatform.Azure, SamWorkspaceRoles.owner, Set[SamResourceRole](), true),
        (WorkspaceCloudPlatform.Azure, SamWorkspaceRoles.projectOwner, Set[SamResourceRole](), true),
        (WorkspaceCloudPlatform.Azure, SamWorkspaceRoles.writer, Set[SamResourceRole](), true),
        // can compute is not valid for azure workspaces - the user needs at least the writter permission
        (WorkspaceCloudPlatform.Azure,
         SamWorkspaceRoles.reader,
         Set[SamResourceRole](SamWorkspaceRoles.canCompute),
         false
        ),
        (WorkspaceCloudPlatform.Gcp, SamWorkspaceRoles.owner, Set[SamResourceRole](), true),
        (WorkspaceCloudPlatform.Gcp, SamWorkspaceRoles.projectOwner, Set[SamResourceRole](), true),
        (WorkspaceCloudPlatform.Gcp, SamWorkspaceRoles.writer, Set(SamWorkspaceRoles.canCompute), true),
        (WorkspaceCloudPlatform.Gcp, SamWorkspaceRoles.writer, Set[SamResourceRole](), false),
        (WorkspaceCloudPlatform.Gcp, SamWorkspaceRoles.reader, Set(SamWorkspaceRoles.canCompute), true),
        (WorkspaceCloudPlatform.Gcp, SamWorkspaceRoles.reader, Set[SamResourceRole](), false)
      )
    ) { (cloudPlatform, highestAccessLevel, additionalRoles, canComputeResult) =>
      val workspace = cloudPlatform match {
        case WorkspaceCloudPlatform.Gcp   => this.workspace
        case WorkspaceCloudPlatform.Azure => this.workspace.copy(workspaceType = WorkspaceType.McWorkspace)
      }
      val workspaceSamResource = SamUserResource(
        workspace.workspaceId,
        SamRolesAndActions(Set(highestAccessLevel), Set()),
        SamRolesAndActions(additionalRoles, Set()),
        SamRolesAndActions(Set(), Set(SamWorkspaceActions.compute)),
        Set(),
        Set()
      )

      val sam = mock[SamDAO]
      when(sam.listUserResources(SamResourceTypeNames.workspace, defaultRequestContext))
        .thenReturn(Future(Seq(workspaceSamResource)))
      val wsRepo = mock[WorkspaceRepository]
      when(wsRepo.listWorkspacesByIds(ArgumentMatchers.eq(Seq(workspace.workspaceIdAsUUID)), any))
        .thenReturn(Future(Seq(workspace)))
      val wsm = mock[WorkspaceManagerDAO]
      val wsmWorkspaces = cloudPlatform match {
        case WorkspaceCloudPlatform.Gcp => List()
        case WorkspaceCloudPlatform.Azure =>
          List(
            new WorkspaceDescription()
              .azureContext(
                new AzureContext()
                  .tenantId(UUID.randomUUID().toString)
                  .subscriptionId(UUID.randomUUID().toString)
                  .resourceGroupId(UUID.randomUUID().toString)
              )
              .id(workspace.workspaceIdAsUUID)
              .stage(WorkspaceStageModel.MC_WORKSPACE)
          )
      }
      when(wsm.listWorkspaces(defaultRequestContext)).thenReturn(wsmWorkspaces)
      val service = workspaceServiceConstructor(
        samDAO = sam,
        workspaceManagerDAO = wsm,
        workspaceRepository = wsRepo
      )(defaultRequestContext)

      val params = WorkspaceFieldSpecs(fields = Some(Set("workspace", "public", "accessLevel", "canCompute")))
      val result = Await.result(service.listWorkspaces(params, -1), Duration.Inf)

      val resultWorkspace = result match {
        case jsa: JsArray =>
          jsa.elements.headOption.map {
            _.convertTo[WorkspaceListResponse]
          }
        case _ => None
      }
      resultWorkspace.get.canCompute.get shouldBe canComputeResult
    }
  }

  it should "map the results of the resource auth domains" in {
    val authGroupName = "expected-auth-group"
    val workspaceSamResource = SamUserResource(
      workspace.workspaceId,
      SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set()),
      SamRolesAndActions(Set(), Set()),
      SamRolesAndActions(Set(), Set()),
      Set(WorkbenchGroupName(authGroupName)),
      Set()
    )

    val sam = mock[SamDAO]
    when(sam.listUserResources(SamResourceTypeNames.workspace, defaultRequestContext))
      .thenReturn(Future(Seq(workspaceSamResource)))
    val wsRepo = mock[WorkspaceRepository]
    when(wsRepo.listWorkspacesByIds(ArgumentMatchers.eq(Seq(workspace.workspaceIdAsUUID)), any))
      .thenReturn(Future(Seq(workspace)))
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.listWorkspaces(defaultRequestContext)).thenReturn(List())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceManagerDAO = wsm,
      workspaceRepository = wsRepo
    )(defaultRequestContext)

    val params = WorkspaceFieldSpecs(fields = Some(Set("workspace", "accessLevel", "public")))
    val result = Await.result(service.listWorkspaces(params, -1), Duration.Inf)

    val resultWorkspace = result match {
      case jsa: JsArray =>
        jsa.elements.headOption.map {
          _.convertTo[WorkspaceListResponse]
        }
      case _ => None
    }
    resultWorkspace.get.workspace.authorizationDomain shouldBe Some(Set(ManagedGroupRef(RawlsGroupName(authGroupName))))
  }

  it should "match the submission stats with the correct workspace" in {
    val workspace1 = workspace.copy(name = "name1", namespace = "namespace1")
    val workspace2 = workspace.copy(workspaceId = UUID.randomUUID().toString, name = "name2", namespace = "namespace2")
    val workspaceIds = Seq(workspace2.workspaceIdAsUUID, workspace1.workspaceIdAsUUID)
    val workspace1SamResource = SamUserResource(
      workspace1.workspaceId,
      SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set()),
      SamRolesAndActions(Set(), Set()),
      SamRolesAndActions(Set(), Set()),
      Set(),
      Set()
    )
    val workspace2SamResource = workspace1SamResource.copy(
      resourceId = workspace2.workspaceId,
      direct = SamRolesAndActions(Set(SamWorkspaceRoles.writer), Set(SamWorkspaceActions.compute))
    )
    val sam = mock[SamDAO]
    when(sam.listUserResources(SamResourceTypeNames.workspace, defaultRequestContext))
      .thenReturn(Future(Seq(workspace2SamResource, workspace1SamResource)))
    val wsRepo = mock[WorkspaceRepository]
    when(wsRepo.listWorkspacesByIds(ArgumentMatchers.eq(workspaceIds), any))
      .thenReturn(Future(Seq(workspace1, workspace2)))
    val workspace1Stats = WorkspaceSubmissionStats(None, None, 1)
    val workspace2Stats = WorkspaceSubmissionStats(None, None, 3)
    when(wsRepo.listSubmissionSummaryStats(workspaceIds)).thenReturn(
      Future(
        Map(
          workspace1.workspaceIdAsUUID -> workspace1Stats,
          workspace2.workspaceIdAsUUID -> workspace2Stats
        )
      )
    )
    val wsm = mock[WorkspaceManagerDAO]
    when(wsm.listWorkspaces(defaultRequestContext)).thenReturn(List())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceManagerDAO = wsm,
      workspaceRepository = wsRepo
    )(defaultRequestContext)

    val params =
      WorkspaceFieldSpecs(fields = Some(Set("workspace", "accessLevel", "public", "workspaceSubmissionStats")))
    val result = Await.result(service.listWorkspaces(params, -1), Duration.Inf)

    val resultWorkspaces: Map[String, WorkspaceListResponse] = result match {
      case jsa: JsArray =>
        jsa.elements.map { js =>
          val ws = js.convertTo[WorkspaceListResponse]
          ws.workspace.workspaceId -> ws
        }.toMap
      case _ => Map()
    }
    val details1 =
      WorkspaceDetails.fromWorkspaceAndOptions(workspace1, Some(Set()), false, Some(WorkspaceCloudPlatform.Gcp))
    val expectedResult1 =
      WorkspaceListResponse(WorkspaceAccessLevels.Owner, None, None, details1, Some(workspace1Stats), false, None)
    val details2 =
      WorkspaceDetails.fromWorkspaceAndOptions(workspace2, Some(Set()), false, Some(WorkspaceCloudPlatform.Gcp))
    val expectedResult2 =
      WorkspaceListResponse(WorkspaceAccessLevels.Write, None, None, details2, Some(workspace2Stats), false, None)

    resultWorkspaces(workspace1.workspaceId) shouldBe expectedResult1
    resultWorkspaces(workspace2.workspaceId) shouldBe expectedResult2
  }

  it should "match an azure workspace with the WSM results" in {
    val workspace1 = workspace.copy(name = "name1", namespace = "namespace1", workspaceType = WorkspaceType.McWorkspace)
    val workspace2 = workspace.copy(workspaceId = UUID.randomUUID().toString, name = "name2", namespace = "namespace2")
    val workspaceIds = Seq(workspace2.workspaceIdAsUUID, workspace1.workspaceIdAsUUID)
    val workspace1SamResource = SamUserResource(
      workspace1.workspaceId,
      SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set()),
      SamRolesAndActions(Set(), Set()),
      SamRolesAndActions(Set(), Set()),
      Set(),
      Set()
    )
    val workspace2SamResource = workspace1SamResource.copy(
      resourceId = workspace2.workspaceId,
      direct = SamRolesAndActions(Set(SamWorkspaceRoles.writer), Set(SamWorkspaceActions.compute))
    )
    val sam = mock[SamDAO]
    when(sam.listUserResources(SamResourceTypeNames.workspace, defaultRequestContext))
      .thenReturn(Future(Seq(workspace2SamResource, workspace1SamResource)))
    val wsRepo = mock[WorkspaceRepository]
    when(wsRepo.listWorkspacesByIds(ArgumentMatchers.eq(workspaceIds), any))
      .thenReturn(Future(Seq(workspace1, workspace2)))
    val workspace1Stats = WorkspaceSubmissionStats(None, None, 1)
    val workspace2Stats = WorkspaceSubmissionStats(None, None, 3)
    when(wsRepo.listSubmissionSummaryStats(workspaceIds)).thenReturn(
      Future(
        Map(
          workspace1.workspaceIdAsUUID -> workspace1Stats,
          workspace2.workspaceIdAsUUID -> workspace2Stats
        )
      )
    )
    val wsm = mock[WorkspaceManagerDAO]
    val workspace1WSMDescription = new WorkspaceDescription()
      .azureContext(
        new AzureContext()
          .tenantId(UUID.randomUUID().toString)
          .subscriptionId(UUID.randomUUID().toString)
          .resourceGroupId(UUID.randomUUID().toString)
      )
      .id(workspace1.workspaceIdAsUUID)
      .stage(WorkspaceStageModel.MC_WORKSPACE)

    when(wsm.listWorkspaces(defaultRequestContext)).thenReturn(List(workspace1WSMDescription))
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceManagerDAO = wsm,
      workspaceRepository = wsRepo
    )(defaultRequestContext)

    val params =
      WorkspaceFieldSpecs(fields = Some(Set("workspace", "accessLevel", "public", "workspaceSubmissionStats")))
    val result = Await.result(service.listWorkspaces(params, -1), Duration.Inf)

    val resultWorkspaces: Map[String, WorkspaceListResponse] = result match {
      case jsa: JsArray =>
        jsa.elements.map { js =>
          val ws = js.convertTo[WorkspaceListResponse]
          ws.workspace.workspaceId -> ws
        }.toMap
      case _ => Map()
    }
    val details1 =
      WorkspaceDetails.fromWorkspaceAndOptions(workspace1, Some(Set()), false, Some(WorkspaceCloudPlatform.Azure))
    val expectedResult1 =
      WorkspaceListResponse(WorkspaceAccessLevels.Owner, None, None, details1, Some(workspace1Stats), false, None)
    val details2 =
      WorkspaceDetails.fromWorkspaceAndOptions(workspace2, Some(Set()), false, Some(WorkspaceCloudPlatform.Gcp))
    val expectedResult2 =
      WorkspaceListResponse(WorkspaceAccessLevels.Write, None, None, details2, Some(workspace2Stats), false, None)

    resultWorkspaces(workspace1.workspaceId) shouldBe expectedResult1
    resultWorkspaces(workspace2.workspaceId) shouldBe expectedResult2
  }

  behavior of "assertNoGoogleChildrenBlockingWorkspaceDeletion"

  it should "not error if the only child is the google project" in {
    val samDAO = mock[SamDAO]
    when(samDAO.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(
        Future(
          Seq(
            SamFullyQualifiedResourceId(workspace.googleProjectId.value, SamResourceTypeNames.googleProject.value)
          )
        )
      )
    when(
      samDAO.listResourceChildren(
        SamResourceTypeNames.googleProject,
        workspace.googleProjectId.value,
        defaultRequestContext
      )
    )
      .thenReturn(Future(Seq()))
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)

    Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(workspace), Duration.Inf) shouldBe ()
  }

  it should "error if the workspace google project has a child resource" in {
    val samDAO = mock[SamDAO]
    when(samDAO.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(Future(Seq()))
    when(
      samDAO.listResourceChildren(
        SamResourceTypeNames.googleProject,
        workspace.googleProjectId.value,
        defaultRequestContext
      )
    )
      .thenReturn(Future(Seq(SamFullyQualifiedResourceId("some-child", SamResourceTypeNames.googleProject.value))))
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(workspace), Duration.Inf)
    }

    error.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
    error.errorReport.message shouldBe "Workspace deletion blocked by child resources"
    error.errorReport.causes.size shouldBe 1
  }

  it should "error if the workspace has a child resource besides it's google project" in {
    val samDAO = mock[SamDAO]
    when(samDAO.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(
        Future(
          Seq(
            SamFullyQualifiedResourceId(workspace.googleProjectId.value, SamResourceTypeNames.googleProject.value)
          )
        )
      )
    when(
      samDAO.listResourceChildren(
        SamResourceTypeNames.googleProject,
        workspace.googleProjectId.value,
        defaultRequestContext
      )
    )
      .thenReturn(Future(Seq(SamFullyQualifiedResourceId("some-child", SamResourceTypeNames.googleProject.value))))
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(workspace), Duration.Inf)
    }

    error.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
    error.errorReport.message shouldBe "Workspace deletion blocked by child resources"
    error.errorReport.causes.size shouldBe 1
  }

  it should "return an error for each blocking child resource in the error report" in {
    val samDAO = mock[SamDAO]
    when(samDAO.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, defaultRequestContext))
      .thenReturn(
        Future(
          Seq(
            SamFullyQualifiedResourceId(workspace.googleProjectId.value, SamResourceTypeNames.googleProject.value),
            SamFullyQualifiedResourceId("another-resource", SamResourceTypeNames.googleProject.value)
          )
        )
      )
    when(
      samDAO.listResourceChildren(
        SamResourceTypeNames.googleProject,
        workspace.googleProjectId.value,
        defaultRequestContext
      )
    )
      .thenReturn(Future(Seq(SamFullyQualifiedResourceId("some-child", SamResourceTypeNames.googleProject.value))))
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(workspace), Duration.Inf)
    }

    error.errorReport.statusCode.get shouldBe StatusCodes.BadRequest
    error.errorReport.message shouldBe "Workspace deletion blocked by child resources"
    error.errorReport.causes.size shouldBe 2
  }

  it should "error if there is no googleProjectId" in {
    val samDAO = mock[SamDAO]
    val workspaceService = workspaceServiceConstructor(samDAO = samDAO)(defaultRequestContext)
    val wsId = UUID.randomUUID().toString
    val azureWorkspace = Workspace.buildReadyMcWorkspace(
      namespace = "test-azure-bp",
      name = s"test-azure-ws-$wsId",
      workspaceId = wsId,
      createdDate = DateTime.now,
      lastModified = DateTime.now,
      createdBy = "testuser@example.com",
      attributes = Map()
    )

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.assertNoGoogleChildrenBlockingWorkspaceDeletion(azureWorkspace), Duration.Inf)
    }

    error.errorReport.statusCode.get shouldBe StatusCodes.InternalServerError
    assert(error.errorReport.message contains "with no googleProjectId")
  }

  behavior of "getAcl"

  def mockWsmForAclTests(ownerEmail: String = "owner@example.com",
                         writerEmail: String = "writer@example.com",
                         readerEmail: String = "reader@example.com"
  ): WorkspaceManagerDAO = {
    val projectOwnerBinding =
      new RoleBinding().role(IamRole.PROJECT_OWNER).members(List("projectOwner@example.com").asJava)
    val ownerBinding = new RoleBinding().role(IamRole.OWNER).members(List(ownerEmail).asJava)
    val writerBinding = new RoleBinding().role(IamRole.WRITER).members(List(writerEmail).asJava)
    val readerBinding = new RoleBinding().role(IamRole.READER).members(List(readerEmail).asJava)
    val discovererBinding =
      new RoleBinding().role(IamRole.DISCOVERER).members(List("discoverer@example.com", readerEmail).asJava)
    val applicationBinding = new RoleBinding().role(IamRole.APPLICATION).members(List("application@example.com").asJava)
    val wsmRoleBindings = new RoleBindingList()
    wsmRoleBindings.addAll(
      List(projectOwnerBinding,
           ownerBinding,
           writerBinding,
           readerBinding,
           discovererBinding,
           applicationBinding
      ).asJava
    )
    val wsmDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(wsmDAO.getRoles(any(), any())).thenReturn(wsmRoleBindings)
    wsmDAO
  }

  def mockSamForAclTests(): SamDAO = {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.getUserIdInfo(any(), any())).thenReturn(
      Future.successful(SamDAO.User(UserIdInfo("fake_user_id", "user@example.com", Option("fake_google_subject_id"))))
    )
    when(samDAO.getUserStatus(any()))
      .thenReturn(Future.successful(Option(SamUserStatusResponse("fake_user_id", "user@example.com", true))))
    samDAO
  }

  def mockWorkspaceRepositoryForAclTests(workspaceType: WorkspaceType,
                                         workspaceId: UUID = UUID.randomUUID()
  ): WorkspaceRepository = {
    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    val googleProjectId = workspaceType match {
      case WorkspaceType.McWorkspace    => GoogleProjectId("")
      case WorkspaceType.RawlsWorkspace => GoogleProjectId("fake-project-id")
    }

    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(
      Future.successful(
        Option(
          Workspace("fake_namespace",
                    "fake_name",
                    workspaceId.toString,
                    "fake_bucket",
                    None,
                    DateTime.now(),
                    DateTime.now(),
                    "creator@example.com",
                    Map.empty
          ).copy(workspaceType = workspaceType, googleProjectId = googleProjectId)
        )
      )
    )
    workspaceRepository
  }

  def samWorkspacePoliciesForAclTests(projectOwnerEmail: String,
                                      ownerEmail: String,
                                      writerEmail: String,
                                      readerEmail: String
  ): Set[SamPolicyWithNameAndEmail] = Set(
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.owner,
                              SamPolicy(Set(WorkbenchEmail(ownerEmail)), Set.empty, Set.empty),
                              WorkbenchEmail("ownerPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.writer,
                              SamPolicy(Set(WorkbenchEmail(writerEmail)), Set.empty, Set.empty),
                              WorkbenchEmail("writerPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.reader,
                              SamPolicy(Set(WorkbenchEmail(readerEmail)), Set.empty, Set.empty),
                              WorkbenchEmail("readerPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareWriter,
                              SamPolicy(Set.empty, Set.empty, Set.empty),
                              WorkbenchEmail("shareWriterPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.canCompute,
                              SamPolicy(Set.empty, Set.empty, Set.empty),
                              WorkbenchEmail("canComputePolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareReader,
                              SamPolicy(Set.empty, Set.empty, Set.empty),
                              WorkbenchEmail("shareReaderPolicy@example.com")
    ),
    SamPolicyWithNameAndEmail(
      SamWorkspacePolicyNames.projectOwner,
      SamPolicy(Set(WorkbenchEmail(projectOwnerEmail)), Set.empty, Set.empty),
      WorkbenchEmail("projectOwnerPolicy@example.com")
    )
  )

  it should "fetch policies from Sam for Rawls workspaces" in {
    val projectOwnerEmail = "projectOwner@example.com"
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(projectOwnerEmail, ownerEmail, writerEmail, readerEmail))
    )

    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.RawlsWorkspace)

    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository, samDAO = samDAO)(defaultRequestContext)
    val result = Await.result(service.getACL(WorkspaceName("fake_namespace", "fake_name")), Duration.Inf)

    val expected = WorkspaceACL(
      Map(
        ownerEmail -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
        writerEmail -> AccessEntry(WorkspaceAccessLevels.Write, false, false, false),
        readerEmail -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false)
      )
    )

    result shouldBe expected
    verify(samDAO).listPoliciesForResource(any(), any(), any())
  }

  it should "fetch policies from WSM for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)

    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace)

    val samDAO = mockSamForAclTests()
    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                  samDAO = samDAO,
                                  workspaceManagerDAO = wsmDAO
      )(defaultRequestContext)

    val expected = WorkspaceACL(
      Map(
        ownerEmail -> AccessEntry(WorkspaceAccessLevels.Owner, false, true, true),
        writerEmail -> AccessEntry(WorkspaceAccessLevels.Write, false, false, false),
        readerEmail -> AccessEntry(WorkspaceAccessLevels.Read, false, false, false)
      )
    )

    val result = Await.result(service.getACL(WorkspaceName("fake_namespace", "fake_name")), Duration.Inf)

    result shouldBe expected
    verify(samDAO, never).listPoliciesForResource(any(), any(), any())
    verify(wsmDAO).getRoles(any(), any())
  }

  behavior of "updateAcl"
  it should "call Sam for Rawls workspaces" in {
    val projectOwnerEmail = "projectOwner@example.com"
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"

    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(projectOwnerEmail, ownerEmail, writerEmail, readerEmail))
    )
    when(samDAO.addUserToPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())
    when(samDAO.removeUserFromPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())

    val workspaceId = UUID.randomUUID()
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.RawlsWorkspace, workspaceId)

    val requesterPaysSetupService = mock[RequesterPaysSetupServiceImpl](RETURNS_SMART_NULLS)
    when(requesterPaysSetupService.revokeUserFromWorkspace(any(), any())).thenReturn(Future.successful(Seq.empty))

    val mockFastPassService = mock[FastPassServiceImpl]
    when(mockFastPassService.syncFastPassesForUserInWorkspace(any[Workspace], any[String]))
      .thenReturn(Future.successful())

    val service =
      workspaceServiceConstructor(
        workspaceRepository = workspaceRepository,
        samDAO = samDAO,
        requesterPaysSetupService = requesterPaysSetupService,
        fastPassServiceConstructor = _ => mockFastPassService
      )(
        defaultRequestContext
      )

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.NoAccess, Option(false), Option(false)),
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Write, Option(false), Option(false))
    )

    Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)

    verify(samDAO).addUserToPolicy(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                   any(),
                                   ArgumentMatchers.eq(SamWorkspacePolicyNames.writer),
                                   ArgumentMatchers.eq(readerEmail),
                                   any()
    )
    verify(samDAO).removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                        any(),
                                        ArgumentMatchers.eq(SamWorkspacePolicyNames.reader),
                                        ArgumentMatchers.eq(readerEmail),
                                        any()
    )
    verify(samDAO).removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.workspace),
                                        any(),
                                        ArgumentMatchers.eq(SamWorkspacePolicyNames.writer),
                                        ArgumentMatchers.eq(writerEmail),
                                        any()
    )
  }

  it should "call WSM for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val workspaceId = UUID.randomUUID()

    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclManagerDatasource = mock[SlickDataSource]
    when(aclManagerDatasource.inTransaction[Option[RawlsBillingProject]](any(), any())).thenReturn(
      Future.successful(
        Option(
          RawlsBillingProject(
            RawlsBillingProjectName("fake_namespace"),
            CreationStatuses.Ready,
            None,
            None,
            billingProfileId = Option(UUID.randomUUID().toString)
          )
        )
      )
    )

    val mockFastPassService = mock[FastPassServiceImpl]
    when(mockFastPassService.syncFastPassesForUserInWorkspace(any[Workspace], any[String]))
      .thenReturn(
        Future.successful(
        )
      )
    val service =
      workspaceServiceConstructor(
        workspaceRepository = workspaceRepository,
        samDAO = samDAO,
        workspaceManagerDAO = wsmDAO,
        aclManagerDatasource = aclManagerDatasource,
        fastPassServiceConstructor = _ => mockFastPassService
      )(defaultRequestContext)

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.NoAccess, Option(false), Option(false)),
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Write, Option(false), Option(false))
    )

    Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)

    verify(samDAO, never).addUserToPolicy(any(), any(), any(), any(), any())
    verify(samDAO, never).removeUserFromPolicy(any(), any(), any(), any(), any())
    verify(wsmDAO).removeRole(ArgumentMatchers.eq(workspaceId),
                              ArgumentMatchers.eq(WorkbenchEmail(writerEmail)),
                              ArgumentMatchers.eq(IamRole.WRITER),
                              any()
    )
    verify(wsmDAO).removeRole(ArgumentMatchers.eq(workspaceId),
                              ArgumentMatchers.eq(WorkbenchEmail(readerEmail)),
                              ArgumentMatchers.eq(IamRole.READER),
                              any()
    )
    verify(wsmDAO).grantRole(ArgumentMatchers.eq(workspaceId),
                             ArgumentMatchers.eq(WorkbenchEmail(readerEmail)),
                             ArgumentMatchers.eq(IamRole.WRITER),
                             any()
    )
  }

  it should "not allow share writers for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val workspaceId = UUID.randomUUID()

    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, Option(true), Option(false))
    )

    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                  samDAO = samDAO,
                                  workspaceManagerDAO = wsmDAO
      )(defaultRequestContext)
    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)
    }

    exception.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "not allow share readers for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val workspaceId = UUID.randomUUID()

    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclUpdates = Set(
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Read, Option(true), Option(false))
    )

    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                  samDAO = samDAO,
                                  workspaceManagerDAO = wsmDAO
      )(defaultRequestContext)
    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)
    }

    exception.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "not allow compute writers for McWorkspaces" in {
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"
    val workspaceId = UUID.randomUUID()

    val wsmDAO = mockWsmForAclTests(ownerEmail, writerEmail, readerEmail)
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.McWorkspace, workspaceId)
    val samDAO = mockSamForAclTests()

    val aclUpdates = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, Option(false), Option(true))
    )

    val service =
      workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                  samDAO = samDAO,
                                  workspaceManagerDAO = wsmDAO
      )(defaultRequestContext)
    val exception = intercept[InvalidWorkspaceAclUpdateException] {
      Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdates, true), Duration.Inf)
    }

    exception.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "not allow readers to have compute access but should allow writers for Rawls workspaces" in {
    val projectOwnerEmail = "projectOwner@example.com"
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"

    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(projectOwnerEmail, ownerEmail, writerEmail, readerEmail))
    )
    when(samDAO.addUserToPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())

    val workspaceId = UUID.randomUUID()
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.RawlsWorkspace, workspaceId)
    val mockFastPassService = mock[FastPassServiceImpl]
    when(mockFastPassService.syncFastPassesForUserInWorkspace(any[Workspace], any[String]))
      .thenReturn(Future.successful())

    val service = workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                              samDAO = samDAO,
                                              fastPassServiceConstructor = _ => mockFastPassService
    )(defaultRequestContext)

    val writerAclUpdate = Set(
      WorkspaceACLUpdate(writerEmail, WorkspaceAccessLevels.Write, Option(false), Option(true))
    )
    Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), writerAclUpdate, true), Duration.Inf)

    val readerAclUpdate = Set(
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Read, Option(false), Option(true))
    )

    val thrown = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), readerAclUpdate, true), Duration.Inf)
    }
    thrown.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
  }

  it should "allow readers with and without share access for Rawls workspaces" in {
    val projectOwnerEmail = "projectOwner@example.com"
    val ownerEmail = "owner@example.com"
    val writerEmail = "writer@example.com"
    val readerEmail = "reader@example.com"

    val samDAO = mockSamForAclTests()
    when(samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any(), any())).thenReturn(
      Future.successful(samWorkspacePoliciesForAclTests(projectOwnerEmail, ownerEmail, writerEmail, readerEmail))
    )
    when(samDAO.addUserToPolicy(any(), any(), any(), any(), any())).thenReturn(Future.successful())

    val workspaceId = UUID.randomUUID()
    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.RawlsWorkspace, workspaceId)
    val mockFastPassService = mock[FastPassServiceImpl]
    when(mockFastPassService.syncFastPassesForUserInWorkspace(any[Workspace], any[String]))
      .thenReturn(Future.successful())

    val service = workspaceServiceConstructor(workspaceRepository = workspaceRepository,
                                              samDAO = samDAO,
                                              fastPassServiceConstructor = _ => mockFastPassService
    )(defaultRequestContext)

    val aclUpdate = Set(
      WorkspaceACLUpdate(readerEmail, WorkspaceAccessLevels.Read, Option(true), Option(false)),
      WorkspaceACLUpdate("foo@bar.com", WorkspaceAccessLevels.Read, Option(false), Option(false))
    )

    Await.result(service.updateACL(WorkspaceName("fake_namespace", "fake_name"), aclUpdate, true), Duration.Inf)
  }

  behavior of "getBucketUsage"
  // TODO: neither this nor getBucketOptions seem to verify we have a gcp workspace,
  //  or that googleProjectId/bucketName is available - this should probably be fixed
  //  note: in Workspace.buildMcWorkspace (WorkspaceModel:285), it's handled like this
  //    val googleProjectId =
  //      if (workspaceType == WorkspaceType.RawlsWorkspace) GoogleProjectId("google-id") else GoogleProjectId("")
  //    practically, if it's a MC workspace, the GoogleProjectId will be invalid, and the bucket name will be an empty string
  //  this will cause an exception in google, but it won't blow up the world, so maybe it's fine
  //  but it seems nice to just check before the call and return a more helpful exception
  it should "get the bucket usage for a gcp workspace" in {
    val workspace = this.workspace.copy(googleProjectId = GoogleProjectId("project-id"), bucketName = "test-bucket")
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future.successful(Some(workspace)))
    val sam = mock[SamDAO]
    when(sam.getUserStatus(defaultRequestContext)).thenReturn(
      Future.successful(
        Some(
          SamUserStatusResponse(
            defaultRequestContext.userInfo.userSubjectId.value,
            defaultRequestContext.userInfo.userEmail.value,
            true
          )
        )
      )
    )
    when(
      sam.userHasAction(SamResourceTypeNames.workspace,
                        workspace.workspaceId,
                        SamWorkspaceActions.read,
                        defaultRequestContext
      )
    ).thenReturn(Future(true))
    val bucketUsage = mock[BucketUsageResponse]
    val gcs = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    when(gcs.getBucketUsage(workspace.googleProjectId, workspace.bucketName, None))
      .thenReturn(Future.successful(bucketUsage))
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceRepository = repository,
      gcsDAO = gcs
    )(defaultRequestContext)

    Await.result(service.getBucketUsage(workspace.toWorkspaceName), Duration.Inf) shouldBe bucketUsage
    verify(gcs).getBucketUsage(workspace.googleProjectId, workspace.bucketName, None)
  }

  it should "work on a locked workspace" in {
    val workspace = this.workspace.copy(isLocked = true, googleProjectId = GoogleProjectId("project-id"))
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future.successful(Some(workspace)))
    val sam = mock[SamDAO]
    when(sam.getUserStatus(defaultRequestContext)).thenReturn(
      Future.successful(
        Some(
          SamUserStatusResponse(
            defaultRequestContext.userInfo.userSubjectId.value,
            defaultRequestContext.userInfo.userEmail.value,
            true
          )
        )
      )
    )
    when(
      sam.userHasAction(SamResourceTypeNames.workspace,
                        workspace.workspaceId,
                        SamWorkspaceActions.read,
                        defaultRequestContext
      )
    ).thenReturn(Future(true))
    val bucketUsage = mock[BucketUsageResponse]
    val gcs = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    when(gcs.getBucketUsage(workspace.googleProjectId, workspace.bucketName, None))
      .thenReturn(Future.successful(bucketUsage))
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceRepository = repository,
      gcsDAO = gcs
    )(defaultRequestContext)

    Await.result(service.getBucketUsage(workspace.toWorkspaceName), Duration.Inf) shouldBe bucketUsage
    verify(gcs).getBucketUsage(workspace.googleProjectId, workspace.bucketName, None)
  }

  it should "map non-standard codes from a GoogleJsonResponseException to a rawls exception" in {
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future.successful(Some(workspace)))
    val sam = mock[SamDAO]
    when(sam.getUserStatus(defaultRequestContext)).thenReturn(
      Future.successful(
        Some(
          SamUserStatusResponse(
            defaultRequestContext.userInfo.userSubjectId.value,
            defaultRequestContext.userInfo.userEmail.value,
            true
          )
        )
      )
    )
    when(
      sam.userHasAction(SamResourceTypeNames.workspace,
                        workspace.workspaceId,
                        SamWorkspaceActions.read,
                        defaultRequestContext
      )
    ).thenReturn(Future(true))
    val bucketUsage = mock[BucketUsageResponse]
    val gcs = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    doAnswer { _ =>
      throw new GoogleJsonResponseException(
        new HttpResponseException.Builder(489, "a weird google error", new HttpHeaders()),
        new GoogleJsonError()
      )
    }.when(gcs).getBucketUsage(workspace.googleProjectId, workspace.bucketName, None)

    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceRepository = repository,
      gcsDAO = gcs
    )(defaultRequestContext)

    val error = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getBucketUsage(workspace.toWorkspaceName), Duration.Inf) shouldBe bucketUsage
    }

    error.errorReport.statusCode.get.intValue shouldBe 489
    error.errorReport.statusCode.get.reason() shouldBe "Google API failure"
    verify(gcs).getBucketUsage(workspace.googleProjectId, workspace.bucketName, None)
  }

  behavior of "getBucketOptions"
  it should "get the bucket options for a gcp workspace" in {
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future.successful(Some(workspace)))
    val sam = mock[SamDAO]
    when(sam.getUserStatus(defaultRequestContext)).thenReturn(
      Future.successful(
        Some(
          SamUserStatusResponse(
            defaultRequestContext.userInfo.userSubjectId.value,
            defaultRequestContext.userInfo.userEmail.value,
            true
          )
        )
      )
    )
    when(
      sam.userHasAction(SamResourceTypeNames.workspace,
                        workspace.workspaceId,
                        SamWorkspaceActions.read,
                        defaultRequestContext
      )
    ).thenReturn(Future(true))
    val bucketDetails = mock[WorkspaceBucketOptions]
    val gcs = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    when(gcs.getBucketDetails(workspace.bucketName, workspace.googleProjectId))
      .thenReturn(Future.successful(bucketDetails))
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceRepository = repository,
      gcsDAO = gcs
    )(defaultRequestContext)

    Await.result(service.getBucketOptions(workspace.toWorkspaceName), Duration.Inf) shouldBe bucketDetails
    verify(gcs).getBucketDetails(workspace.bucketName, workspace.googleProjectId)
  }
}
