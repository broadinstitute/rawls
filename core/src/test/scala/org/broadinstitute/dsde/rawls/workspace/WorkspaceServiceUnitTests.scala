package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.fastpass.FastPassService
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.GcpBucketRequesterPaysConfig
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingTypes.GcpBucketRequesterPays
import org.broadinstitute.dsde.rawls.model.WorkspaceType.WorkspaceType
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.policy.PolicyService
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.submissions.SubmissionsRepository
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport, UserDisabledException, WorkspaceAccessDeniedException}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
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

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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

  val ctx: RawlsRequestContext = RawlsRequestContext(
    UserInfo(RawlsUserEmail("user@example.com"),
             OAuth2BearerToken("Bearer 123"),
             123,
             RawlsUserSubjectId("fake_user_id")
    )
  )

  val enabledUser: SamUserStatusResponse = SamUserStatusResponse("fake_user_id", "user@example.com", true)

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
    leonardoService: LeonardoService = mock[LeonardoService](RETURNS_SMART_NULLS),
    gcsDAO: GoogleServicesDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS),
    samDAO: SamDAO = mock[SamDAO],
    notificationDAO: NotificationDAO = mock[NotificationDAO](RETURNS_SMART_NULLS),
    userServiceConstructor: RawlsRequestContext => UserService = _ => mock[UserService](RETURNS_SMART_NULLS),
    workbenchMetricBaseName: String = "",
    config: WorkspaceServiceConfig = mock[WorkspaceServiceConfig](RETURNS_SMART_NULLS),
    requesterPaysSetupService: RequesterPaysSetupService = mock[RequesterPaysSetupService](RETURNS_SMART_NULLS),
    resourceBufferService: ResourceBufferService = mock[ResourceBufferService](RETURNS_SMART_NULLS),
    servicePerimeterService: ServicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS),
    googleIamDao: GoogleIamDAO = mock[GoogleIamDAO](RETURNS_SMART_NULLS),
    terraBillingProjectOwnerRole: String = "",
    terraWorkspaceCanComputeRole: String = "",
    terraWorkspaceNextflowRole: String = "",
    terraBucketReaderRole: String = "",
    terraBucketWriterRole: String = "",
    aclManagerDatasource: SlickDataSource = mock[SlickDataSource](RETURNS_SMART_NULLS),
    fastPassServiceConstructor: RawlsRequestContext => FastPassService = _ =>
      mock[FastPassService](RETURNS_SMART_NULLS),
    workspaceRepository: WorkspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS),
    billingRepository: BillingRepository = mock[BillingRepository](RETURNS_SMART_NULLS),
    submissionsRepository: SubmissionsRepository = mock[SubmissionsRepository](RETURNS_SMART_NULLS),
    workspaceSettingRepository: WorkspaceSettingRepository = mock[WorkspaceSettingRepository](RETURNS_SMART_NULLS),
    policyService: PolicyService = mock[PolicyService](RETURNS_SMART_NULLS),
    workspaceSettingServiceConstructor: RawlsRequestContext => WorkspaceSettingService = _ =>
      mock[WorkspaceSettingService](RETURNS_SMART_NULLS),
    entityServiceConstructor: RawlsRequestContext => EntityService = _ => mock[EntityService](RETURNS_SMART_NULLS)
  ): RawlsRequestContext => WorkspaceService = info =>
    new WorkspaceService(
      info,
      mock[SlickDataSource](RETURNS_SMART_NULLS),
      executionServiceCluster,
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
      fastPassServiceConstructor,
      workspaceRepository,
      billingRepository,
      submissionsRepository,
      workspaceSettingRepository,
      policyService,
      workspaceSettingServiceConstructor,
      entityServiceConstructor
    )(scala.concurrent.ExecutionContext.global)

  behavior of "getWorkspaceById"

  it should "return the workspace on success" in {
    val sam = mock[SamDAO]
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
      .thenReturn(Future(true))
    when(sam.getResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq()))
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.workspaceIdAsUUID, Some(WorkspaceAttributeSpecs(false))))
      .thenReturn(Future(Some(workspace)))
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceRepository = repository
    )(ctx)

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
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
      .thenReturn(Future(false))
    val service = workspaceServiceConstructor(samDAO = sam, workspaceRepository = repository)(ctx)

    val exception = intercept[NoSuchWorkspaceException] {
      Await.result(service.getWorkspaceById(workspace.workspaceId, WorkspaceFieldSpecs()), Duration.Inf)
    }

    exception.workspace shouldBe workspace.workspaceId
    exception.getMessage should (not include workspace.name)
    exception.getMessage should (not include workspace.namespace)
    verify(sam).userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx)
  }

  it should "return an exception with the workspaceId when no workspace is found" in {
    val sam = mock[SamDAO]
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.workspaceIdAsUUID, Some(WorkspaceAttributeSpecs(true))))
      .thenReturn(Future(None))

    val exception = intercept[NoSuchWorkspaceException] {
      val service = workspaceServiceConstructor(samDAO = sam, workspaceRepository = repository)(ctx)
      Await.result(service.getWorkspaceById(workspace.workspaceId, WorkspaceFieldSpecs()), Duration.Inf)
    }

    exception.workspace shouldEqual workspace.workspaceId
  }

  behavior of "getWorkspace"

  it should "return the workspace on success" in {
    val sam = mock[SamDAO]
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
      .thenReturn(Future(true))
    when(sam.getResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq()))
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.toWorkspaceName, Some(WorkspaceAttributeSpecs(false))))
      .thenReturn(Future(Some(workspace)))
    val service = workspaceServiceConstructor(
      samDAO = sam,
      workspaceRepository = repository
    )(ctx)

    val result = Await.result(
      service.getWorkspace(workspace.toWorkspaceName, WorkspaceFieldSpecs(Some(Set("workspace")))),
      Duration.Inf
    )

    val fields = result.fields("workspace").asJsObject.getFields("name", "namespace")
    fields should contain(workspace.name)
    fields should contain(workspace.namespace)
  }

  it should "throw an exception when invalid fields are requested" in {
    val service = workspaceServiceConstructor()(ctx)
    val invalidField = "thisFieldIsInvalid"
    val fields = WorkspaceFieldSpecs(Some(Set(invalidField)))

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getWorkspace(workspace.toWorkspaceName, fields), Duration.Inf)
    }

    exception.errorReport.message should include(invalidField)
  }

  it should "return an unauthorized error if the user is disabled" in {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser.copy(enabled = false))))

    val exception = intercept[UserDisabledException] {
      val service = workspaceServiceConstructor(samDAO = samDAO)(ctx)
      Await.result(service.getWorkspace(WorkspaceName("fake_namespace", "fake_name"), WorkspaceFieldSpecs()),
                   Duration.Inf
      )
    }
    exception.errorReport.statusCode shouldBe Some(StatusCodes.Unauthorized)
  }

  behavior of "getWorkspaceDetails"

  it should "not preform operations for fields that are not requested" in {
    val options = WorkspaceService.QueryOptions(Set(), WorkspaceAttributeSpecs(false))
    val service = workspaceServiceConstructor()(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.canCompute shouldBe None
    result.catalog shouldBe None
    result.canShare shouldBe None
  }

  it should "check for the catalog permission in sam the field is requested" in {
    val options = WorkspaceService.QueryOptions(Set("catalog"), WorkspaceAttributeSpecs(false))
    val sam = mock[SamDAO]
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.catalog, ctx))
      .thenReturn(Future(true))
    val service = workspaceServiceConstructor(samDAO = sam)(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.catalog shouldBe Some(true)
    verify(sam).userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.catalog, ctx)
  }

  it should "return the highest access level in accessLevel" in {
    val options = WorkspaceService.QueryOptions(Set("accessLevel"), WorkspaceAttributeSpecs(false))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Set(SamResourceRole("READER"), SamResourceRole("OWNER"))))
    val service = workspaceServiceConstructor(samDAO = sam)(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.accessLevel shouldBe Some(WorkspaceAccessLevels.Owner)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
  }

  it should "return noaccess for accessLevel when sam return no roles for the user" in {
    // this isn't realistic, since the user should have at least read access to get here,
    // but it's the default specified
    val options = WorkspaceService.QueryOptions(Set("accessLevel"), WorkspaceAttributeSpecs(false))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Set()))
    val service = workspaceServiceConstructor(samDAO = sam)(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.accessLevel shouldBe Some(WorkspaceAccessLevels.NoAccess)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
  }

  it should "return true for canCompute if the user is an owner" in {
    val options = WorkspaceService.QueryOptions(Set("canCompute"), WorkspaceAttributeSpecs(false))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Set(SamResourceRole("OWNER"))))
    val service = workspaceServiceConstructor(samDAO = sam)(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.workspace.name shouldBe workspace.name
    result.workspace.namespace shouldBe workspace.namespace
    result.canCompute shouldBe Some(true)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
  }

  it should "return true for canCompute if the user is a writer on an azure workspace" in {
    val workspace = this.workspace.copy(workspaceType = WorkspaceType.McWorkspace)
    val options = WorkspaceService.QueryOptions(Set("canCompute"), WorkspaceAttributeSpecs(false))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Set(SamResourceRole("OWNER"))))
    val service = workspaceServiceConstructor(samDAO = sam)(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.workspace.name shouldBe workspace.name
    result.workspace.namespace shouldBe workspace.namespace
    result.canCompute shouldBe Some(true)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
  }

  it should "query sam for canCompute if the user is not an owner on a gcp workspace" in {
    val options = WorkspaceService.QueryOptions(Set("canCompute"), WorkspaceAttributeSpecs(false))
    val sam = mock[SamDAO]
    when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Set(SamResourceRole("WRITER"))))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.compute, ctx))
      .thenReturn(Future(true))
    val service = workspaceServiceConstructor(samDAO = sam)(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.workspace.name shouldBe workspace.name
    result.workspace.namespace shouldBe workspace.namespace
    result.canCompute shouldBe Some(true)
    verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
    verify(sam).userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.compute, ctx)
  }

  it should "return true for canShare if the user is a workspace or project owner" in
    forAll(Table("role", "OWNER", "PROJECT_OWNER")) { (role: String) =>
      val options = WorkspaceService.QueryOptions(Set("canShare"), WorkspaceAttributeSpecs(false))
      val sam = mock[SamDAO]
      when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
        .thenReturn(Future(Set(SamResourceRole(role))))
      val service = workspaceServiceConstructor(samDAO = sam)(ctx)

      val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

      result.workspace.name shouldBe workspace.name
      result.workspace.namespace shouldBe workspace.namespace
      result.canShare shouldBe Some(true)
      verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
    }

  it should "query sam for canShare if the user is not an owner" in
    forAll(
      Table(
        ("role", "samAnswer"),
        ("WRITER", true),
        ("READER", true),
        ("NO ACCESS", false)
      )
    ) { (role: String, samAnswer: Boolean) =>
      val options = WorkspaceService.QueryOptions(Set("canShare"), WorkspaceAttributeSpecs(false))
      val sam = mock[SamDAO]
      when(sam.listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
        .thenReturn(Future(Set(SamResourceRole(role))))
      when(
        sam.userHasAction(
          SamResourceTypeNames.workspace,
          workspace.workspaceId,
          SamWorkspaceActions.sharePolicy(role.toLowerCase),
          ctx
        )
      ).thenReturn(Future(samAnswer))
      val service = workspaceServiceConstructor(samDAO = sam)(ctx)

      val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

      result.workspace.name shouldBe workspace.name
      result.workspace.namespace shouldBe workspace.namespace
      result.canShare shouldBe Some(samAnswer)
      verify(sam).listUserRolesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
      verify(sam).userHasAction(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspaceActions.sharePolicy(role.toLowerCase),
        ctx
      )
    }

  it should "get the bucket options from gcs when requested" in {
    val options = WorkspaceService.QueryOptions(Set("bucketOptions"), WorkspaceAttributeSpecs(false))
    val gcs = mock[GoogleServicesDAO]
    val bucketDetails = WorkspaceBucketOptions(true, "")
    when(gcs.getBucketDetails(workspace.bucketName, workspace.googleProjectId)).thenReturn(Future(bucketDetails))
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future(Some(workspace)))
    val sam = mock[SamDAO]
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
      .thenReturn(Future(true))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.write, ctx))
      .thenReturn(Future(true))
    val settings = mock[WorkspaceSettingRepository]
    when(settings.getWorkspaceSettingOfType(workspace.workspaceIdAsUUID, GcpBucketRequesterPays))
      .thenReturn(Future(None))
    val service = workspaceServiceConstructor(
      workspaceRepository = repository,
      workspaceSettingRepository = settings,
      samDAO = sam,
      gcsDAO = gcs
    )(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.bucketOptions shouldBe Some(bucketDetails)
    verify(gcs).getBucketDetails(workspace.bucketName, workspace.googleProjectId)
  }

  it should "get the owner emails using the policy from sam when requested" in {
    val options = WorkspaceService.QueryOptions(Set("owners"), WorkspaceAttributeSpecs(false))
    val sam = mock[SamDAO]
    val ownerEmails = Set("user1@test.com", "user2@test.com")
    val owners = SamPolicy(ownerEmails.map(WorkbenchEmail), Set(), Set())
    when(sam.getPolicy(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspacePolicyNames.owner, ctx))
      .thenReturn(Future(owners))
    val service = workspaceServiceConstructor(samDAO = sam)(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.owners shouldBe Some(ownerEmails)
  }

  it should "get the auth domain from sam when requested" in {
    val options = WorkspaceService.QueryOptions(Set("workspace"), WorkspaceAttributeSpecs(false))
    val sam = mock[SamDAO]
    val authDomains = Seq("some-auth-domain")
    when(sam.getResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(authDomains))
    val service = workspaceServiceConstructor(samDAO = sam)(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    val expectedAuthDomains = authDomains.map(authDomainName => ManagedGroupRef(RawlsGroupName(authDomainName))).toSet
    result.workspace.authorizationDomain shouldEqual Some(expectedAuthDomains)
  }

  it should "get the submissionSummaryStats when requested" in {
    val options = WorkspaceService.QueryOptions(Set("workspaceSubmissionStats"), WorkspaceAttributeSpecs(false))
    val stats = WorkspaceSubmissionStats(None, None, 3)
    val workspaceRepository = mock[WorkspaceRepository]
    when(workspaceRepository.getSubmissionSummaryStats(workspace.workspaceIdAsUUID)).thenReturn(Future(Some(stats)))
    val service = workspaceServiceConstructor(workspaceRepository = workspaceRepository)(ctx)

    val result = Await.result(service.getWorkspaceDetails(workspace, options), Duration.Inf)

    result.workspaceSubmissionStats shouldBe Some(stats)
  }

  behavior of "deleteWorkspace"

  it should "fail if the user does not have the delete permission for the workspace" in {
    val sam = mock[SamDAO]
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(false))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
      .thenReturn(Future(true))
    val repo = mock[WorkspaceRepository]
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    val service = workspaceServiceConstructor(samDAO = sam, workspaceRepository = repo)(ctx)

    intercept[WorkspaceAccessDeniedException] {
      Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)
    }

    verify(sam).userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx)
  }

  it should "fail if called for a multi cloud workspace" in {
    val workspace = this.workspace.copy(workspaceType = WorkspaceType.McWorkspace)
    val sam = mock[SamDAO]
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(true))
    val repo = mock[WorkspaceRepository]
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    val service = workspaceServiceConstructor(samDAO = sam, workspaceRepository = repo)(ctx)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)
    }

    exception.errorReport.statusCode shouldBe Some(StatusCodes.BadRequest)
  }

  it should "delete the workspace in sam and the database" in {
    val sam = mock[SamDAO]
    val repo = mock[WorkspaceRepository]
    val requesterPaysService = mock[RequesterPaysSetupService]
    val submissionsRepository = mock[SubmissionsRepository]
    val leo = mock[LeonardoService]
    val fastPass = mock[FastPassService]
    val gcs = mock[GoogleServicesDAO]
    val tps = mock[PolicyService]
    // mocked operations are defined in the order they are called by the service
    // initial auth checks/workspace retrieval
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(true))
    when(sam.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq.empty))
    when(sam.getResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq()))
    when(sam.getResourceAuthDomain(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future(Seq()))
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    // delete requester pays records
    when(requesterPaysService.deleteAllRecordsForWorkspace(workspace)).thenReturn(Future(1))
    // abort workflows
    when(submissionsRepository.getActiveWorkflowsAndSetStatusToAborted(workspace)).thenReturn(Future(Seq()))
    // delete fast pass grants
    when(fastPass.removeFastPassGrantsForWorkspace(workspace)).thenReturn(Future())
    // notify leo to clean up resources
    when(leo.cleanupResources(workspace.googleProjectId, workspace.workspaceIdAsUUID, ctx)).thenReturn(Future())
    // delete google project
    when(sam.listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future(Set()))
    when(gcs.deleteGoogleProject(workspace.googleProjectId)).thenReturn(Future())
    when(sam.deleteResource(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future())
    // delete workspace and associated records
    when(repo.deleteRawlsWorkspace(workspace)).thenReturn(Future())
    // delete workflow collection in sam
    when(sam.deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx))
      .thenReturn(Future())
    // delete workspace pao in tps
    when(tps.deleteWorkspacePao(any(), any())).thenReturn(Future.unit)
    // delete workspace in sam
    when(sam.deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)).thenReturn(Future())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      requesterPaysSetupService = requesterPaysService,
      fastPassServiceConstructor = _ => fastPass,
      leonardoService = leo,
      workspaceRepository = repo,
      gcsDAO = gcs,
      submissionsRepository = submissionsRepository,
      policyService = tps
    )(ctx)

    val result = Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)

    result shouldBe WorkspaceDeletionResult.fromGcpBucketName(workspace.bucketName)
    verify(repo).deleteRawlsWorkspace(workspace)
    verify(sam).deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
  }

  it should "ignore 404 errors from sam when deleting the google project resource" in {
    val sam = mock[SamDAO]
    val repo = mock[WorkspaceRepository]
    val requesterPaysService = mock[RequesterPaysSetupService]
    val submissionsRepository = mock[SubmissionsRepository]
    val leo = mock[LeonardoService]
    val fastPass = mock[FastPassService]
    val gcs = mock[GoogleServicesDAO]
    val tps = mock[PolicyService]
    // mocked operations are defined in the order they are called by the service
    // initial auth checks/workspace retrieval
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(true))
    when(sam.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq.empty))
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    // delete requester pays records
    when(requesterPaysService.deleteAllRecordsForWorkspace(workspace)).thenReturn(Future(1))
    // abort workflows
    when(submissionsRepository.getActiveWorkflowsAndSetStatusToAborted(workspace)).thenReturn(Future(Seq()))
    // delete fast pass grants
    when(fastPass.removeFastPassGrantsForWorkspace(workspace)).thenReturn(Future())
    // notify leo to clean up resources
    when(leo.cleanupResources(workspace.googleProjectId, workspace.workspaceIdAsUUID, ctx)).thenReturn(Future())
    // delete google project
    when(sam.listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future(Set()))
    when(gcs.deleteGoogleProject(workspace.googleProjectId)).thenReturn(Future())
    // throw 404 when deleting the google project resource in sam
    when(sam.deleteResource(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future.failed(RawlsExceptionWithErrorReport(StatusCodes.NotFound, "")))
    // delete workspace and associated records
    when(repo.deleteRawlsWorkspace(workspace)).thenReturn(Future())
    // delete workflow collection in sam
    when(sam.deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx))
      .thenReturn(Future())
    // delete workspace pao in tps
    when(tps.deleteWorkspacePao(any(), any())).thenReturn(Future.unit)
    when(sam.deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)).thenReturn(Future())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      requesterPaysSetupService = requesterPaysService,
      fastPassServiceConstructor = _ => fastPass,
      leonardoService = leo,
      workspaceRepository = repo,
      gcsDAO = gcs,
      submissionsRepository = submissionsRepository,
      policyService = tps
    )(ctx)

    val result = Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)

    result shouldBe WorkspaceDeletionResult.fromGcpBucketName(workspace.bucketName)
    verify(sam).deleteResource(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx)
  }

  it should "ignore 404 errors from sam when deleting the workflow collection resource" in {
    val sam = mock[SamDAO]
    val repo = mock[WorkspaceRepository]
    val requesterPaysService = mock[RequesterPaysSetupService]
    val submissionsRepository = mock[SubmissionsRepository]
    val leo = mock[LeonardoService]
    val fastPass = mock[FastPassService]
    val gcs = mock[GoogleServicesDAO]
    val tps = mock[PolicyService]
    // mocked operations are defined in the order they are called by the service
    // initial auth checks/workspace retrieval
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(true))
    when(sam.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq.empty))
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    // delete requester pays records
    when(requesterPaysService.deleteAllRecordsForWorkspace(workspace)).thenReturn(Future(1))
    // abort workflows
    when(submissionsRepository.getActiveWorkflowsAndSetStatusToAborted(workspace)).thenReturn(Future(Seq()))
    // delete fast pass grants
    when(fastPass.removeFastPassGrantsForWorkspace(workspace)).thenReturn(Future())
    // notify leo to clean up resources
    when(leo.cleanupResources(workspace.googleProjectId, workspace.workspaceIdAsUUID, ctx)).thenReturn(Future())
    // delete google project
    when(sam.listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future(Set()))
    when(gcs.deleteGoogleProject(workspace.googleProjectId)).thenReturn(Future())
    when(sam.deleteResource(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future())
    // delete workspace and associated records
    when(repo.deleteRawlsWorkspace(workspace)).thenReturn(Future())
    // delete workspace pao in tps
    when(tps.deleteWorkspacePao(any(), any())).thenReturn(Future.unit)
    // throw 404 when deleting workflow collection in sam
    when(sam.deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx))
      .thenReturn(Future.failed(RawlsExceptionWithErrorReport(StatusCodes.NotFound, "")))
    when(sam.deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)).thenReturn(Future())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      requesterPaysSetupService = requesterPaysService,
      fastPassServiceConstructor = _ => fastPass,
      leonardoService = leo,
      workspaceRepository = repo,
      gcsDAO = gcs,
      submissionsRepository = submissionsRepository,
      policyService = tps
    )(ctx)

    val result = Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)

    result shouldBe WorkspaceDeletionResult.fromGcpBucketName(workspace.bucketName)
    verify(sam).deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx)
  }

  it should "throw non 404 errors from sam when deleting the workflow collection resource" in {
    val sam = mock[SamDAO]
    val repo = mock[WorkspaceRepository]
    val requesterPaysService = mock[RequesterPaysSetupService]
    val submissionsRepository = mock[SubmissionsRepository]
    val leo = mock[LeonardoService]
    val fastPass = mock[FastPassService]
    val gcs = mock[GoogleServicesDAO]
    // mocked operations are defined in the order they are called by the service
    // initial auth checks/workspace retrieval
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(true))
    when(sam.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq.empty))
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    // delete requester pays records
    when(requesterPaysService.deleteAllRecordsForWorkspace(workspace)).thenReturn(Future(1))
    // abort workflows
    when(submissionsRepository.getActiveWorkflowsAndSetStatusToAborted(workspace)).thenReturn(Future(Seq()))
    // delete fast pass grants
    when(fastPass.removeFastPassGrantsForWorkspace(workspace)).thenReturn(Future())
    // notify leo to clean up resources
    when(leo.cleanupResources(workspace.googleProjectId, workspace.workspaceIdAsUUID, ctx)).thenReturn(Future())
    // delete google project
    when(sam.listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future(Set()))
    when(gcs.deleteGoogleProject(workspace.googleProjectId)).thenReturn(Future())
    when(sam.deleteResource(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future())
    // delete workspace and associated records
    when(repo.deleteRawlsWorkspace(workspace)).thenReturn(Future())
    // throw exception when deleting workflow collection in sam
    val workflowDeletionException = RawlsExceptionWithErrorReport(StatusCodes.BadGateway, "")
    when(sam.deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx))
      .thenReturn(Future.failed(workflowDeletionException))
    when(sam.deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)).thenReturn(Future())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      requesterPaysSetupService = requesterPaysService,
      fastPassServiceConstructor = _ => fastPass,
      leonardoService = leo,
      workspaceRepository = repo,
      gcsDAO = gcs,
      submissionsRepository = submissionsRepository
    )(ctx)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)
    }
    exception shouldBe workflowDeletionException
    verify(sam).deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx)
  }

  it should "ignore 404 errors when deleting the workspace resource in sam" in {
    val sam = mock[SamDAO]
    val repo = mock[WorkspaceRepository]
    val requesterPaysService = mock[RequesterPaysSetupService]
    val submissionsRepository = mock[SubmissionsRepository]
    val leo = mock[LeonardoService]
    val fastPass = mock[FastPassService]
    val gcs = mock[GoogleServicesDAO]
    val tps = mock[PolicyService]
    // mocked operations are defined in the order they are called by the service
    // initial auth checks/workspace retrieval
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(true))
    when(sam.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq.empty))
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    // delete requester pays records
    when(requesterPaysService.deleteAllRecordsForWorkspace(workspace)).thenReturn(Future(1))
    // abort workflows
    when(submissionsRepository.getActiveWorkflowsAndSetStatusToAborted(workspace)).thenReturn(Future(Seq()))
    // delete fast pass grants
    when(fastPass.removeFastPassGrantsForWorkspace(workspace)).thenReturn(Future())
    // notify leo to clean up resources
    when(leo.cleanupResources(workspace.googleProjectId, workspace.workspaceIdAsUUID, ctx)).thenReturn(Future())
    // delete google project
    when(sam.listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future(Set()))
    when(gcs.deleteGoogleProject(workspace.googleProjectId)).thenReturn(Future())
    when(sam.deleteResource(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future())
    // delete workspace and associated records
    when(repo.deleteRawlsWorkspace(workspace)).thenReturn(Future())
    // delete workspace pao in tps
    when(tps.deleteWorkspacePao(any(), any())).thenReturn(Future.unit)
    // delete workflow collection in sam
    when(sam.deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx))
      .thenReturn(Future())
    // throw 404 when deleting workspace in sam
    when(sam.deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future.failed(RawlsExceptionWithErrorReport(StatusCodes.NotFound, "")))

    val service = workspaceServiceConstructor(
      samDAO = sam,
      requesterPaysSetupService = requesterPaysService,
      fastPassServiceConstructor = _ => fastPass,
      leonardoService = leo,
      workspaceRepository = repo,
      gcsDAO = gcs,
      submissionsRepository = submissionsRepository,
      policyService = tps
    )(ctx)

    val result = Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)

    result shouldBe WorkspaceDeletionResult.fromGcpBucketName(workspace.bucketName)
    verify(sam).deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)
  }

  it should "rethrow errors for resource children when deleting the workspace resource in sam" in {
    val sam = mock[SamDAO]
    val repo = mock[WorkspaceRepository]
    val requesterPaysService = mock[RequesterPaysSetupService]
    val submissionsRepository = mock[SubmissionsRepository]
    val leo = mock[LeonardoService]
    val fastPass = mock[FastPassService]
    val gcs = mock[GoogleServicesDAO]
    val tps = mock[PolicyService]
    // mocked operations are defined in the order they are called by the service
    // initial auth checks/workspace retrieval
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(true))
    when(sam.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq.empty))
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    // delete requester pays records
    when(requesterPaysService.deleteAllRecordsForWorkspace(workspace)).thenReturn(Future(1))
    // abort workflows
    when(submissionsRepository.getActiveWorkflowsAndSetStatusToAborted(workspace)).thenReturn(Future(Seq()))
    // delete fast pass grants
    when(fastPass.removeFastPassGrantsForWorkspace(workspace)).thenReturn(Future())
    // notify leo to clean up resources
    when(leo.cleanupResources(workspace.googleProjectId, workspace.workspaceIdAsUUID, ctx)).thenReturn(Future())
    // delete google project
    when(sam.listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future(Set()))
    when(gcs.deleteGoogleProject(workspace.googleProjectId)).thenReturn(Future())
    when(sam.deleteResource(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future())
    // delete workspace and associated records
    when(repo.deleteRawlsWorkspace(workspace)).thenReturn(Future())
    // delete workspace pao in tps
    when(tps.deleteWorkspacePao(any(), any())).thenReturn(Future.unit)
    // delete workflow collection in sam
    when(sam.deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx))
      .thenReturn(Future())
    // throw exception for child resources when deleting workspace in sam
    val samError = RawlsExceptionWithErrorReport(StatusCodes.BadRequest, "Cannot delete a resource with children")
    when(sam.deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future.failed(samError))

    val service = workspaceServiceConstructor(
      samDAO = sam,
      requesterPaysSetupService = requesterPaysService,
      fastPassServiceConstructor = _ => fastPass,
      leonardoService = leo,
      workspaceRepository = repo,
      gcsDAO = gcs,
      submissionsRepository = submissionsRepository,
      policyService = tps
    )(ctx)

    val exception = intercept[RawlsExceptionWithErrorReport] {
      val result = Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)
      result shouldBe WorkspaceDeletionResult.fromGcpBucketName(workspace.bucketName)
    }

    exception shouldBe samError
  }

  it should "delete pets when deleting the google project" in {
    val sam = mock[SamDAO]
    val repo = mock[WorkspaceRepository]
    val requesterPaysService = mock[RequesterPaysSetupService]
    val submissionsRepository = mock[SubmissionsRepository]
    val leo = mock[LeonardoService]
    val fastPass = mock[FastPassService]
    val gcs = mock[GoogleServicesDAO]
    val tps = mock[PolicyService]
    // mocked operations are defined in the order they are called by the service
    // initial auth checks/workspace retrieval
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(true))
    when(sam.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq.empty))
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    // delete requester pays records
    when(requesterPaysService.deleteAllRecordsForWorkspace(workspace)).thenReturn(Future(1))
    // abort workflows
    when(submissionsRepository.getActiveWorkflowsAndSetStatusToAborted(workspace)).thenReturn(Future(Seq()))
    // delete fast pass grants
    when(fastPass.removeFastPassGrantsForWorkspace(workspace)).thenReturn(Future())
    // notify leo to clean up resources
    when(leo.cleanupResources(workspace.googleProjectId, workspace.workspaceIdAsUUID, ctx)).thenReturn(Future())
    // delete pets in project
    val pet = UserIdInfo(UUID.randomUUID().toString, "pet-email", None)
    when(sam.listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future(Set(pet)))
    val petKeyJson = "fake-json"
    when(sam.getPetServiceAccountKeyForUser(workspace.googleProjectId, RawlsUserEmail(pet.userEmail)))
      .thenReturn(Future(petKeyJson))
    val petUserInfo = mock[UserInfo]
    when(gcs.getUserInfoUsingJson(petKeyJson)).thenReturn(Future(petUserInfo))
    when(sam.deleteUserPetServiceAccount(workspace.googleProjectId, ctx.copy(userInfo = petUserInfo)))
      .thenReturn(Future())
    // delete google project
    when(gcs.deleteGoogleProject(workspace.googleProjectId)).thenReturn(Future())
    when(sam.deleteResource(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future())
    // delete workspace and associated records
    when(repo.deleteRawlsWorkspace(workspace)).thenReturn(Future())
    // delete workspace pao in tps
    when(tps.deleteWorkspacePao(any(), any())).thenReturn(Future.unit)
    // delete workflow collection in sam
    when(sam.deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx))
      .thenReturn(Future())
    // delete workspace in sam
    when(sam.deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)).thenReturn(Future())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      requesterPaysSetupService = requesterPaysService,
      fastPassServiceConstructor = _ => fastPass,
      leonardoService = leo,
      workspaceRepository = repo,
      gcsDAO = gcs,
      submissionsRepository = submissionsRepository,
      policyService = tps
    )(ctx)

    val result = Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)

    result shouldBe WorkspaceDeletionResult.fromGcpBucketName(workspace.bucketName)
    verify(sam).listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx)
    verify(sam).getPetServiceAccountKeyForUser(workspace.googleProjectId, RawlsUserEmail(pet.userEmail))
    verify(gcs).getUserInfoUsingJson(petKeyJson)
    verify(sam).deleteUserPetServiceAccount(workspace.googleProjectId, ctx.copy(userInfo = petUserInfo))
  }

  it should "ignore 404s from Sam when retrieving pets" in {
    val sam = mock[SamDAO]
    val repo = mock[WorkspaceRepository]
    val requesterPaysService = mock[RequesterPaysSetupService]
    val submissionsRepository = mock[SubmissionsRepository]
    val leo = mock[LeonardoService]
    val fastPass = mock[FastPassService]
    val gcs = mock[GoogleServicesDAO]
    val tps = mock[PolicyService]
    // mocked operations are defined in the order they are called by the service
    // initial auth checks/workspace retrieval
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.delete, ctx))
      .thenReturn(Future(true))
    when(sam.listResourceChildren(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(Seq.empty))
    when(repo.getWorkspace(ArgumentMatchers.eq(workspace.toWorkspaceName), any[Option[WorkspaceAttributeSpecs]]))
      .thenReturn(Future(Some(workspace)))
    // delete requester pays records
    when(requesterPaysService.deleteAllRecordsForWorkspace(workspace)).thenReturn(Future(1))
    // abort workflows
    when(submissionsRepository.getActiveWorkflowsAndSetStatusToAborted(workspace)).thenReturn(Future(Seq()))
    // delete fast pass grants
    when(fastPass.removeFastPassGrantsForWorkspace(workspace)).thenReturn(Future())
    // notify leo to clean up resources
    when(leo.cleanupResources(workspace.googleProjectId, workspace.workspaceIdAsUUID, ctx)).thenReturn(Future())
    // delete pets in project
    val pet = UserIdInfo(UUID.randomUUID().toString, "pet-email", None)
    when(sam.listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future.failed(RawlsExceptionWithErrorReport(StatusCodes.NotFound, "")))
    // delete google project
    when(gcs.deleteGoogleProject(workspace.googleProjectId)).thenReturn(Future())
    when(sam.deleteResource(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx))
      .thenReturn(Future())
    // delete workspace and associated records
    when(repo.deleteRawlsWorkspace(workspace)).thenReturn(Future())
    // delete workspace pao in tps
    when(tps.deleteWorkspacePao(any(), any())).thenReturn(Future.unit)
    // delete workflow collection in sam
    when(sam.deleteResource(SamResourceTypeNames.workflowCollection, workspace.workflowCollectionName.get, ctx))
      .thenReturn(Future())
    // delete workspace in sam
    when(sam.deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx)).thenReturn(Future())
    val service = workspaceServiceConstructor(
      samDAO = sam,
      requesterPaysSetupService = requesterPaysService,
      fastPassServiceConstructor = _ => fastPass,
      leonardoService = leo,
      workspaceRepository = repo,
      gcsDAO = gcs,
      submissionsRepository = submissionsRepository,
      policyService = tps
    )(ctx)

    val result = Await.result(service.deleteWorkspace(workspace.toWorkspaceName), Duration.Inf)

    result shouldBe WorkspaceDeletionResult.fromGcpBucketName(workspace.bucketName)
    verify(sam).listAllResourceMemberIds(SamResourceTypeNames.googleProject, workspace.googleProjectId.value, ctx)
  }

  behavior of "getAcl"

  def mockSamForAclTests(): SamDAO = {
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.getUserIdInfo(any(), any()))
      .thenReturn(Future(SamDAO.User(UserIdInfo("fake_user_id", "user@example.com", Option("fake_google_subject_id")))))
    when(samDAO.getUserStatus(any())).thenReturn(Future(Option(enabledUser)))
    samDAO
  }

  def mockWorkspaceRepositoryForAclTests(workspaceType: WorkspaceType): WorkspaceRepository = {
    val workspaceRepository = mock[WorkspaceRepository](RETURNS_SMART_NULLS)
    val googleProjectId = workspaceType match {
      case WorkspaceType.McWorkspace    => GoogleProjectId("")
      case WorkspaceType.RawlsWorkspace => GoogleProjectId("fake-project-id")
    }
    val workspace = this.workspace.copy(workspaceType = workspaceType, googleProjectId = googleProjectId)
    when(workspaceRepository.getWorkspace(any[WorkspaceName](), any())).thenReturn(Future(Option(workspace)))
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
    when(samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx))
      .thenReturn(Future(samWorkspacePoliciesForAclTests(projectOwnerEmail, ownerEmail, writerEmail, readerEmail)))

    val workspaceRepository = mockWorkspaceRepositoryForAclTests(WorkspaceType.RawlsWorkspace)

    val service = workspaceServiceConstructor(workspaceRepository = workspaceRepository, samDAO = samDAO)(ctx)
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

  behavior of "getBucketOptions"

  it should "get the bucket options for a gcp workspace" in {
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future(Some(workspace)))
    val sam = mock[SamDAO]
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
      .thenReturn(Future(true))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.write, ctx))
      .thenReturn(Future(true))
    val settings = mock[WorkspaceSettingRepository]
    when(settings.getWorkspaceSettingOfType(workspace.workspaceIdAsUUID, GcpBucketRequesterPays))
      .thenReturn(Future(None))
    val bucketDetails = mock[WorkspaceBucketOptions]
    val gcs = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    when(gcs.getBucketDetails(workspace.bucketName, workspace.googleProjectId)).thenReturn(Future(bucketDetails))
    val service = workspaceServiceConstructor(samDAO = sam,
                                              workspaceRepository = repository,
                                              gcsDAO = gcs,
                                              workspaceSettingRepository = settings
    )(ctx)

    Await.result(service.getBucketOptions(workspace.toWorkspaceName), Duration.Inf) shouldBe bucketDetails
    verify(gcs).getBucketDetails(workspace.bucketName, workspace.googleProjectId)
  }

  List((false, false), (false, true), (true, true)) foreach { case (isRequesterPays, hasWriteAccess) =>
    it should s"get bucket options and bill the workspace project if a user has " +
      s"${if (hasWriteAccess) "write" else "read"} access and the workspace" +
      s"${if (isRequesterPays) "is" else "is non"} requester pays" in {
        val repository = mock[WorkspaceRepository]
        when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future(Some(workspace)))
        val sam = mock[SamDAO]
        when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
        when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
          .thenReturn(Future(true))
        when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.write, ctx))
          .thenReturn(Future(hasWriteAccess))
        val settings = mock[WorkspaceSettingRepository]
        when(settings.getWorkspaceSettingOfType(workspace.workspaceIdAsUUID, GcpBucketRequesterPays))
          .thenReturn(Future(Some(GcpBucketRequesterPaysSetting(GcpBucketRequesterPaysConfig(isRequesterPays)))))
        val bucketDetails = mock[WorkspaceBucketOptions]
        val gcs = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
        when(gcs.getBucketDetails(workspace.bucketName, workspace.googleProjectId)).thenReturn(Future(bucketDetails))
        val service = workspaceServiceConstructor(samDAO = sam,
                                                  workspaceRepository = repository,
                                                  gcsDAO = gcs,
                                                  workspaceSettingRepository = settings
        )(ctx)

        Await.result(service.getBucketOptions(workspace.toWorkspaceName), Duration.Inf) shouldBe bucketDetails
        verify(gcs).getBucketDetails(workspace.bucketName, workspace.googleProjectId)
      }
  }

  it should "get bucket options and bill a user project if a user provides one to which they have write access" in {
    List((false, false), (false, true), (true, false), (true, true)) foreach { case (isRequesterPays, hasWriteAccess) =>
      val userProjectWs: Workspace = Workspace(
        "test-namespace",
        "user-project-ws",
        UUID.randomUUID().toString,
        "userBucket",
        Some("workflow-collection"),
        new DateTime(),
        new DateTime(),
        "test",
        Map.empty
      )
      val userProjectId: GoogleProjectId = GoogleProjectId("123")

      val repository = mock[WorkspaceRepository]
      when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future(Some(workspace)))
      when(repository.getWorkspaceByGoogleProject(userProjectId)).thenReturn(Future(Some(userProjectWs)))
      val sam = mock[SamDAO]
      when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
      when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
        .thenReturn(Future(true))
      when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.write, ctx))
        .thenReturn(Future(hasWriteAccess))
      when(sam.userHasAction(SamResourceTypeNames.workspace, userProjectWs.workspaceId, SamWorkspaceActions.write, ctx))
        .thenReturn(Future(true))
      val settings = mock[WorkspaceSettingRepository]
      when(settings.getWorkspaceSettingOfType(workspace.workspaceIdAsUUID, GcpBucketRequesterPays))
        .thenReturn(Future(Some(GcpBucketRequesterPaysSetting(GcpBucketRequesterPaysConfig(isRequesterPays)))))
      val bucketDetails = mock[WorkspaceBucketOptions]
      val gcs = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(gcs.getBucketDetails(workspace.bucketName, userProjectId)).thenReturn(Future(bucketDetails))
      val service = workspaceServiceConstructor(samDAO = sam,
                                                workspaceRepository = repository,
                                                gcsDAO = gcs,
                                                workspaceSettingRepository = settings
      )(ctx)

      Await.result(service.getBucketOptions(workspace.toWorkspaceName, Some(userProjectId)),
                   Duration.Inf
      ) shouldBe bucketDetails
      verify(gcs).getBucketDetails(workspace.bucketName, userProjectId)
    }
  }

  it should "fail if a user provides a user project to which they do not have write access" in {
    List((false, false), (false, true), (true, false), (true, true)) foreach { case (isRequesterPays, hasWriteAccess) =>
      val userProjectWs: Workspace = Workspace(
        "test-namespace",
        "user-project-ws",
        UUID.randomUUID().toString,
        "userBucket",
        Some("workflow-collection"),
        new DateTime(),
        new DateTime(),
        "test",
        Map.empty
      )
      val userProjectId: GoogleProjectId = GoogleProjectId("123")

      val repository = mock[WorkspaceRepository]
      when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future(Some(workspace)))
      when(repository.getWorkspaceByGoogleProject(userProjectId)).thenReturn(Future(Some(userProjectWs)))
      val sam = mock[SamDAO]
      when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
      when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
        .thenReturn(Future(true))
      when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.write, ctx))
        .thenReturn(Future(hasWriteAccess))
      when(sam.userHasAction(SamResourceTypeNames.workspace, userProjectWs.workspaceId, SamWorkspaceActions.write, ctx))
        .thenReturn(Future(false))
      val settings = mock[WorkspaceSettingRepository]
      when(settings.getWorkspaceSettingOfType(workspace.workspaceIdAsUUID, GcpBucketRequesterPays))
        .thenReturn(Future(Some(GcpBucketRequesterPaysSetting(GcpBucketRequesterPaysConfig(isRequesterPays)))))
      val gcs = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val service = workspaceServiceConstructor(samDAO = sam,
                                                workspaceRepository = repository,
                                                gcsDAO = gcs,
                                                workspaceSettingRepository = settings
      )(ctx)

      intercept[RawlsExceptionWithErrorReport] {
        Await.result(service.getBucketOptions(workspace.toWorkspaceName, Some(userProjectId)), Duration.Inf)
      }
      verify(gcs, never).getBucketDetails(workspace.bucketName, userProjectId)
    }
  }

  it should "fail on a requester pays workspace for a user who does not provide a user project" in {
    val repository = mock[WorkspaceRepository]
    when(repository.getWorkspace(workspace.toWorkspaceName, None)).thenReturn(Future(Some(workspace)))
    val sam = mock[SamDAO]
    when(sam.getUserStatus(ctx)).thenReturn(Future(Some(enabledUser)))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.read, ctx))
      .thenReturn(Future(true))
    when(sam.userHasAction(SamResourceTypeNames.workspace, workspace.workspaceId, SamWorkspaceActions.write, ctx))
      .thenReturn(Future(false))
    val settings = mock[WorkspaceSettingRepository]
    when(settings.getWorkspaceSettingOfType(workspace.workspaceIdAsUUID, GcpBucketRequesterPays))
      .thenReturn(Future(Some(GcpBucketRequesterPaysSetting(GcpBucketRequesterPaysConfig(true)))))
    val bucketDetails = mock[WorkspaceBucketOptions]
    val gcs = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
    val service = workspaceServiceConstructor(samDAO = sam,
                                              workspaceRepository = repository,
                                              gcsDAO = gcs,
                                              workspaceSettingRepository = settings
    )(ctx)

    intercept[RawlsExceptionWithErrorReport] {
      Await.result(service.getBucketOptions(workspace.toWorkspaceName), Duration.Inf) shouldBe bucketDetails
    }
    verify(gcs, never).getBucketDetails(workspace.bucketName, workspace.googleProjectId)
  }
}
