package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.model.JobReport.StatusEnum
import bio.terra.workspace.model.{AzureContext, ErrorReport => _, JobReport, JobResult, WorkspaceDescription}
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.google.api.services.cloudresourcemanager.model.Project
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, TestData}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.mock.{CustomizableMockSamDAO, MockSamDAO}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceAclManager, RawlsWorkspaceAclManager}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import spray.json.DefaultJsonProtocol._
import spray.json.{enrichAny, JsObject}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Created by dvoet on 4/24/15.
 */
//noinspection TypeAnnotation,TypeAnnotation,NameBooleanParameters,RedundantNewCaseClass,NameBooleanParameters
class WorkspaceApiServiceSpec extends ApiServiceSpec {
  import driver.api._

  trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
    val user: String
    def requireUserInfo(span: Option[Span]): Directive1[UserInfo] =
      // just return the cookie text as the common name
      user match {
        case testData.userProjectOwner.userEmail.value =>
          provide(
            UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userProjectOwner.userSubjectId)
          )
        case testData.userOwner.userEmail.value =>
          provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userOwner.userSubjectId))
        case testData.userWriter.userEmail.value =>
          provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userWriter.userSubjectId))
        case testData.userReader.userEmail.value =>
          provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, testData.userReader.userSubjectId))
        case "no-access" =>
          provide(
            UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212348"))
          )
        case _ =>
          provide(
            UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212349"))
          )
      }
  }

  case class TestApiService(dataSource: SlickDataSource,
                            user: String,
                            gcsDAO: MockGoogleServicesDAO,
                            gpsDAO: MockGooglePubSubDAO
  )(implicit override val executionContext: ExecutionContext)
      extends ApiServices
      with MockUserInfoDirectivesWithUser

  case class TestApiServiceCustomizableMockSam(dataSource: SlickDataSource,
                                               user: String,
                                               gcsDAO: MockGoogleServicesDAO,
                                               gpsDAO: MockGooglePubSubDAO
  )(implicit override val executionContext: ExecutionContext)
      extends ApiServices
      with MockUserInfoDirectivesWithUser {
    override val samDAO: CustomizableMockSamDAO = new CustomizableMockSamDAO(dataSource)
  }

  def withApiServices[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(
    testCode: TestApiService => T
  ): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  val userWriterNoCompute = RawlsUserEmail("writer-access-no-compute")
  val userWriterNoComputeOnProject = RawlsUserEmail("writer-access-no-compute-on-project")

  def withApiServicesSecure[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(
    testCode: TestApiService => T
  ): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO) {
      override val samDAO: MockSamDAO = new MockSamDAO(dataSource) {
        override def userHasAction(resourceTypeName: SamResourceTypeName,
                                   resourceId: String,
                                   action: SamResourceAction,
                                   ctx: RawlsRequestContext
        ): Future[Boolean] = {
          val result = ctx.userInfo.userEmail match {
            case testData.userOwner.userEmail        => true
            case testData.userProjectOwner.userEmail => true
            case testData.userWriter.userEmail =>
              Set(SamWorkspaceActions.read,
                  SamWorkspaceActions.write,
                  SamWorkspaceActions.compute,
                  SamBillingProjectActions.launchBatchCompute
              ).contains(action)
            case `userWriterNoCompute` => Set(SamWorkspaceActions.read, SamWorkspaceActions.write).contains(action)
            case `userWriterNoComputeOnProject` =>
              Set(SamWorkspaceActions.read, SamWorkspaceActions.write, SamWorkspaceActions.compute).contains(action)
            case testData.userReader.userEmail => Set(SamWorkspaceActions.read).contains(action)
            case _                             => false
          }
          Future.successful(result)
        }
      }
      // these need to be overridden to use the new samDAO
      override val rawlsWorkspaceAclManager = new RawlsWorkspaceAclManager(samDAO)
      override val multiCloudWorkspaceAclManager =
        new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, dataSource)
    }
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withApiServicesMockitoSam[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(
    testCode: TestApiService => T
  ): T = {
    val apiService =
      new TestApiService(dataSource, user, spy(new MockGoogleServicesDAO("test")), new MockGooglePubSubDAO) {
        override val samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      }
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def withTestDataApiServicesAndUser[T](user: String)(testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServicesSecure(dataSource, user) { services =>
        testCode(services)
      }
    }

  def withApiServicesMockitoWSMDao[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource,
                                        testData.userOwner.userEmail.value,
                                        spy(new MockGoogleServicesDAO("test")),
                                        new MockGooglePubSubDAO
    ) {
      override val workspaceManagerDAO: WorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    }
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServicesMockitoSam[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServicesMockitoSam(dataSource) { services =>
        testCode(services)
      }
    }

  def withEmptyWorkspaceApiServices[T](user: String)(testCode: TestApiService => T): T =
    withCustomTestDatabase(new EmptyWorkspace) { dataSource: SlickDataSource =>
      withApiServices(dataSource, user)(testCode)
    }

  def withLockedWorkspaceApiServices[T](user: String)(testCode: TestApiService => T): T =
    withCustomTestDatabase(new LockedWorkspace) { dataSource: SlickDataSource =>
      withApiServicesSecure(dataSource, user)(testCode)
    }

  def withApiServicesCustomizableMockSam[T](dataSource: SlickDataSource,
                                            user: String = testData.userProjectOwner.userEmail.value
  )(testCode: TestApiServiceCustomizableMockSam => T): T = {
    val apiService = new TestApiServiceCustomizableMockSam(dataSource,
                                                           user,
                                                           new MockGoogleServicesDAO("test"),
                                                           new MockGooglePubSubDAO
    )
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServicesCustomizableMockSam[T](testCode: TestApiServiceCustomizableMockSam => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServicesCustomizableMockSam(dataSource)(testCode)
    }

  def withEmptyDatabaseAndApiServices[T](testCode: TestApiService => T): T =
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def withApiServicesMockitoGcsDao[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      val apiService = new TestApiService(dataSource,
                                          testData.userProjectOwner.userEmail.value,
                                          mock[MockGoogleServicesDAO](RETURNS_SMART_NULLS),
                                          new MockGooglePubSubDAO
      )
      try
        testCode(apiService)
      finally
        apiService.cleanupSupervisor
    }

  class TestWorkspaces() extends TestData {
    val userProjectOwner = RawlsUser(
      UserInfo(RawlsUserEmail("project-owner-access"),
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543210101")
      )
    )
    val userOwner = RawlsUser(
      UserInfo(testData.userOwner.userEmail,
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212345")
      )
    )
    val userWriter = RawlsUser(
      UserInfo(testData.userWriter.userEmail,
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212346")
      )
    )
    val userReader = RawlsUser(
      UserInfo(testData.userReader.userEmail,
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212347")
      )
    )

    val billingProject = RawlsBillingProject(RawlsBillingProjectName("ns"), CreationStatuses.Ready, None, None)

    val workspaceName = WorkspaceName(billingProject.projectName.value, "testworkspace")

    val workspace2Name = WorkspaceName(billingProject.projectName.value, "testworkspace2")

    val workspace3Name = WorkspaceName(billingProject.projectName.value, "testworkspace3")

    val workspace1Id = UUID.randomUUID().toString
    val workspace = makeWorkspaceWithUsers(
      billingProject,
      workspaceName.name,
      workspace1Id,
      "bucket1",
      Some(workspace1Id),
      testDate,
      testDate,
      "testUser",
      Map(AttributeName.withDefaultNS("a") -> AttributeString("x")),
      false
    )

    val workspace2Id = UUID.randomUUID().toString
    val workspace2 = makeWorkspaceWithUsers(
      billingProject,
      workspace2Name.name,
      workspace2Id,
      "bucket2",
      Some(workspace2Id),
      testDate,
      testDate,
      "testUser",
      Map(AttributeName.withDefaultNS("b") -> AttributeString("y")),
      false
    )

    val sample1 = Entity("sample1", "sample", Map.empty)
    val sample2 = Entity("sample2", "sample", Map.empty)
    val sample3 = Entity("sample3", "sample", Map.empty)
    val sample4 = Entity("sample4", "sample", Map.empty)
    val sample5 = Entity("sample5", "sample", Map.empty)
    val sample6 = Entity("sample6", "sample", Map.empty)
    val sampleSet = Entity(
      "sampleset",
      "sample_set",
      Map(
        AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
          Seq(
            sample1.toReference,
            sample2.toReference,
            sample3.toReference
          )
        )
      )
    )

    val methodConfig = MethodConfiguration(
      "dsde",
      "testConfig",
      Some("Sample"),
      None,
      Map("param1" -> AttributeString("foo")),
      Map("out1" -> AttributeString("bar"), "out2" -> AttributeString("splat")),
      AgoraMethod(workspaceName.namespace, "method-a", 1)
    )
    val methodConfigName = MethodConfigurationName(methodConfig.name, methodConfig.namespace, workspaceName)
    val submissionTemplate = createTestSubmission(
      workspace,
      methodConfig,
      sampleSet,
      WorkbenchEmail(userOwner.userEmail.value),
      Seq(sample1, sample2, sample3),
      Map(sample1 -> testData.inputResolutions,
          sample2 -> testData.inputResolutions,
          sample3 -> testData.inputResolutions
      ),
      Seq(sample4, sample5, sample6),
      Map(sample4 -> testData.inputResolutions2,
          sample5 -> testData.inputResolutions2,
          sample6 -> testData.inputResolutions2
      )
    )
    val submissionSuccess = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Done,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Succeeded))
    )
    val submissionFail = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Done,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Failed))
    )
    val submissionRunning1 = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Submitted,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Running))
    )
    val submissionRunning2 = submissionTemplate.copy(
      submissionId = UUID.randomUUID().toString,
      status = SubmissionStatuses.Submitted,
      workflows = submissionTemplate.workflows.map(_.copy(status = WorkflowStatuses.Running))
    )

    override def save() =
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject),
        workspaceQuery.createOrUpdate(workspace),
        workspaceQuery.createOrUpdate(workspace2),
        withWorkspaceContext(workspace) { ctx =>
          DBIO.seq(
            entityQuery.save(ctx, sample1),
            entityQuery.save(ctx, sample2),
            entityQuery.save(ctx, sample3),
            entityQuery.save(ctx, sample4),
            entityQuery.save(ctx, sample5),
            entityQuery.save(ctx, sample6),
            entityQuery.save(ctx, sampleSet),
            methodConfigurationQuery.create(ctx, methodConfig),
            submissionQuery.create(ctx, submissionSuccess),
            submissionQuery.create(ctx, submissionFail),
            submissionQuery.create(ctx, submissionRunning1),
            submissionQuery.create(ctx, submissionRunning2)
          )
        }
      )
  }

  val testWorkspaces = new TestWorkspaces

  def withTestWorkspacesApiServices[T](testCode: TestApiService => T): T =
    withCustomTestDatabase(testWorkspaces) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def withTestWorkspacesApiServicesAndUser[T](user: String)(testCode: TestApiService => T): T =
    withCustomTestDatabase(testWorkspaces) { dataSource: SlickDataSource =>
      withApiServicesSecure(dataSource, user)(testCode)
    }

  "WorkspaceApi" should "return 201 for post to workspaces" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      Map.empty
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, responseAs[String]) {
          status
        }
        assertResult(newWorkspace) {
          val ws = runAndWait(workspaceQuery.findByName(newWorkspace.toWorkspaceName)).get
          WorkspaceRequest(ws.namespace, ws.name, ws.attributes, Option(Set.empty))
        }
        assertResult(newWorkspace) {
          val ws = responseAs[WorkspaceDetails]
          WorkspaceRequest(ws.namespace, ws.name, ws.attributes.getOrElse(Map()), Option(Set.empty))
        }
        // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
        assertResult(Some(Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newWorkspace.path))))) {
          header("Location")
        }
      }
  }

  it should "return 201 for an MC workspace" in withTestDataApiServices { services =>
    val billingProfileId = UUID.randomUUID()
    val billingProject = RawlsBillingProject(RawlsBillingProjectName("test-azure-bp"),
                                             CreationStatuses.Ready,
                                             None,
                                             None,
                                             billingProfileId = Some(billingProfileId.toString)
    )
    runAndWait(
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject)
      )
    )
    when(services.billingProfileManagerDAO.getBillingProfile(any[UUID], any[RawlsRequestContext])).thenReturn(
      Some(
        new ProfileModel()
          .id(billingProfileId)
          .tenantId(UUID.randomUUID())
          .subscriptionId(UUID.randomUUID())
          .cloudPlatform(bio.terra.profile.model.CloudPlatform.AZURE)
          .managedResourceGroupId("fake")
      )
    )
    val newWorkspace = WorkspaceRequest(
      namespace = "test-azure-bp",
      name = "newWorkspace",
      Map.empty
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, responseAs[String]) {
          status
        }
        val ws = responseAs[WorkspaceDetails]
        ws.workspaceType shouldBe Some(WorkspaceType.McWorkspace)
      }
  }

  it should "return 403 on create workspace with invalid-namespace attributes" in withTestDataApiServices { services =>
    val invalidAttrNamespace = "invalid"

    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      Map(AttributeName(invalidAttrNamespace, "attribute") -> AttributeString("foo"))
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(invalidAttrNamespace))
      }
  }

  it should "return 403 on create workspace with library-namespace attributes" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      Map(AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo"))
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "concurrently create workspaces" in withTestDataApiServices { services =>
    def generator(i: Int): ReadAction[Option[Workspace]] = {
      val newWorkspace = WorkspaceRequest(
        namespace = testData.wsName.namespace,
        name = s"newWorkspace$i",
        Map.empty
      )

      Post(s"/workspaces", httpJson(newWorkspace)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          workspaceQuery.findByName(newWorkspace.toWorkspaceName)
        }
    }

    runMultipleAndWait(100)(generator)
  }

  it should "return 201 for post to workspaces with noWorkspaceOwner" in withTestDataApiServices { services =>
    val newWorkspace = WorkspaceRequest(
      namespace = testData.wsName.namespace,
      name = "newWorkspace",
      Map.empty,
      noWorkspaceOwner = Option(true)
    )

    Post(s"/workspaces", httpJson(newWorkspace)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, responseAs[String]) {
          status
        }
        assertResult(newWorkspace) {
          val ws = runAndWait(workspaceQuery.findByName(newWorkspace.toWorkspaceName)).get
          WorkspaceRequest(ws.namespace, ws.name, ws.attributes, Option(Set.empty), noWorkspaceOwner = Option(true))
        }
        assertResult(newWorkspace) {
          val ws = responseAs[WorkspaceDetails]
          WorkspaceRequest(ws.namespace,
                           ws.name,
                           ws.attributes.getOrElse(Map()),
                           Option(Set.empty),
                           noWorkspaceOwner = Option(true)
          )
        }
        // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
        assertResult(Some(Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(newWorkspace.path))))) {
          header("Location")
        }
      }
  }

  private def toRawlsRequestContext(user: RawlsUser) = RawlsRequestContext(
    UserInfo(user.userEmail, OAuth2BearerToken(""), 0, user.userSubjectId)
  )
  private def populateBillingProjectPolicies(services: TestApiServiceCustomizableMockSam,
                                             workspace: Workspace = testData.workspace
  ) = {
    val populateAcl = for {
      _ <- services.samDAO.registerUser(toRawlsRequestContext(testData.userProjectOwner))

      _ <- services.samDAO.overwritePolicy(
        SamResourceTypeNames.billingProject,
        workspace.namespace,
        SamBillingProjectPolicyNames.owner,
        SamPolicy(Set(WorkbenchEmail(testData.userProjectOwner.userEmail.value)),
                  Set(SamBillingProjectActions.createWorkspace),
                  Set(SamBillingProjectRoles.owner)
        ),
        testContext
      )
      _ <- services.samDAO.overwritePolicy(
        SamResourceTypeNames.workspace,
        workspace.workspaceId,
        SamWorkspacePolicyNames.reader,
        SamPolicy(Set(WorkbenchEmail(testData.userProjectOwner.userEmail.value)),
                  Set(SamWorkspaceActions.read),
                  Set(SamWorkspaceRoles.reader)
        ),
        testContext
      )

    } yield ()

    Await.result(populateAcl, Duration.Inf)
  }

  it should "have an empty owner policy when creating a workspace with noWorkspaceOwner" in withTestDataApiServicesCustomizableMockSam {
    services =>
      val newWorkspace = WorkspaceRequest(
        namespace = testData.wsName.namespace,
        name = "newWorkspace",
        Map.empty,
        noWorkspaceOwner = Option(true)
      )

      populateBillingProjectPolicies(services)

      val expectedOwnerPolicyEmail = ""

      Post(s"/workspaces", httpJson(newWorkspace)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created, responseAs[String]) {
            status
          }
          // check the workspace owner policy
          assertResult(expectedOwnerPolicyEmail) {
            val ws = runAndWait(workspaceQuery.findByName(newWorkspace.toWorkspaceName)).get
            val policiesWithNameAndEmail = Await.result(
              services.samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, ws.workspaceId, testContext),
              Duration.Inf
            )
            val actualOwnerPolicyEmail =
              policiesWithNameAndEmail.find(_.policyName == SamWorkspacePolicyNames.owner).get.email.value
            actualOwnerPolicyEmail
          }
        }
  }

  it should "return 403 for post to workspaces with noWorkspaceOwner without BP owner permissions" in withTestDataApiServicesMockitoSam {
    services =>
      val newWorkspace = WorkspaceRequest(
        namespace = testData.wsName.namespace,
        name = "newWorkspace",
        Map.empty,
        noWorkspaceOwner = Option(true)
      )

      // User has BP user role, but not owner role
      when(
        services.samDAO.listUserRolesForResource(
          ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
          ArgumentMatchers.eq(testData.billingProject.projectName.value),
          any[RawlsRequestContext]
        )
      ).thenReturn(
        Future.successful(
          Set[SamResourceRole](SamBillingProjectRoles.workspaceCreator, SamBillingProjectRoles.batchComputeUser)
        )
      )

      // User has BP user permissions and therefore can create workspaces
      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
          ArgumentMatchers.eq(testData.billingProject.projectName.value),
          ArgumentMatchers.eq(SamBillingProjectActions.createWorkspace),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(true))

      Post(s"/workspaces", httpJson(newWorkspace)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden, responseAs[String]) {
            status
          }

          val errorText = responseAs[ErrorReport].message
          assert(errorText.contains(newWorkspace.namespace))
        }
  }

  it should "get a workspace" in withTestWorkspacesApiServices { services =>
    Get(testWorkspaces.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val dateTime = currentTime()
        assertResult(
          WorkspaceResponse(
            Option(WorkspaceAccessLevels.Owner),
            Option(true),
            Option(true),
            Option(true),
            WorkspaceDetails.fromWorkspaceAndOptions(testWorkspaces.workspace.copy(lastModified = dateTime),
                                                     Some(Set()),
                                                     true,
                                                     Some(WorkspaceCloudPlatform.Gcp)
            ),
            Option(WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2)),
            Option(WorkspaceBucketOptions(false)),
            Option(Set.empty),
            None,
            None
          )
        ) {
          val response = responseAs[WorkspaceResponse]
          WorkspaceResponse(
            response.accessLevel,
            response.canShare,
            response.canCompute,
            response.catalog,
            response.workspace.copy(lastModified = dateTime),
            response.workspaceSubmissionStats,
            response.bucketOptions,
            response.owners,
            None,
            None
          )
        }
      }
  }

  it should "get a workspace by id" in withTestWorkspacesApiServices { services =>
    Get(s"/workspaces/id/${testWorkspaces.workspace.workspaceId}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val dateTime = currentTime()
        assertResult(
          WorkspaceResponse(
            Option(WorkspaceAccessLevels.Owner),
            Option(true),
            Option(true),
            Option(true),
            WorkspaceDetails.fromWorkspaceAndOptions(testWorkspaces.workspace.copy(lastModified = dateTime),
                                                     Some(Set()),
                                                     true,
                                                     Some(WorkspaceCloudPlatform.Gcp)
            ),
            Option(WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2)),
            Option(WorkspaceBucketOptions(false)),
            Option(Set.empty),
            None,
            None
          )
        ) {
          val response = responseAs[WorkspaceResponse]
          WorkspaceResponse(
            response.accessLevel,
            response.canShare,
            response.canCompute,
            response.catalog,
            response.workspace.copy(lastModified = dateTime),
            response.workspaceSubmissionStats,
            response.bucketOptions,
            response.owners,
            None,
            None
          )
        }
      }
  }

  // see also WorkspaceApiGetOptionsSpec for additional tests related to get-workspace

  it should "return 404 getting a non-existent workspace" in withTestDataApiServices { services =>
    Get(testData.workspace.copy(name = "DNE").path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 404 getting a non-existent workspace by id" in withTestDataApiServices { services =>
    Get(s"/workspaces/id/${UUID.randomUUID()}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 400 getting a workspace by id with an invalid UUID" in withTestDataApiServices { services =>
    Get("/workspaces/id/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "delete a workspace" in withTestDataApiServices { services =>
    Delete(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.workspace.toWorkspaceName))
    }
  }

  it should "delete all entities when deleting a workspace" in withTestDataApiServices { services =>
    // check that length of result is > 0:
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(entityQuery.findActiveEntityByWorkspace(workspaceContext.workspaceIdAsUUID).length.result) > 0
      }
    }
    // delete the workspace
    Delete(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }
    // now you should have no entities listed
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(entityQuery.findActiveEntityByWorkspace(workspaceContext.workspaceIdAsUUID).length.result) == 0
      }
    }
  }

  it should "delete all method configs when deleting a workspace" in withTestDataApiServices { services =>
    // check that length of result is > 0:
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(
          methodConfigurationQuery
            .findActiveByName(workspaceContext.workspaceIdAsUUID,
                              testData.agoraMethodConfig.namespace,
                              testData.agoraMethodConfig.name
            )
            .length
            .result
        ) > 0
      }
    }
    // delete the workspace
    Delete(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }
    // now you should have no method configs listed
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(
          methodConfigurationQuery
            .findActiveByName(workspaceContext.workspaceIdAsUUID,
                              testData.agoraMethodConfig.namespace,
                              testData.agoraMethodConfig.name
            )
            .length
            .result
        ) == 0
      }
    }
  }

  it should "delete all submissions when deleting a workspace" in withTestDataApiServices { services =>
    // check that length of result is > 0:
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(submissionQuery.findByWorkspaceId(workspaceContext.workspaceIdAsUUID).length.result) > 0
      }
    }
    // delete the workspace
    Delete(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }
    // now you should have no submissions listed
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      assert {
        runAndWait(submissionQuery.findByWorkspaceId(workspaceContext.workspaceIdAsUUID).length.result) == 0
      }
    }
  }

  it should "delete workspace and workflow collection sam resource when deleting a workspace" in withTestDataApiServicesMockitoSam {
    services =>
      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          ArgumentMatchers.eq(SamWorkspaceActions.delete),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(true))
      when(
        services.samDAO.getUserStatus(any[RawlsRequestContext])
      ).thenReturn(
        Future.successful(
          Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
        )
      )
      when(
        services.samDAO.listResourceChildren(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          any[RawlsRequestContext]()
        )
      ).thenReturn(Future(Seq[SamFullyQualifiedResourceId]()))
      when(
        services.samDAO.listResourceChildren(
          ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
          ArgumentMatchers.eq(testData.workspace.googleProjectId.value),
          any[RawlsRequestContext]()
        )
      ).thenReturn(Future(Seq[SamFullyQualifiedResourceId]()))
      when(
        services.samDAO.deleteResource(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(()))

      when(
        services.samDAO.deleteResource(
          ArgumentMatchers.eq(SamResourceTypeNames.workflowCollection),
          ArgumentMatchers.eq(testData.workspace.workflowCollectionName.get),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(()))

      // mocking for deleting a google project
      val petSAJson = "petJson"
      val googleProjectId = testData.workspace.googleProjectId
      when(
        services.samDAO.listAllResourceMemberIds(ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
                                                 ArgumentMatchers.eq(googleProjectId.value),
                                                 ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(
        Future.successful(
          Set(UserIdInfo(userInfo.userSubjectId.value, userInfo.userEmail.value, Option("googleSubId")))
        )
      )
      when(services.samDAO.getPetServiceAccountKeyForUser(googleProjectId, userInfo.userEmail))
        .thenReturn(Future.successful(petSAJson))
      when(
        services.samDAO.getUserPetServiceAccount(
          any[RawlsRequestContext],
          ArgumentMatchers.eq(testData.workspace.googleProjectId)
        )
      ).thenReturn(Future.successful(WorkbenchEmail("pet-email@domain.org")))

      when(services.samDAO.deleteUserPetServiceAccount(ArgumentMatchers.eq(googleProjectId), any[RawlsRequestContext]))
        .thenReturn(
          Future.successful()
        ) // uses any[RawlsRequestContext] here since MockGoogleServicesDAO defaults to returning a different UserInfo
      when(
        services.samDAO.deleteResource(
          ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
          ArgumentMatchers.eq(googleProjectId.value),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful())

      Delete(testData.workspace.path) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Accepted, responseAs[String]) {
            status
          }
        }

      verify(services.samDAO).deleteResource(
        ArgumentMatchers.eq(SamResourceTypeNames.workspace),
        ArgumentMatchers.eq(testData.workspace.workspaceId),
        any[RawlsRequestContext]
      )

      verify(services.samDAO).deleteResource(
        ArgumentMatchers.eq(SamResourceTypeNames.workflowCollection),
        ArgumentMatchers.eq(testData.workspace.workflowCollectionName.get),
        any[RawlsRequestContext]
      )
  }

  it should "delete the google project in the workspace when deleting a workspace" in withTestDataApiServicesMockitoSam {
    services =>
      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          ArgumentMatchers.eq(SamWorkspaceActions.delete),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(true))
      when(
        services.samDAO.getUserStatus(any[RawlsRequestContext])
      ).thenReturn(
        Future.successful(
          Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
        )
      )
      when(
        services.samDAO.listResourceChildren(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          any[RawlsRequestContext]()
        )
      ).thenReturn(Future(Seq[SamFullyQualifiedResourceId]()))
      when(
        services.samDAO.listResourceChildren(
          ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
          ArgumentMatchers.eq(testData.workspace.googleProjectId.value),
          any[RawlsRequestContext]()
        )
      ).thenReturn(Future(Seq[SamFullyQualifiedResourceId]()))
      when(
        services.samDAO.deleteResource(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(()))

      when(
        services.samDAO.deleteResource(
          ArgumentMatchers.eq(SamResourceTypeNames.workflowCollection),
          ArgumentMatchers.eq(testData.workspace.workflowCollectionName.get),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(()))

      // mocking for deleting a google project
      val petSAJson = "petJson"
      val googleProjectId = testData.workspace.googleProjectId
      when(
        services.samDAO.listAllResourceMemberIds(ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
                                                 ArgumentMatchers.eq(googleProjectId.value),
                                                 ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(
        Future.successful(
          Set(UserIdInfo(userInfo.userSubjectId.value, userInfo.userEmail.value, Option("googleSubId")))
        )
      )
      when(services.samDAO.getPetServiceAccountKeyForUser(googleProjectId, userInfo.userEmail))
        .thenReturn(Future.successful(petSAJson))
      when(
        services.samDAO.getUserPetServiceAccount(
          any[RawlsRequestContext],
          ArgumentMatchers.eq(testData.workspace.googleProjectId)
        )
      ).thenReturn(Future.successful(WorkbenchEmail("pet-email@domain.org")))
      when(services.samDAO.deleteUserPetServiceAccount(ArgumentMatchers.eq(googleProjectId), any[RawlsRequestContext]))
        .thenReturn(
          Future.successful()
        ) // uses any[RawlsRequestContext] here since MockGoogleServicesDAO defaults to returning a different UserInfo
      when(
        services.samDAO.deleteResource(
          ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
          ArgumentMatchers.eq(googleProjectId.value),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful())

      Delete(testData.workspace.path) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Accepted, responseAs[String]) {
            status
          }
        }

      verify(services.samDAO).deleteResource(
        ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
        ArgumentMatchers.eq(googleProjectId.value),
        any[RawlsRequestContext]
      )
      verify(services.gcsDAO).deleteGoogleProject(ArgumentMatchers.eq(googleProjectId))
  }

  it should "tolerate a missing google-project Sam resource when deleting a workspace" in withTestDataApiServicesMockitoSam {
    services =>
      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          ArgumentMatchers.eq(SamWorkspaceActions.delete),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(true))
      when(
        services.samDAO.getUserStatus(any[RawlsRequestContext])
      ).thenReturn(
        Future.successful(
          Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
        )
      )
      when(
        services.samDAO.listResourceChildren(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          any[RawlsRequestContext]()
        )
      ).thenReturn(Future(Seq[SamFullyQualifiedResourceId]()))
      when(
        services.samDAO.listResourceChildren(
          ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
          ArgumentMatchers.eq(testData.workspace.googleProjectId.value),
          any[RawlsRequestContext]()
        )
      ).thenReturn(Future(Seq[SamFullyQualifiedResourceId]()))
      when(
        services.samDAO.deleteResource(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(()))

      when(
        services.samDAO.deleteResource(
          ArgumentMatchers.eq(SamResourceTypeNames.workflowCollection),
          ArgumentMatchers.eq(testData.workspace.workflowCollectionName.get),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(()))

      // mocking for deleting a google project
      val petSAJson = "petJson"
      val googleProjectId = testData.workspace.googleProjectId
      when(
        services.samDAO.listAllResourceMemberIds(ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
                                                 ArgumentMatchers.eq(googleProjectId.value),
                                                 ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(
        Future.successful(
          Set(UserIdInfo(userInfo.userSubjectId.value, userInfo.userEmail.value, Option("googleSubId")))
        )
      )
      when(services.samDAO.getPetServiceAccountKeyForUser(googleProjectId, userInfo.userEmail))
        .thenReturn(Future.successful(petSAJson))
      when(
        services.samDAO.getUserPetServiceAccount(
          any[RawlsRequestContext],
          ArgumentMatchers.eq(testData.workspace.googleProjectId)
        )
      ).thenReturn(Future.successful(WorkbenchEmail("pet-email@domain.org")))
      when(services.samDAO.deleteUserPetServiceAccount(ArgumentMatchers.eq(googleProjectId), any[RawlsRequestContext]))
        .thenReturn(
          Future.successful()
        ) // uses any[RawlsRequestContext] here since MockGoogleServicesDAO defaults to returning a different UserInfo
      when(
        services.samDAO.deleteResource(
          ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
          ArgumentMatchers.eq(googleProjectId.value),
          any[RawlsRequestContext]
        )
      ).thenReturn(
        Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"resource not found")))
      )

      Delete(testData.workspace.path) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Accepted, responseAs[String]) {
            status
          }
        }

      verify(services.samDAO).deleteResource(
        ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
        ArgumentMatchers.eq(googleProjectId.value),
        any[RawlsRequestContext]
      )
      verify(services.gcsDAO).deleteGoogleProject(ArgumentMatchers.eq(googleProjectId))
  }

  it should "delete an Azure workspace" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      withApiServicesMockitoWSMDao(dataSource) { services =>
        val billingProject = RawlsBillingProject(RawlsBillingProjectName("test-azure-bp"),
                                                 CreationStatuses.Ready,
                                                 None,
                                                 None,
                                                 billingProfileId = Some(UUID.randomUUID().toString)
        )
        val wsId = UUID.randomUUID()
        val azureWorkspace = Workspace.buildReadyMcWorkspace(
          namespace = "test-azure-bp",
          name = s"test-azure-ws-${wsId}",
          workspaceId = wsId.toString,
          createdDate = DateTime.now,
          lastModified = DateTime.now,
          createdBy = "testuser@example.com",
          attributes = Map()
        )

        runAndWait(
          DBIO.seq(
            rawlsBillingProjectQuery.create(billingProject),
            workspaceQuery.createOrUpdate(azureWorkspace)
          )
        )

        when(services.workspaceManagerDAO.getWorkspace(any[UUID], any[RawlsRequestContext]))
          .thenReturn(new WorkspaceDescription().id(UUID.randomUUID()).azureContext(new AzureContext()))
        when(services.workspaceManagerDAO.deleteWorkspaceV2(any[UUID], any[RawlsRequestContext]))
          .thenReturn(new JobResult().jobReport(new JobReport().id(UUID.randomUUID.toString)))
        when(services.workspaceManagerDAO.getDeleteWorkspaceV2Result(any[UUID], any[String], any[RawlsRequestContext]))
          .thenReturn(
            new JobResult().jobReport(new JobReport().id(UUID.randomUUID.toString).status(StatusEnum.SUCCEEDED))
          )

        Delete(azureWorkspace.path) ~>
          sealRoute(services.workspaceRoutes) ~>
          check {
            assertResult(StatusCodes.Accepted) {
              status
            }
            responseAs[Option[String]] shouldBe Some("Your workspace has been deleted.")
          }
        assertResult(None) {
          runAndWait(workspaceQuery.findByName(azureWorkspace.toWorkspaceName))
        }
        verify(services.workspaceManagerDAO).deleteWorkspaceV2(ArgumentMatchers.eq(azureWorkspace.workspaceIdAsUUID),
                                                               any[RawlsRequestContext]
        )
      }
    }
  }

  // TODO - once workspace migration is complete and there are no more v1 workspaces or v1 billing projects, we can remove this https://broadworkbench.atlassian.net/browse/CA-1118
  it should "delete a v1 workspace" in withEmptyDatabaseAndApiServices { services =>
    val billingProject = RawlsBillingProject(RawlsBillingProjectName("v1-test-ns"), CreationStatuses.Ready, None, None)
    val v1Workspace = new Workspace(
      billingProject.projectName.value,
      "myWorkspaceV1",
      UUID.randomUUID().toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "test",
      Map.empty,
      false,
      WorkspaceVersions.V1,
      GoogleProjectId("googleprojectid"),
      Option(GoogleProjectNumber("googleProjectNumber")),
      Option(RawlsBillingAccountName("fakeBillingAcct")),
      None,
      Option(currentTime()),
      WorkspaceType.RawlsWorkspace,
      WorkspaceState.Ready
    )

    runAndWait(
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject),
        workspaceQuery.createOrUpdate(v1Workspace)
      )
    )

    Delete(v1Workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted, responseAs[String]) {
          status
        }
      }

    assertResult(None) {
      runAndWait(workspaceQuery.findByName(v1Workspace.toWorkspaceName))
    }
  }

  // see also WorkspaceApiListOptionsSpec for tests against list-workspaces that use the ?fields query param
  it should "list workspaces" in withTestWorkspacesApiServices { services =>
    Get("/workspaces") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val dateTime = currentTime()
        assertResult(
          Set(
            WorkspaceListResponse(
              WorkspaceAccessLevels.Owner,
              WorkspaceDetails.fromWorkspaceAndOptions(testWorkspaces.workspace.copy(lastModified = dateTime),
                                                       Option(Set.empty),
                                                       true,
                                                       Some(WorkspaceCloudPlatform.Gcp)
              ),
              Option(WorkspaceSubmissionStats(Option(testDate), Option(testDate), 2)),
              false,
              Some(List.empty)
            ),
            WorkspaceListResponse(
              WorkspaceAccessLevels.Owner,
              WorkspaceDetails.fromWorkspaceAndOptions(testWorkspaces.workspace2.copy(lastModified = dateTime),
                                                       Option(Set.empty),
                                                       true,
                                                       Some(WorkspaceCloudPlatform.Gcp)
              ),
              Option(WorkspaceSubmissionStats(None, None, 0)),
              false,
              Some(List.empty)
            )
          )
        ) {
          responseAs[Array[WorkspaceListResponse]]
            .toSet[WorkspaceListResponse]
            .map(wslr => wslr.copy(workspace = wslr.workspace.copy(lastModified = dateTime)))
        }
      }
  }

  it should "list workspaces even when a Sam resourceId has an invalid UUID" in withTestDataApiServicesMockitoSam {
    services =>
      // override the call to Sam so that it returns non-uuid values in its resource id values
      // note the second item in the set has a non-UUID as its resourceId. That policy should be ignored silently.
      when(
        services.samDAO.listUserResources(ArgumentMatchers.eq(SamResourceTypeNames.workspace), any[RawlsRequestContext])
      )
        .thenReturn(
          Future.successful(
            Seq(
              SamUserResource(
                testData.workspace.workspaceId,
                SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
                SamRolesAndActions(Set.empty, Set.empty),
                SamRolesAndActions(Set.empty, Set.empty),
                Set.empty,
                Set.empty
              ),
              SamUserResource(
                "invalid-uuid-" + testData.workspaceSubmittedSubmission.workspaceId,
                SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
                SamRolesAndActions(Set.empty, Set.empty),
                SamRolesAndActions(Set.empty, Set.empty),
                Set.empty,
                Set.empty
              ),
              SamUserResource(
                testData.workspaceFailedSubmission.workspaceId,
                SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
                SamRolesAndActions(Set.empty, Set.empty),
                SamRolesAndActions(Set.empty, Set.empty),
                Set.empty,
                Set.empty
              )
            )
          )
        )

      Get("/workspaces") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }

          val dateTime = currentTime()
          assertResult(
            Set(
              WorkspaceListResponse(
                WorkspaceAccessLevels.Owner,
                WorkspaceDetails.fromWorkspaceAndOptions(testData.workspace.copy(lastModified = dateTime),
                                                         Option(Set.empty),
                                                         true,
                                                         Some(WorkspaceCloudPlatform.Gcp)
                ),
                Option(WorkspaceSubmissionStats(None, None, 8)),
                false,
                Some(List.empty)
              ),
              WorkspaceListResponse(
                WorkspaceAccessLevels.Owner,
                WorkspaceDetails.fromWorkspaceAndOptions(
                  testData.workspaceFailedSubmission.copy(lastModified = dateTime),
                  Option(Set.empty),
                  true,
                  Some(WorkspaceCloudPlatform.Gcp)
                ),
                Option(WorkspaceSubmissionStats(None, Option(testDate), 0)),
                false,
                Some(List.empty)
              )
            )
          ) {
            responseAs[Array[WorkspaceListResponse]]
              .toSet[WorkspaceListResponse]
              .map(wslr => wslr.copy(workspace = wslr.workspace.copy(lastModified = dateTime)))
          }
        }
  }

  it should "return 404 Not Found on clone if the source workspace cannot be found" in withTestDataApiServices {
    services =>
      val cloneSrc = testData.workspace.copy(name = "test_nonexistent_1")
      val cloneDest = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_nonexistent_2", Map.empty)
      Post(s"${cloneSrc.path}/clone", httpJson(cloneDest)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
      Get(testData.sample2.path(cloneDest)) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
      Patch(
        testData.sample2.path(cloneDest),
        httpJson(
          Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation)
        )
      ) ~>
        sealRoute(services.entityRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 400 Bad Request on clone if the copyFilesWithPrefix is the empty string" in withTestDataApiServices {
    services =>
      val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace,
                                           name = "test_copy",
                                           Map.empty,
                                           copyFilesWithPrefix = Some("")
      )
      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 200 on update workspace attributes" in withTestDataApiServices { services =>
    Patch(
      testData.workspace.path,
      httpJson(
        Seq(AddUpdateAttribute(AttributeName.withDefaultNS("boo"), AttributeString("bang")): AttributeUpdateOperation)
      )
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult(Option(AttributeString("bang"))) {
          runAndWait(workspaceQuery.findByName(testData.wsName)).get.attributes.get(AttributeName.withDefaultNS("boo"))
        }
      }

    Patch(testData.workspace.path,
          httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        assertResult(None) {
          runAndWait(workspaceQuery.findByName(testData.wsName)).get.attributes.get(AttributeName.withDefaultNS("boo"))
        }
      }
  }

  it should "return 403 on update workspace with invalid-namespace attributes" in withTestDataApiServices { services =>
    val name = AttributeName("invalid", "misc")
    val attr = AttributeString("foo")

    Patch(testData.workspace.path, httpJson(Seq(AddUpdateAttribute(name, attr): AttributeUpdateOperation))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }

        val errorText = responseAs[ErrorReport].message
        assert(errorText.contains(name.namespace))
      }
  }

  it should "return 400 on update with workspace attributes that specify list as value" in withTestDataApiServices {
    services =>
      // we can't make this non-sensical json from our object hierarchy; we have to craft it by hand
      val testPayload =
        """
          |[{
          |    "op" : "AddUpdateAttribute",
          |    "attributeName" : "something",
          |    "addUpdateAttribute" : {
          |      "itemsType" : "SomeValueNotExpected",
          |      "items" : ["foo", "bar", "baz"]
          |    }
          |}]
      """.stripMargin

      Patch(testData.workspace.path, httpJson(testPayload)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 if the bucket location requested is in an invalid format" in withTestDataApiServices {
    services =>
      val newWorkspace = WorkspaceRequest(
        namespace = testData.wsName.namespace,
        name = "newWorkspace",
        attributes = Map.empty,
        authorizationDomain = None,
        bucketLocation = Option("EU")
      )

      Post(s"/workspaces", httpJson(newWorkspace)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          val errorText = responseAs[ErrorReport].message
          assert(status == StatusCodes.BadRequest)
          assert(
            errorText.contains(
              "Workspace bucket location must be a single region of format: [A-Za-z]+-[A-Za-z]+[0-9]+ or the default bucket location ('US')."
            )
          )
        }
  }

  it should "return 201 if the bucket location requested is the default bucket location" in withTestDataApiServices {
    services =>
      val newWorkspace = WorkspaceRequest(
        namespace = testData.wsName.namespace,
        name = "newWorkspace",
        attributes = Map.empty,
        authorizationDomain = None,
        bucketLocation = Option("US")
      )

      Post(s"/workspaces", httpJson(newWorkspace)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assert(status == StatusCodes.Created)
        }
  }

  it should "concurrently update workspace attributes" in withTestDataApiServices { services =>
    def generator(i: Int): ReadAction[Option[Workspace]] =
      Patch(testData.workspace.path,
            httpJson(
              Seq(
                AddUpdateAttribute(AttributeName.withDefaultNS("boo"),
                                   AttributeString(s"bang$i")
                ): AttributeUpdateOperation
              )
            )
      ) ~>
        sealRoute(services.workspaceRoutes) ~> check {
          assertResult(StatusCodes.OK, responseAs[String]) {
            status
          }
          workspaceQuery.findByName(testData.wsName)
        }

    runMultipleAndWait(10)(generator)
  }

  it should "clone a workspace if the source exists" in withTestDataApiServices { services =>
    val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy", Map.empty)
    Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, responseAs[String]) {
          status
        }

        withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
          val copiedWorkspace = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get
          assert(copiedWorkspace.attributes == testData.workspace.attributes)

          withWorkspaceContext(copiedWorkspace) { copiedWorkspaceContext =>
            // Name, namespace, creation date, and owner might change, so this is all that remains.
            assertResult(runAndWait(entityQuery.listActiveEntities(sourceWorkspaceContext)).toSet) {
              runAndWait(entityQuery.listActiveEntities(copiedWorkspaceContext)).toSet
            }
            assertResult(runAndWait(methodConfigurationQuery.listActive(sourceWorkspaceContext)).toSet) {
              runAndWait(methodConfigurationQuery.listActive(copiedWorkspaceContext)).toSet
            }
          }
        }

        // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
        assertResult(
          Some(Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(workspaceCopy.path))))
        ) {
          header("Location")
        }
      }
  }

  it should "clone a workspace and not try to copy over deleted method configs or hidden entities from the source" in withTestDataApiServices {
    services =>
      val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy", Map.empty)

      // contains no references in/out, safe to hide
      val entToDelete = testData.sample8
      val mcToDelete = testData.agoraMethodConfig.toShort
      withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
        assert {
          runAndWait(entityQuery.listActiveEntities(sourceWorkspaceContext)).toSeq.contains(entToDelete)
        }
        assert {
          runAndWait(methodConfigurationQuery.listActive(sourceWorkspaceContext)).contains(mcToDelete)
        }

        runAndWait(methodConfigurationQuery.delete(sourceWorkspaceContext, mcToDelete.namespace, mcToDelete.name))
        runAndWait(entityQuery.hide(sourceWorkspaceContext, Seq(entToDelete.toReference)))

        assert {
          !runAndWait(entityQuery.listActiveEntities(sourceWorkspaceContext)).toSeq.contains(entToDelete)
        }
        assert {
          !runAndWait(methodConfigurationQuery.listActive(sourceWorkspaceContext)).contains(mcToDelete)
        }
      }

      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }

          withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
            val copiedWorkspace = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get
            assert(copiedWorkspace.attributes == testData.workspace.attributes)

            withWorkspaceContext(copiedWorkspace) { copiedWorkspaceContext =>
              // Name, namespace, creation date, and owner might change, so this is all that remains.

              val srcEnts = runAndWait(entityQuery.listActiveEntities(sourceWorkspaceContext))
              val copiedEnts = runAndWait(entityQuery.listActiveEntities(copiedWorkspaceContext))
              assertSameElements(srcEnts, copiedEnts)

              val srcMCs = runAndWait(methodConfigurationQuery.listActive(sourceWorkspaceContext))
              val copiedMCs = runAndWait(methodConfigurationQuery.listActive(copiedWorkspaceContext))
              assertSameElements(srcMCs, copiedMCs)

              assert {
                !copiedEnts.toSeq.contains(entToDelete)
              }
              assert {
                !copiedMCs.contains(mcToDelete)
              }
            }
          }
        }
  }

  it should "return 403 on clone workspace when adding invalid-namespace attributes" in withTestDataApiServices {
    services =>
      val invalidAttrNamespace = "invalid"

      val workspaceCopy = WorkspaceRequest(
        namespace = testData.workspace.namespace,
        name = "test_copy",
        Map(AttributeName(invalidAttrNamespace, "attribute") -> AttributeString("foo"))
      )
      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }

          val errorText = responseAs[ErrorReport].message
          assert(errorText.contains(invalidAttrNamespace))
        }
  }

  it should "return 201 on clone workspace when adding library-namespace attributes" in withTestDataApiServices {
    services =>
      revokeCuratorRole(services)
      val newAttr = AttributeName(AttributeName.libraryNamespace, "attribute") -> AttributeString("foo")

      val workspaceCopy = WorkspaceRequest(
        namespace = testData.workspace.namespace,
        name = "test_copy",
        Map(newAttr)
      )
      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }

          withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
            val copiedWorkspace = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get
            assert(copiedWorkspace.attributes == testData.workspace.attributes + newAttr)

            withWorkspaceContext(copiedWorkspace) { copiedWorkspaceContext =>
              // Name, namespace, creation date, and owner might change, so this is all that remains.
              assertResult(runAndWait(entityQuery.listActiveEntities(sourceWorkspaceContext)).toSet) {
                runAndWait(entityQuery.listActiveEntities(copiedWorkspaceContext)).toSet
              }
              assertResult(runAndWait(methodConfigurationQuery.listActive(sourceWorkspaceContext)).toSet) {
                runAndWait(methodConfigurationQuery.listActive(copiedWorkspaceContext)).toSet
              }
            }
          }

          // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
          assertResult(
            Some(Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(workspaceCopy.path))))
          ) {
            header("Location")
          }
        }
  }

  it should "return 201 on clone workspace with existing library-namespace attributes" in withTestDataApiServices {
    services =>
      val updatedWorkspace =
        testData.workspace.copy(attributes =
          testData.workspace.attributes + (AttributeName(AttributeName.libraryNamespace,
                                                         "attribute"
          ) -> AttributeString("foo"))
        )
      runAndWait(workspaceQuery.createOrUpdate(updatedWorkspace))

      val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy", Map.empty)
      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }

          withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
            val copiedWorkspace = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get
            assert(copiedWorkspace.attributes == updatedWorkspace.attributes)

            withWorkspaceContext(copiedWorkspace) { copiedWorkspaceContext =>
              // Name, namespace, creation date, and owner might change, so this is all that remains.
              assertResult(runAndWait(entityQuery.listActiveEntities(sourceWorkspaceContext)).toSet) {
                runAndWait(entityQuery.listActiveEntities(copiedWorkspaceContext)).toSet
              }
              assertResult(runAndWait(methodConfigurationQuery.listActive(sourceWorkspaceContext)).toSet) {
                runAndWait(methodConfigurationQuery.listActive(copiedWorkspaceContext)).toSet
              }
            }
          }

          // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
          assertResult(
            Some(Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(workspaceCopy.path))))
          ) {
            header("Location")
          }
        }
  }

  it should "add attributes when cloning a workspace" in withTestDataApiServices { services =>
    val workspaceNoAttrs = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy", Map.empty)
    Post(s"${testData.workspace.path}/clone", httpJson(workspaceNoAttrs)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        assertResult(Option(testData.workspace.attributes)) {
          responseAs[WorkspaceDetails].attributes
        }
      }

    val newAtts = Map(
      AttributeName.withDefaultNS("number") -> AttributeNumber(11), // replaces an existing attribute
      AttributeName.withDefaultNS("another") -> AttributeNumber(12) // adds a new attribute
    )

    val workspaceCopyRealm = WorkspaceRequest(namespace = testData.workspace.namespace, name = "test_copy2", newAtts)
    withStatsD {
      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopyRealm)) ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
          assertResult(Option(testData.workspace.attributes ++ newAtts)) {
            responseAs[WorkspaceDetails].attributes
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted.clone"
      val expected = expectedHttpRequestMetrics("post", wsPathForRequestMetrics, StatusCodes.Created.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "clone a workspace with noWorkspaceOwner" in withTestDataApiServices { services =>
    val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace,
                                         name = "test_copy",
                                         Map.empty,
                                         noWorkspaceOwner = Option(true)
    )
    Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Created, responseAs[String]) {
          status
        }

        withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
          val copiedWorkspace = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get

          withWorkspaceContext(copiedWorkspace) { copiedWorkspaceContext =>
            // Name, namespace, creation date, and owner might change, so this is all that remains.
            assertResult(runAndWait(entityQuery.listActiveEntities(sourceWorkspaceContext)).toSet) {
              runAndWait(entityQuery.listActiveEntities(copiedWorkspaceContext)).toSet
            }
            assertResult(runAndWait(methodConfigurationQuery.listActive(sourceWorkspaceContext)).toSet) {
              runAndWait(methodConfigurationQuery.listActive(copiedWorkspaceContext)).toSet
            }
          }
        }

        // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
        assertResult(
          Some(Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(workspaceCopy.path))))
        ) {
          header("Location")
        }
      }
  }

  it should "have an empty owner policy when cloning a workspace with noWorkspaceOwner" in withTestDataApiServicesCustomizableMockSam {
    services =>
      val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace,
                                           name = "test_copy",
                                           Map.empty,
                                           noWorkspaceOwner = Option(true)
      )
      populateBillingProjectPolicies(services)
      val expectedOwnerPolicyEmail = ""

      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created, responseAs[String]) {
            status
          }

          assertResult(expectedOwnerPolicyEmail) {
            val ws = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get
            val policiesWithNameAndEmail = Await.result(
              services.samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, ws.workspaceId, testContext),
              Duration.Inf
            )
            val actualOwnerPolicyEmail =
              policiesWithNameAndEmail.find(_.policyName == SamWorkspacePolicyNames.owner).get.email.value
            actualOwnerPolicyEmail
          }
        }
  }

  it should "return 403 for clone workspace with noWorkspaceOwner without BP owner permissions" in withTestDataApiServicesMockitoSam {
    services =>
      val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace,
                                           name = "test_copy",
                                           Map.empty,
                                           noWorkspaceOwner = Option(true)
      )

      // User has read permissions on existing workspace
      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          ArgumentMatchers.eq(SamWorkspaceActions.read),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(true))

      when(
        services.samDAO.getResourceAuthDomain(
          ArgumentMatchers.eq(SamResourceTypeNames.workspace),
          ArgumentMatchers.eq(testData.workspace.workspaceId),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(Seq.empty))

      // User has BP user role, but not owner role
      when(
        services.samDAO.listUserRolesForResource(
          ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
          ArgumentMatchers.eq(testData.billingProject.projectName.value),
          any[RawlsRequestContext]
        )
      ).thenReturn(
        Future.successful(
          Set[SamResourceRole](SamBillingProjectRoles.workspaceCreator, SamBillingProjectRoles.batchComputeUser)
        )
      )

      // User has BP user permissions and therefore can create workspaces
      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
          ArgumentMatchers.eq(testData.billingProject.projectName.value),
          ArgumentMatchers.eq(SamBillingProjectActions.createWorkspace),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(true))
      when(
        services.samDAO.getUserStatus(any[RawlsRequestContext])
      ).thenReturn(
        Future.successful(
          Some(SamUserStatusResponse(userInfo.userSubjectId.value, userInfo.userEmail.value, enabled = true))
        )
      )

      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden, responseAs[String]) {
            status
          }

          val errorText = responseAs[ErrorReport].message
          assert(errorText.contains(workspaceCopy.namespace))
        }
  }

  it should "return 409 Conflict on clone if the destination already exists" in withTestDataApiServices { services =>
    val workspaceCopy =
      WorkspaceRequest(namespace = testData.workspace.namespace, name = testData.workspaceNoGroups.name, Map.empty)
    Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
        assertResult(None) {
          header("Location")
        }
      }
  }

  it should "clone workspace use different bucket location if bucketLocation is in request" in withApiServicesMockitoGcsDao {
    services =>
      val newBucketLocation = Option("us-terra1");
      val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace,
                                           name = "test_copy",
                                           Map.empty,
                                           bucketLocation = newBucketLocation
      )

      // mock(ito) out the workspace creation
      when(services.gcsDAO.testTerraBillingAccountAccess(any[RawlsBillingAccountName]))
        .thenReturn(Future.successful(true))
      doReturn(
        Future.successful(
          new ProjectBillingInfo()
            .setBillingAccountName(testData.workspace.currentBillingAccountOnGoogleProject.map(_.value).getOrElse(""))
            .setProjectId(testData.workspace.googleProjectId.value)
        ),
        null
      )
        .when(services.gcsDAO)
        .setBillingAccountName(ArgumentMatchers.eq(GoogleProjectId("project-from-buffer")),
                               any[RawlsBillingAccountName],
                               any[Span]
        )
      when(services.gcsDAO.getGoogleProject(any[GoogleProjectId]))
        .thenReturn(Future.successful(new Project().setProjectNumber(null)))
      when(services.gcsDAO.labelSafeMap(any[Map[String, String]], any[String])).thenReturn(Map.empty[String, String])
      when(services.gcsDAO.updateGoogleProject(any[GoogleProjectId], any[Project]))
        .thenReturn(Future.successful(new Project()))
      when(services.gcsDAO.removePolicyBindings(any[GoogleProjectId], any[Map[String, Set[String]]]))
        .thenReturn(Future.successful(true))
      when(services.gcsDAO.getGoogleProjectNumber(any[Project])).thenReturn(GoogleProjectNumber("GoogleProjectNumber"))

      val mockGcsDAO = new MockGoogleServicesDAO("test")
      when(services.gcsDAO.getServiceAccountUserInfo()).thenReturn(mockGcsDAO.getServiceAccountUserInfo())
      when(services.gcsDAO.getUserInfoUsingJson(any[String])).thenReturn(mockGcsDAO.getUserInfoUsingJson(""))

      when(
        services.gcsDAO.setupWorkspace(
          any[UserInfo],
          ArgumentMatchers.eq(GoogleProjectId("project-from-buffer")),
          any[Map[WorkspaceAccessLevel, WorkbenchEmail]],
          any[GcsBucketName],
          any[Map[String, String]],
          any[Span],
          ArgumentMatchers.eq(newBucketLocation)
        )
      )
        .thenReturn(Future.successful(mock[GoogleWorkspaceInfo]))
      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created, responseAs[String]) {
            status
          }

          withWorkspaceContext(testData.workspace) { sourceWorkspaceContext =>
            val copiedWorkspace = runAndWait(workspaceQuery.findByName(workspaceCopy.toWorkspaceName)).get
            assert(copiedWorkspace.attributes == testData.workspace.attributes)

            withWorkspaceContext(copiedWorkspace) { copiedWorkspaceContext =>
              // Name, namespace, creation date, and owner might change, so this is all that remains.
              assertResult(runAndWait(entityQuery.listActiveEntities(sourceWorkspaceContext)).toSet) {
                runAndWait(entityQuery.listActiveEntities(copiedWorkspaceContext)).toSet
              }
              assertResult(runAndWait(methodConfigurationQuery.listActive(sourceWorkspaceContext)).toSet) {
                runAndWait(methodConfigurationQuery.listActive(copiedWorkspaceContext)).toSet
              }
            }
            verify(services.gcsDAO).setupWorkspace(
              any[UserInfo],
              ArgumentMatchers.eq(GoogleProjectId("project-from-buffer")),
              any[Map[WorkspaceAccessLevel, WorkbenchEmail]],
              any[GcsBucketName],
              any[Map[String, String]],
              any[Span],
              ArgumentMatchers.eq(newBucketLocation)
            )
          }

          // TODO: does not test that the path we return is correct.  Update this test in the future if we care about that
          assertResult(
            Some(Location(Uri("http", Uri.Authority(Uri.Host("example.com")), Uri.Path(workspaceCopy.path))))
          ) {
            header("Location")
          }
        }
  }

  it should "return 201 if the cloned bucket location requested is the default bucket location" in withTestDataApiServices {
    services =>
      val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace,
                                           name = "test_copy",
                                           Map.empty,
                                           bucketLocation = Option("US")
      )
      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assert(status == StatusCodes.Created)
        }
  }

  it should "return 400 if the cloned bucket location requested is in an invalid format" in withTestDataApiServices {
    services =>
      val workspaceCopy = WorkspaceRequest(namespace = testData.workspace.namespace,
                                           name = "test_copy",
                                           Map.empty,
                                           bucketLocation = Option("EU")
      )
      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          val errorText = responseAs[ErrorReport].message
          assert(status == StatusCodes.BadRequest)
          assert(
            errorText.contains(
              "Workspace bucket location must be a single region of format: [A-Za-z]+-[A-Za-z]+[0-9]+ or the default bucket location ('US')."
            )
          )
        }
  }

  it should "return 200 when requesting an ACL from an existing workspace" in withTestDataApiServices { services =>
    Get(s"${testData.workspace.path}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
      }
  }

  it should "return 404 when requesting an ACL from a non-existent workspace" in withTestDataApiServices { services =>
    val nonExistent = WorkspaceName("xyzzy", "plugh")
    Get(s"${nonExistent.path}/acl") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound)(status)
      }
  }

  it should "return 200 when replacing an ACL for an existing workspace" in withTestDataApiServices { services =>
    Patch(s"${testData.workspace.path}/acl", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
      }
  }

  it should "modifying a workspace acl should modify the workspace last modified date" in withTestDataApiServices {
    services =>
      Patch(s"${testData.workspace.path}/acl", httpJsonEmpty) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK)(status)
        }
      Get(testData.workspace.path) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
        }
  }

  it should "return 404 when replacing an ACL on a non-existent workspace" in withTestDataApiServices { services =>
    val nonExistent = WorkspaceName("xyzzy", "plugh")
    Patch(s"${nonExistent.path}/acl", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound)(status)
      }
  }

  it should "get bucket options" in withTestDataApiServices { services =>
    Get(s"${testData.workspace.path}/bucketOptions") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        assertResult(WorkspaceBucketOptions(false)) {
          responseAs[WorkspaceBucketOptions]
        }
      }
  }

  // Begin tests where routes are restricted by ACLs

  // Get Workspace requires READ access.  Accept if OWNER, WRITE, READ; Reject if NO ACCESS

  it should "allow an project-owner-access user to get a workspace" in withTestWorkspacesApiServicesAndUser(
    testData.userProjectOwner.userEmail.value
  ) { services =>
    Get(testWorkspaces.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow an owner-access user to get a workspace" in withTestWorkspacesApiServicesAndUser(
    testData.userOwner.userEmail.value
  ) { services =>
    Get(testWorkspaces.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to get a workspace" in withTestWorkspacesApiServicesAndUser(
    testData.userWriter.userEmail.value
  ) { services =>
    Get(testWorkspaces.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a read-access user to get a workspace" in withTestWorkspacesApiServicesAndUser(
    testData.userReader.userEmail.value
  ) { services =>
    Get(testWorkspaces.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "not allow a no-access user to get a workspace" in withTestWorkspacesApiServicesAndUser("no-access") {
    services =>
      Get(testWorkspaces.workspace.path) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "not allow a no-access user to get a workspace by id" in withTestWorkspacesApiServicesAndUser("no-access") {
    services =>
      Get(s"/workspaces/id/${testWorkspaces.workspace.workspaceId}") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  // Update Workspace requires WRITE access.  Accept if OWNER or WRITE; Reject if READ or NO ACCESS

  it should "allow an project-owner-access user to update a workspace" in withTestDataApiServicesAndUser(
    testData.userProjectOwner.userEmail.value
  ) { services =>
    Patch(testData.workspace.path,
          httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "check that an update to a workspace modifies the last modified date" in withTestDataApiServicesAndUser(
    testData.userProjectOwner.userEmail.value
  ) { services =>
    var mutableWorkspace: Workspace = testData.workspace.copy()
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        mutableWorkspace = responseAs[WorkspaceResponse].workspace.toWorkspace
      }

    Patch(testData.workspace.path,
          httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        val updatedWorkspace: Workspace = responseAs[WorkspaceResponse].workspace.toWorkspace
        assertWorkspaceModifiedDate(status, updatedWorkspace)
        assert {
          updatedWorkspace.lastModified.isAfter(mutableWorkspace.lastModified)
        }
      }
  }

  it should "allow an owner-access user to update a workspace" in withTestDataApiServicesAndUser(
    testData.userOwner.userEmail.value
  ) { services =>
    Patch(testData.workspace.path,
          httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "allow a write-access user to update a workspace" in withTestDataApiServicesAndUser(
    testData.userWriter.userEmail.value
  ) { services =>
    Patch(testData.workspace.path,
          httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "not allow a read-access user to update a workspace" in withTestDataApiServicesAndUser(
    testData.userReader.userEmail.value
  ) { services =>
    Patch(testData.workspace.path,
          httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "not allow a no-access user to update a workspace" in withTestDataApiServicesAndUser("no-access") {
    services =>
      Patch(testData.workspace.path,
            httpJson(Seq(RemoveAttribute(AttributeName.withDefaultNS("boo")): AttributeUpdateOperation))
      ) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "not allow ACL updates with a member specified twice" in withTestDataApiServicesAndUser(
    testData.userOwner.userEmail.value
  ) { services =>
    Patch(
      s"${testData.workspace.path}/acl",
      httpJson(
        Seq(
          WorkspaceACLUpdate(testData.userProjectOwner.userEmail.value, WorkspaceAccessLevels.ProjectOwner, None),
          WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Read, None),
          WorkspaceACLUpdate(testData.userWriter.userEmail.value, WorkspaceAccessLevels.Owner, None)
        )
      )
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest)(status)
      }
  }

  it should "not allow an owner to grant compute permissions to reader" in withTestDataApiServicesAndUser(
    "owner-access"
  ) { services =>
    import WorkspaceACLJsonSupport._
    Patch(s"${testData.workspace.path}/acl",
          httpJson(
            Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Read, None, Option(true)))
          )
    ) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "allow an owner to grant compute permissions to writer" in withTestDataApiServicesAndUser("owner-access") {
    services =>
      // canCompute omitted defaults to true
      Patch(
        s"${testData.workspace.path}/acl",
        httpJson(Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, None, None)))
      ) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK)(status)
        }

      // canCompute explicitly set to false
      Patch(
        s"${testData.workspace.path}/acl",
        httpJson(
          Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, None, Option(false)))
        )
      ) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK)(status)
        }

      // canCompute explicitly set to true
      Patch(
        s"${testData.workspace.path}/acl",
        httpJson(
          Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Write, None, Option(true)))
        )
      ) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK)(status)
        }

      // canCompute explicitly set to true for owner has no effect
      Patch(
        s"${testData.workspace.path}/acl",
        httpJson(
          Seq(WorkspaceACLUpdate(testData.userReader.userEmail.value, WorkspaceAccessLevels.Owner, None, Option(false)))
        )
      ) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK)(status)
        }
  }

  it should "return 204 for a user with read access on an unlocked workspace" in withTestDataApiServicesAndUser(
    "reader-access"
  ) { services =>
    Get(s"${testData.workspace.path}/checkIamActionWithLock/read") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
  }

  it should "return 403 for a reader testing the write action on an unlocked workspace" in withTestDataApiServicesAndUser(
    "reader-access"
  ) { services =>
    Get(s"${testData.workspace.path}/checkIamActionWithLock/write") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 204 for a reader testing the read action on a locked workspace" in withTestDataApiServicesAndUser(
    "reader-access"
  ) { services =>
    Get(s"${testData.workspaceLocked.path}/checkIamActionWithLock/read") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
  }

  it should "return 204 for a writer testing the write action on an unlocked workspace" in withTestDataApiServicesAndUser(
    "writer-access"
  ) { services =>
    Get(s"${testData.workspace.path}/checkIamActionWithLock/write") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
  }

  it should "return 403 for a writer testing the write action on a locked workspace" in withTestDataApiServicesAndUser(
    "writer-access"
  ) { services =>
    Get(s"${testData.workspaceLocked.path}/checkIamActionWithLock/write") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  // End ACL-restriction Tests

  // Workspace Locking
  it should "allow an owner to lock (and re-lock) the workspace" in withEmptyWorkspaceApiServices(
    testData.userOwner.userEmail.value
  ) { services =>
    Put(s"${testData.workspace.path}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }
    Put(s"${testData.workspace.path}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }
  }

  it should "locking (and unlocking) a workspace should modify the workspace last modified date" in withEmptyWorkspaceApiServices(
    testData.userOwner.userEmail.value
  ) { services =>
    Put(s"${testData.workspace.path}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
      }

    Put(s"${testData.workspace.path}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }

    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceResponse].workspace.toWorkspace)
      }

  }

  it should "not allow anyone to write to a workspace when locked" in withLockedWorkspaceApiServices(
    testData.userWriter.userEmail.value
  ) { services =>
    Patch(testData.workspace.path, httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden)(status)
      }
  }

  it should "allow a reader to read a workspace, even when locked" in withLockedWorkspaceApiServices(
    testData.userReader.userEmail.value
  ) { services =>
    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
      }
  }

  it should "not allow an owner to lock a workspace with incomplete submissions" in withTestDataApiServicesAndUser(
    testData.userOwner.userEmail.value
  ) { services =>
    Put(s"${testData.workspace.path}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict)(status)
      }
  }

  it should "not allow an owner to delete a locked workspace" in withLockedWorkspaceApiServices(
    testData.userOwner.userEmail.value
  ) { services =>
    Delete(s"${testData.workspace.path}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden)(status)
      }
  }

  it should "allow an owner to unlock the workspace (repeatedly)" in withEmptyWorkspaceApiServices(
    testData.userOwner.userEmail.value
  ) { services =>
    Put(s"${testData.workspace.path}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }
    Put(s"${testData.workspace.path}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }
    Put(s"${testData.workspace.path}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }
  }

  it should "not allow a non-owner to lock or unlock the workspace" in withTestDataApiServicesAndUser(
    testData.userWriter.userEmail.value
  ) { services =>
    Put(s"${testData.workspace.path}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden)(status)
      }
    Put(s"${testData.workspace.path}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden)(status)
      }
  }

  it should "not allow a no-access user to infer the existence of the workspace by locking or unlocking" in withLockedWorkspaceApiServices(
    "no-access"
  ) { services =>
    Put(s"${testData.workspace.path}/lock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound)(status)
      }
    Put(s"${testData.workspace.path}/unlock") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound)(status)
      }
  }

  it should "return 403 creating workspace in billing project with no access" in withTestDataApiServicesMockitoSam {
    services =>
      val billingProjectName = testData.wsName.namespace.value
      when(
        services.samDAO.userHasAction(any[SamResourceTypeName],
                                      any[String],
                                      any[SamResourceAction],
                                      any[RawlsRequestContext]
        )
      )
        .thenReturn(Future.successful(true))
      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
          ArgumentMatchers.eq(billingProjectName),
          ArgumentMatchers.eq(SamBillingProjectActions.createWorkspace),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(false))

      val newWorkspace = WorkspaceRequest(
        namespace = billingProjectName,
        name = "newWorkspace",
        Map.empty
      )

      Post(s"/workspaces", httpJson(newWorkspace)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden, responseAs[String]) {
            status
          }
        }
  }

  it should "return 200 when a user can read a workspace bucket" in withTestDataApiServicesAndUser(
    testData.userReader.userEmail.value
  ) { services =>
    Get(s"${testData.workspace.path}/checkBucketReadAccess") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String])(status)
      }
  }

  it should "return 404 when a user can't read the bucket because they dont have workspace access" in withTestDataApiServicesAndUser(
    "no-access"
  ) { services =>
    Get(s"${testData.workspace.path}/checkBucketReadAccess") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound)(status)
      }
  }

  it should "return 404 when requesting bucket size for a non-existent workspace" in withTestWorkspacesApiServicesAndUser(
    "reader-access"
  ) { services =>
    Get(s"${testWorkspaces.workspace.copy(name = "DNE").path}/bucketUsage") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound)(status)
      }
  }

  it should "return 404 when a no-access user requests bucket usage" in withTestWorkspacesApiServicesAndUser(
    "no-access"
  ) { services =>
    Get(s"${testWorkspaces.workspace.path}/bucketUsage") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "not allow a reader-access user to request bucket usage" in withTestWorkspacesApiServicesAndUser(
    "reader-access"
  ) { services =>
    Get(s"${testWorkspaces.workspace.path}/bucketUsage") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  for (access <- Seq("owner-access", "writer-access"))
    it should s"return 200 when workspace with $access requests bucket usage for an existing workspace" in withTestWorkspacesApiServicesAndUser(
      access
    ) { services =>
      Get(s"${testWorkspaces.workspace.path}/bucketUsage") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(BucketUsageResponse(BigInt(42), Some(new DateTime(0)))) {
            responseAs[BucketUsageResponse]
          }
        }
    }

  it should "return the pending workspace file transfers for a recently cloned workspace" in withTestDataApiServices {
    services =>
      val clonedWorkspaceName = WorkspaceName(testData.workspace.namespace, "test_copy")
      val workspaceCopy = WorkspaceRequest(
        namespace = clonedWorkspaceName.namespace,
        name = clonedWorkspaceName.name,
        attributes = Map.empty,
        authorizationDomain = None,
        copyFilesWithPrefix = Some("/notebooks"),
        noWorkspaceOwner = None,
        bucketLocation = None
      )

      Post(s"${testData.workspace.path}/clone", httpJson(workspaceCopy)) ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      val clonedWorkspaceResult = runAndWait(workspaceQuery.findByName(clonedWorkspaceName)).get
      val expected = Seq(
        PendingCloneWorkspaceFileTransfer(
          clonedWorkspaceResult.workspaceIdAsUUID,
          testData.workspace.bucketName,
          clonedWorkspaceResult.bucketName,
          workspaceCopy.copyFilesWithPrefix.get,
          clonedWorkspaceResult.googleProjectId
        )
      )

      Get(s"${clonedWorkspaceName.path}/fileTransfers") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(expected) {
            responseAs[Seq[PendingCloneWorkspaceFileTransfer]]
          }
        }
  }

  it should "return no pending workspace file transfers for a workspace that already exists" in withTestDataApiServices {
    services =>
      Get(s"${testData.workspace.path}/fileTransfers") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(Seq.empty) {
            responseAs[Seq[PendingCloneWorkspaceFileTransfer]]
          }
        }
  }

  it should "require at least reader access on the workspace to query for active file transfers" in withTestWorkspacesApiServicesAndUser(
    "no-access"
  ) { services =>
    Get(s"${testWorkspaces.workspace.path}/fileTransfers") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "prevent user without compute permission from creating submission" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      testCreateSubmission(dataSource, userWriterNoCompute, StatusCodes.Forbidden)
  }

  it should "allow user with explicit compute permission to create submission" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      testCreateSubmission(dataSource, testData.userWriter.userEmail, StatusCodes.Created)
  }

  it should "allow user to create submission with workspace compute permission and no billing project compute permission" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      testCreateSubmission(dataSource, userWriterNoComputeOnProject, StatusCodes.Created)
  }

  private def testCreateSubmission(dataSource: SlickDataSource, userEmail: RawlsUserEmail, exectedStatus: StatusCode) =
    withApiServicesSecure(dataSource, userEmail.value) { services =>
      val wsName = testData.wsName
      val agoraMethodConf = MethodConfiguration("no_input",
                                                "dsde",
                                                Some("Sample"),
                                                None,
                                                Map.empty,
                                                Map.empty,
                                                AgoraMethod("dsde", "no_input", 1)
      )
      val dockstoreMethodConf =
        MethodConfiguration("no_input_dockstore",
                            "dsde",
                            Some("Sample"),
                            None,
                            Map.empty,
                            Map.empty,
                            DockstoreMethod("dockstore-no-input-path", "dockstore-no-input-version")
        )

      List(agoraMethodConf, dockstoreMethodConf).foreach(
        createSubmission(wsName, _, testData.sample1, None, services, exectedStatus)
      )
    }

  private def createSubmission(wsName: WorkspaceName,
                               methodConf: MethodConfiguration,
                               submissionEntity: Entity,
                               submissionExpression: Option[String],
                               services: TestApiService,
                               expectedStatus: StatusCode,
                               userComment: Option[String] = None
  ): Option[String] = {

    Get(s"${wsName.path}/methodconfigs/${methodConf.namespace}/${methodConf.name}") ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        if (status == StatusCodes.NotFound) {
          Post(s"${wsName.path}/methodconfigs", httpJson(methodConf)) ~>
            sealRoute(services.methodConfigRoutes) ~>
            check {
              assertResult(StatusCodes.Created, responseAs[String]) {
                status
              }
            }
        } else {
          assertResult(StatusCodes.OK) {
            status
          }
        }
      }

    val submissionRq = SubmissionRequest(
      methodConfigurationNamespace = methodConf.namespace,
      methodConfigurationName = methodConf.name,
      entityType = Option(submissionEntity.entityType),
      entityName = Option(submissionEntity.name),
      expression = submissionExpression,
      useCallCache = false,
      deleteIntermediateOutputFiles = false,
      workflowFailureMode = None,
      userComment = userComment
    )
    Post(s"${wsName.path}/submissions", httpJson(submissionRq)) ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(expectedStatus, responseAs[String]) {
          status
        }
        if (expectedStatus == StatusCodes.Created) {
          val response = responseAs[SubmissionReport]
          Option(response.submissionId)
        } else None
      }
  }

  it should "enable and disable RequesterPaysForLinkedServiceAccounts" in withTestDataApiServicesAndUser(
    testData.userWriter.userEmail.value
  ) { services =>
    Put(s"${testData.workspace.path}/enableRequesterPaysForLinkedServiceAccounts") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }

    Put(s"${testData.workspace.path}/disableRequesterPaysForLinkedServiceAccounts") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
      }
  }

  it should "fail when user with only read access to workspace tries to edit comments" in {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      // mock service for user that has owner access to workspace
      withApiServicesSecure(dataSource) { servicesForOwner =>
        // mock service for user that has only read access to the same workspace
        withApiServicesSecure(dataSource, testData.userReader.userEmail.value) { servicesForReader =>
          val wsName = testData.wsName
          val userComment = Option("Comment for submission by workspace owner")
          val agoraMethodConf = MethodConfiguration("no_input",
                                                    "dsde",
                                                    Some("Sample"),
                                                    None,
                                                    Map.empty,
                                                    Map.empty,
                                                    AgoraMethod("dsde", "no_input", 1)
          )

          // submission created by user with owner access
          val submissionId = createSubmission(wsName,
                                              agoraMethodConf,
                                              testData.sample1,
                                              None,
                                              servicesForOwner,
                                              StatusCodes.Created,
                                              userComment
          ).get

          // user with read access to workspace should be able to get submission details and read the user comment
          Get(s"${wsName.path}/submissions/$submissionId") ~>
            sealRoute(servicesForReader.submissionRoutes) ~>
            check {
              assertResult(StatusCodes.OK) {
                status
              }
              val response = responseAs[Submission]
              response.userComment shouldBe Option("Comment for submission by workspace owner")
            }

          // user with read access should not be able to update the submission comment
          Patch(
            s"${wsName.path}/submissions/$submissionId",
            JsObject(
              List("userComment" -> "user comment updated".toJson): _*
            )
          ) ~>
            sealRoute(servicesForReader.submissionRoutes) ~>
            check {
              val response = responseAs[String]
              status should be(StatusCodes.Forbidden)
              response should include("insufficient permissions to perform operation on myNamespace/myWorkspace")
            }
        }
      }
    }
  }
}
