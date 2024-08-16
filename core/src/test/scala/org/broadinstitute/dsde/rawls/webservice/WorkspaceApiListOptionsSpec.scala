package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestData
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import scala.concurrent.ExecutionContext

class WorkspaceApiListOptionsSpec extends ApiServiceSpec {
  import driver.api._

  trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
    val user: String
    def requireUserInfo(otelContext: Option[Context]): Directive1[UserInfo] =
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

  def withApiServices[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(
    testCode: TestApiService => T
  ): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  class TestWorkspaces() extends TestData {
    val userOwner = RawlsUser(
      UserInfo(testData.userOwner.userEmail,
               OAuth2BearerToken("token"),
               123,
               RawlsUserSubjectId("123456789876543212345")
      )
    )

    val billingProject = RawlsBillingProject(RawlsBillingProjectName("ns"), CreationStatuses.Ready, None, None)

    val workspaceName = WorkspaceName(billingProject.projectName.value, "testworkspace")

    val workspace2Name = WorkspaceName(billingProject.projectName.value, "testworkspace2")

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
      Map(AttributeName.withDefaultNS("a") -> AttributeString("x"),
          AttributeName.withDefaultNS("description") -> AttributeString("workspace one")
      ),
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
      Map(AttributeName.withDefaultNS("b") -> AttributeString("y"),
          AttributeName.withDefaultNS("description") -> AttributeString("workspace two")
      ),
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
      DBIO
        .seq(
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
        .withPinnedSession
  }

  val testWorkspaces = new TestWorkspaces

  def withTestWorkspacesApiServices[T](testCode: TestApiService => T): T =
    withCustomTestDatabase(testWorkspaces) { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  "WorkspaceApi list-workspaces with fields param" should "return full response if no fields param" in withTestWorkspacesApiServices {
    services =>
      Get("/workspaces") ~>
        sealRoute(services.workspaceRoutes()) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }

          println(responseAs[String])

          val dateTime = currentTime()
          assertResult(
            Set(
              WorkspaceListResponse(
                WorkspaceAccessLevels.Owner,
                Some(true),
                Some(true),
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
                Some(true),
                Some(true),
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

  it should "return full response if querystring exists but no fields param" in withTestWorkspacesApiServices {
    services =>
      Get("/workspaces?thisisnotfields=noitsnot") ~>
        sealRoute(services.workspaceRoutes()) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }

          val dateTime = currentTime()
          assertResult(
            Set(
              WorkspaceListResponse(
                WorkspaceAccessLevels.Owner,
                Some(true),
                Some(true),
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
                Some(true),
                Some(true),
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

  it should "filter response to a single key" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=accessLevel") ~>
      sealRoute(services.workspaceRoutes()) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject("accessLevel" -> JsString(WorkspaceAccessLevels.Owner.toString)),
          JsObject("accessLevel" -> JsString(WorkspaceAccessLevels.Owner.toString))
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "filter response to multiple keys" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=accessLevel,public") ~>
      sealRoute(services.workspaceRoutes()) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject(
            "accessLevel" -> JsString(WorkspaceAccessLevels.Owner.toString),
            "public" -> JsBoolean(false)
          ),
          JsObject(
            "accessLevel" -> JsString(WorkspaceAccessLevels.Owner.toString),
            "public" -> JsBoolean(false)
          )
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "filter response to nested keys" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=workspaceSubmissionStats,workspace.workspaceId,workspace.bucketName") ~>
      sealRoute(services.workspaceRoutes()) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject(
            "workspace" -> JsObject(
              "workspaceId" -> JsString(testWorkspaces.workspace1Id.toString),
              "bucketName" -> JsString("bucket1")
            ),
            "workspaceSubmissionStats" -> JsObject(
              "lastSuccessDate" -> JsString(testDate.toString),
              "lastFailureDate" -> JsString(testDate.toString),
              "runningSubmissionsCount" -> JsNumber(2)
            )
          ),
          JsObject(
            "workspace" -> JsObject(
              "workspaceId" -> JsString(testWorkspaces.workspace2Id.toString),
              "bucketName" -> JsString("bucket2")
            ),
            "workspaceSubmissionStats" -> JsObject(
              "runningSubmissionsCount" -> JsNumber(0)
            )
          )
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "filter response to entire subtrees" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=public,workspace.attributes") ~>
      sealRoute(services.workspaceRoutes()) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject(
            "public" -> JsBoolean(false),
            "workspace" -> JsObject(
              "attributes" -> JsObject(
                "a" -> JsString("x"),
                "description" -> JsString("workspace one")
              )
            )
          ),
          JsObject(
            "public" -> JsBoolean(false),
            "workspace" -> JsObject(
              "attributes" -> JsObject(
                "b" -> JsString("y"),
                "description" -> JsString("workspace two")
              )
            )
          )
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "filter response to individual attributes" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=public,workspace.attributes.description") ~>
      sealRoute(services.workspaceRoutes()) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        val actual = responseAs[String].parseJson
        val expected = JsArray(
          JsObject(
            "public" -> JsBoolean(false),
            "workspace" -> JsObject(
              "attributes" -> JsObject(
                "description" -> JsString("workspace one")
              )
            )
          ),
          JsObject(
            "public" -> JsBoolean(false),
            "workspace" -> JsObject(
              "attributes" -> JsObject(
                "description" -> JsString("workspace two")
              )
            )
          )
        )
        orderInsensitiveCompare(expected, actual)
      }
  }

  it should "throw error with unrecognized field value" in withTestWorkspacesApiServices { services =>
    // NB: "workspaceType" is valid for get-workspace but not list-workspaces.
    Get("/workspaces?fields=accessLevel,workspaceType,somethingNotRecognized") ~>
      sealRoute(services.workspaceRoutes()) ~>
      check {
        assertResult(StatusCodes.BadRequest)(status)
        val actual = responseAs[ErrorReport]

        // NB: field names in error response are alphabetized for deterministic behavior
        assertResult("Unrecognized field names: somethingNotRecognized, workspaceType")(actual.message)
      }
  }

  it should "throw error if field param specified multiple times" in withTestWorkspacesApiServices { services =>
    Get("/workspaces?fields=accessLevel&fields=public") ~>
      sealRoute(services.workspaceRoutes()) ~>
      check {
        assertResult(StatusCodes.BadRequest)(status)
        val actual = responseAs[ErrorReport]

        assertResult("Parameter 'fields' may not be present multiple times.")(actual.message)
      }
  }

  /** Workspaces are returned in database row order. This order is deterministic for repeated runs against
    * the same static data in the DB ... but repeated runs of this spec each re-insert data to the DB,
    * which result in different row orders. We cannot predict row order ahead of time - we'd need to add a
    * SORT BY clause to the SELECT query, which would change the order at runtime. So, in this spec
    * we'll compare arrays out of order.
    *
    * WorkspaceApiSpec and elsewhere handle this by calling .toSet on results; that's another option.
    * "should contain theSameElementsAs" gives us slightly better error messaging.
    *
    * @param expected
    * @param actual
    */
  def orderInsensitiveCompare(expected: JsArray, actual: Any): Unit =
    actual match {
      case jsa: JsArray =>
        jsa.elements should contain theSameElementsAs (expected.elements)
      case x =>
        fail(s"actual value is of type ${x.getClass.getName}; expected JsArray.")
    }

}
