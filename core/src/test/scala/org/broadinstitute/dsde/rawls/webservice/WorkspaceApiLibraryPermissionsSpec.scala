package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, TestData}
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.workbench.google.mock.MockGooglePubSubDAO
import spray.json.DefaultJsonProtocol._

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by davidan on 4/15/17.
  */
class WorkspaceApiLibraryPermissionsSpec extends ApiServiceSpec {

  // awkward, but we copy these next two here to avoid pulling in the LibraryPermissionsSupport trait
  val publishedFlag = AttributeName.withLibraryNS("published")
  val discoverableWSAttribute = AttributeName.withLibraryNS("discoverableByGroups")

  // ==========================================
  // set up fixture data for the tests
  // ==========================================
  case class LibraryPermissionUser(curator: Boolean,
                                   catalog: Boolean,
                                   canShare: Boolean,
                                   level: WorkspaceAccessLevel,
                                   rawlsUser: RawlsUser
  ) {}

  def makeEmail(curator: Boolean, catalog: Boolean, canShare: Boolean, level: WorkspaceAccessLevel) =
    s"$level-curator:$curator-catalog:$catalog-canShare:$canShare"

  def makeRawlsUser(level: WorkspaceAccessLevel,
                    curator: Boolean = false,
                    catalog: Boolean = false,
                    canShare: Boolean = false
  ) = {
    val email = makeEmail(curator, catalog, canShare, level)
    RawlsUser(UserInfo(RawlsUserEmail(email), OAuth2BearerToken("token"), 123, RawlsUserSubjectId(email)))
  }

  class LibraryPermissionTestData extends TestData {

    val users: Seq[LibraryPermissionUser] = for {
      curator <- Seq(true, false)
      catalog <- Seq(true, false)
      canShare <- Seq(true, false)
      level <- Seq(WorkspaceAccessLevels.Owner, WorkspaceAccessLevels.Write, WorkspaceAccessLevels.Read)
    } yield {
      val rawlsUser = makeRawlsUser(level, curator = curator, catalog = catalog, canShare = canShare)
      LibraryPermissionUser(curator, catalog, canShare, level, rawlsUser)
    }

    val wsUnpublishedId = UUID.randomUUID()

    val wsUnpublishedName = WorkspaceName("myNamespace", "unpublishedWorkspace")

    val unpublishedOwnerGroup = makeRawlsGroup(
      s"${wsUnpublishedName.name}-${wsUnpublishedName.name}-OWNER",
      users.filter(_.level == WorkspaceAccessLevels.Owner).map(_.rawlsUser: RawlsUserRef).toSet
    )
    val unpublishedWriterGroup = makeRawlsGroup(
      s"${wsUnpublishedName.name}-${wsUnpublishedName.name}-WRITER",
      users.filter(_.level == WorkspaceAccessLevels.Write).map(_.rawlsUser: RawlsUserRef).toSet
    )
    val unpublishedReaderGroup = makeRawlsGroup(
      s"${wsUnpublishedName.name}-${wsUnpublishedName.name}-READER",
      users.filter(_.level == WorkspaceAccessLevels.Read).map(_.rawlsUser: RawlsUserRef).toSet
    )

    val unpublishedWorkspace = Workspace(
      wsUnpublishedName.namespace,
      wsUnpublishedName.name,
      wsUnpublishedId.toString,
      "aBucket",
      Some("workflow-collection"),
      currentTime(),
      currentTime(),
      "testUser",
      Map.empty
    )

    def enableCurators(gcsDAO: GoogleServicesDAO) =
      users.filter(_.curator).map(u => gcsDAO.addLibraryCurator(u.rawlsUser.userEmail.value))

    override def save(): ReadWriteAction[Unit] = {
      import driver.api._

      DBIO.seq(
        workspaceQuery.createOrUpdate(unpublishedWorkspace)
      )
    }
  }

  val libraryPermissionTestData = new LibraryPermissionTestData()

  case class TestApiService(dataSource: SlickDataSource,
                            user: String,
                            gcsDAO: MockGoogleServicesDAO,
                            gpsDAO: MockGooglePubSubDAO
  )(implicit override val executionContext: ExecutionContext)
      extends ApiServices
      with MockUserInfoDirectivesWithUser

  def withApiServices[T](dataSource: SlickDataSource, libTest: LibraryPermissionTest)(
    testCode: TestApiService => T
  ): T = {
    val apiService = new TestApiService(dataSource,
                                        libTest.user.userEmail.value,
                                        new MockGoogleServicesDAO("test"),
                                        new MockGooglePubSubDAO
    ) {
      override val samDAO = new MockSamDAO(dataSource) {

        override def userHasAction(resourceTypeName: SamResourceTypeName,
                                   resourceId: String,
                                   action: SamResourceAction,
                                   cts: RawlsRequestContext
        ): Future[Boolean] = {
          val result = action match {
            case SamWorkspaceActions.catalog                                              => libTest.catalog
            case SamResourceAction(actionName) if actionName.startsWith("share_policy::") => libTest.canShare
            case SamWorkspaceActions.own   => libTest.accessLevel >= WorkspaceAccessLevels.Owner
            case SamWorkspaceActions.write => libTest.accessLevel >= WorkspaceAccessLevels.Write
            case SamWorkspaceActions.read  => libTest.accessLevel >= WorkspaceAccessLevels.Read
            case _                         => true
          }
          Future.successful(result)
        }
      }
    }

    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withLibraryPermissionTestDataApiServicesAndUser[T](
    libTest: LibraryPermissionTest
  )(testCode: TestApiService => T): T =
    withCustomTestDatabase(libraryPermissionTestData) { dataSource: SlickDataSource =>
      withApiServices(dataSource, libTest) { services =>
        Await.result(Future.sequence(libraryPermissionTestData.enableCurators(services.gcsDAO)), Duration.Inf)
        testCode(services)
      }
    }

  trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
    val user: String
    def requireUserInfo(span: Option[Span]): Directive1[UserInfo] =
      provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, RawlsUserSubjectId(user)))
  }

  // ==========================================
  // payloads to use during tests
  // ==========================================
  val updatePublishValue = AttributeBoolean(true)
  val updatePublish: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(publishedFlag, updatePublishValue))
  val updateDiscoverValue = AttributeValueList(Seq(AttributeString("one"), AttributeString("two")))
  val updateDiscover: Seq[AttributeUpdateOperation] = Seq(
    AddUpdateAttribute(discoverableWSAttribute, updateDiscoverValue)
  )
  val updateLibName = AttributeName.withLibraryNS("foo")
  val updateLibValue = AttributeString("bar")
  val updateLibAttr: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(updateLibName, updateLibValue))
  val updateLibName2 = AttributeName.withLibraryNS("foo2")
  val updateLibValue2 = AttributeNumber(2)
  val updateLibAttr2: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(updateLibName2, updateLibValue2))
  val updateTagName = AttributeName.withTagsNS()
  val updateTagValue = AttributeValueList(Seq(AttributeString("tag1"), AttributeString("tag2")))
  val updateTagAttr: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(updateTagName, updateTagValue))
  val updateWsName = AttributeName.withDefaultNS("workspaceAttr")
  val updateWsValue = AttributeString("arbitrary workspace value")
  val updateWsAttr: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(updateWsName, updateWsValue))

  // reusable test util - called by every test
  def testUpdateLibraryAttributes(payload: Seq[AttributeUpdateOperation],
                                  expectedStatusCode: StatusCode = StatusCodes.OK,
                                  ws: WorkspaceName = libraryPermissionTestData.wsUnpublishedName
  )(implicit services: TestApiService): Option[Workspace] =
    withStatsD {
      Patch(ws.path + "/library", httpJson(payload)) ~> services.sealedInstrumentedRoutes ~>
        check {
          expectedStatusCode match {
            case StatusCodes.OK =>
              assertResult(StatusCodes.OK) {
                status
              }
              Some(responseAs[WorkspaceDetails].toWorkspace)
            case x =>
              assertResult(expectedStatusCode) {
                status
              }
              None
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted.library"
      val expected = expectedHttpRequestMetrics("patch", wsPathForRequestMetrics, expectedStatusCode.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }

  case class LibraryPermissionTest(
    user: RawlsUser,
    accessLevel: WorkspaceAccessLevel,
    curator: Boolean = false,
    catalog: Boolean = false,
    canShare: Boolean = false,
    workspaceName: WorkspaceName = libraryPermissionTestData.wsUnpublishedName,
    publishedOnly: StatusCode = StatusCodes.Forbidden,
    discoverOnly: StatusCode = StatusCodes.Forbidden,
    publishedPlus: StatusCode = StatusCodes.BadRequest,
    discoverPlus: StatusCode = StatusCodes.Forbidden,
    multiLibrary: StatusCode = StatusCodes.Forbidden,
    libraryPlusTags: StatusCode = StatusCodes.BadRequest,
    libraryPlusWorkspace: StatusCode = StatusCodes.BadRequest,
    workspaceOnly: StatusCode = StatusCodes.BadRequest
  )

  // NB: re-publish case is handled in orchestration; we only need to check change-published here

  val tests = Seq(
    // when owner + curator, can do everything
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Owner, curator = true),
      WorkspaceAccessLevels.Owner,
      curator = true,
      publishedOnly = StatusCodes.OK,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),
    // owner but not curator: everything except change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Owner),
      WorkspaceAccessLevels.Owner,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),
    // owner + catalog: still can't change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Owner, catalog = true),
      WorkspaceAccessLevels.Owner,
      catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),
    // owner + grant: still can't change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Owner, canShare = true),
      WorkspaceAccessLevels.Owner,
      canShare = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),
    // owner + grant + catalog: still can't change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Owner, canShare = true, catalog = true),
      WorkspaceAccessLevels.Owner,
      canShare = true,
      catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),

    // writer + curator: can only edit library attrs
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Write, curator = true),
                          WorkspaceAccessLevels.Write,
                          curator = true,
                          multiLibrary = StatusCodes.OK
    ),
    // plain-old writer: still can only edit library attributes
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Write),
                          WorkspaceAccessLevels.Write,
                          multiLibrary = StatusCodes.OK
    ),
    // writer + catalog: everything except change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Write, catalog = true),
      WorkspaceAccessLevels.Write,
      catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),
    // writer + grant: everything except change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Write, canShare = true),
      WorkspaceAccessLevels.Write,
      canShare = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),
    // writer + grant + catalog: everything except change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Write, canShare = true, catalog = true),
      WorkspaceAccessLevels.Write,
      canShare = true,
      catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),

    // reader + curator: nothing
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, curator = true),
                          WorkspaceAccessLevels.Read,
                          curator = true
    ),
    // plain-old reader: nothing
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read), WorkspaceAccessLevels.Read),
    // reader + catalog: everything except change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Read, catalog = true),
      WorkspaceAccessLevels.Read,
      catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),
    // reader + grant: change discoverability (but not other attrs)
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, canShare = true),
                          WorkspaceAccessLevels.Read,
                          canShare = true,
                          discoverOnly = StatusCodes.OK
    ),
    // reader + grant + catalog: everything except change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Read, canShare = true, catalog = true),
      WorkspaceAccessLevels.Read,
      canShare = true,
      catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    ),
    // reader + curator + catalog: everything except change published
    LibraryPermissionTest(
      makeRawlsUser(WorkspaceAccessLevels.Read, curator = true, catalog = true),
      WorkspaceAccessLevels.Read,
      curator = true,
      catalog = true,
      publishedOnly = StatusCodes.OK,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK
    )
  )

  tests foreach { testcrit =>
    val user = testcrit.user
    val testws = testcrit.workspaceName

    val testNameSuffix = s"${user.userEmail.value}"

    behavior of s"Library attribute permissions for $testNameSuffix"

    it should s"return ${testcrit.publishedOnly.value} when changing published flag as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(
      testcrit
    ) { services =>
      val payload = updatePublish
      val ws = testUpdateLibraryAttributes(payload, testcrit.publishedOnly)(services)
      if (testcrit.publishedOnly == StatusCodes.OK) {
        assertResult(updatePublishValue) {
          ws.get.attributes(publishedFlag)
        }
      }
    }

    it should s"return ${testcrit.discoverOnly.value} when changing discoverability list as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(
      testcrit
    ) { services =>
      val payload = updateDiscover
      val ws = testUpdateLibraryAttributes(payload, testcrit.discoverOnly)(services)
      if (testcrit.discoverOnly == StatusCodes.OK) {
        assertResult(updateDiscoverValue) {
          ws.get.attributes(discoverableWSAttribute)
        }
      }
    }

    it should s"return ${testcrit.publishedPlus.value} when changing published flag with another attr as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(
      testcrit
    ) { services =>
      val payload = updatePublish ++ updateLibAttr
      testUpdateLibraryAttributes(payload, testcrit.publishedPlus)(services)
    }

    it should s"return ${testcrit.discoverPlus.value} when changing discoverability list with another attr as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(
      testcrit
    ) { services =>
      val payload = updateDiscover ++ updateLibAttr
      val ws = testUpdateLibraryAttributes(payload, testcrit.discoverPlus)(services)
      if (testcrit.discoverPlus == StatusCodes.OK) {
        assertResult(updateDiscoverValue) {
          ws.get.attributes(discoverableWSAttribute)
        }
        assertResult(updateLibValue) {
          ws.get.attributes(updateLibName)
        }
      }
    }
    it should s"return ${testcrit.multiLibrary.value} when changing multiple standard library attrs as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(
      testcrit
    ) { services =>
      // tweak to make sure no state is persisted and that we (on some tests) check for unique values
      val uniqueLibName = AttributeName.withLibraryNS(UUID.randomUUID().toString)
      val uniqueLibValue = AttributeString(UUID.randomUUID().toString)
      val uniqueLibAttr: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(uniqueLibName, uniqueLibValue))

      val payload = updateLibAttr ++ updateLibAttr2 ++ uniqueLibAttr
      val ws = testUpdateLibraryAttributes(payload, testcrit.multiLibrary)(services)
      if (testcrit.multiLibrary == StatusCodes.OK) {
        assertResult(updateLibValue) {
          ws.get.attributes(updateLibName)
        }
        assertResult(updateLibValue2) {
          ws.get.attributes(updateLibName2)
        }
        assertResult(uniqueLibValue) {
          ws.get.attributes(uniqueLibName)
        }
      }
    }

    it should s"return ${testcrit.libraryPlusTags.value} when changing library attrs and tags as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(
      testcrit
    ) { services =>
      val payload = updateLibAttr ++ updateTagAttr
      val ws = testUpdateLibraryAttributes(payload, testcrit.libraryPlusTags)(services)
    }

    it should s"return ${testcrit.libraryPlusWorkspace.value} when changing library attrs and workspace attrs as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(
      testcrit
    ) { services =>
      val payload = updateLibAttr ++ updateWsAttr
      val ws = testUpdateLibraryAttributes(payload, testcrit.libraryPlusWorkspace)(services)
    }

    it should s"return ${testcrit.workspaceOnly.value} when changing workspace attrs as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(
      testcrit
    ) { services =>
      val payload = updateWsAttr
      val ws = testUpdateLibraryAttributes(payload, testcrit.workspaceOnly)(services)
    }

  }

}
