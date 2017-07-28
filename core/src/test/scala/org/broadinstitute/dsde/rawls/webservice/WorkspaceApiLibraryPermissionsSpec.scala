package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, TestData}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import spray.http.{OAuth2BearerToken, StatusCode, StatusCodes}
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive1

import scala.concurrent.ExecutionContext

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
  case class LibraryPermissionUser(
      curator: Boolean,
      catalog: Boolean,
      canShare: Boolean,
      level: WorkspaceAccessLevel,
      rawlsUser: RawlsUser) {
  }

  def makeEmail(curator:Boolean, catalog:Boolean, canShare:Boolean, level: WorkspaceAccessLevel) =
    s"$level-curator:$curator-catalog:$catalog-canShare:$canShare"

  def makeRawlsUser(level: WorkspaceAccessLevel, curator:Boolean=false, catalog:Boolean=false, canShare:Boolean=false) = {
    val email = makeEmail(curator, catalog, canShare, level)
    RawlsUser(UserInfo(RawlsUserEmail(email), OAuth2BearerToken("token"), 123, RawlsUserSubjectId(email)))
  }

  class LibraryPermissionTestData extends TestData {

    val users:Seq[LibraryPermissionUser] = for {
      curator <- Seq(true,false)
      catalog <- Seq(true,false)
      canShare <- Seq(true,false)
      level <- Seq(WorkspaceAccessLevels.Owner, WorkspaceAccessLevels.Write, WorkspaceAccessLevels.Read)
    } yield {
      val rawlsUser = makeRawlsUser(level, curator=curator, catalog=catalog, canShare=canShare)
      LibraryPermissionUser(curator, catalog, canShare, level, rawlsUser)
    }

    val wsUnpublishedId = UUID.randomUUID()

    val wsUnpublishedName = WorkspaceName("myNamespace", "unpublishedWorkspace")

    val unpublishedOwnerGroup = makeRawlsGroup(s"${wsUnpublishedName} OWNER", users.filter(_.level == WorkspaceAccessLevels.Owner).map(_.rawlsUser:RawlsUserRef).toSet)
    val unpublishedWriterGroup = makeRawlsGroup(s"${wsUnpublishedName} WRITER", users.filter(_.level == WorkspaceAccessLevels.Write).map(_.rawlsUser:RawlsUserRef).toSet)
    val unpublishedReaderGroup = makeRawlsGroup(s"${wsUnpublishedName} READER", users.filter(_.level == WorkspaceAccessLevels.Read).map(_.rawlsUser:RawlsUserRef).toSet)

    val unpublishedWorkspace = Workspace(wsUnpublishedName.namespace, wsUnpublishedName.name, Set.empty, wsUnpublishedId.toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty,
      Map(WorkspaceAccessLevels.Owner -> unpublishedOwnerGroup, WorkspaceAccessLevels.Write -> unpublishedWriterGroup, WorkspaceAccessLevels.Read -> unpublishedReaderGroup),
      Map(WorkspaceAccessLevels.Owner -> unpublishedOwnerGroup, WorkspaceAccessLevels.Write -> unpublishedWriterGroup, WorkspaceAccessLevels.Read -> unpublishedReaderGroup))

    def enableCurators(gcsDAO: GoogleServicesDAO ) = {
      users.filter(_.curator).map(u=> gcsDAO.addLibraryCurator(u.rawlsUser.userEmail.value))
    }

    override def save(): ReadWriteAction[Unit] = {
      import driver.api._

      DBIO.seq(
        DBIO.sequence(users.map(u => rawlsUserQuery.save(u.rawlsUser)).toSeq),
        rawlsGroupQuery.save(unpublishedOwnerGroup),
        rawlsGroupQuery.save(unpublishedWriterGroup),
        rawlsGroupQuery.save(unpublishedReaderGroup),
        workspaceQuery.save(unpublishedWorkspace),
        workspaceQuery.insertUserCatalogPermissions(wsUnpublishedId, users.filter(_.catalog).map(_.rawlsUser:RawlsUserRef)),
        workspaceQuery.insertUserSharePermissions(wsUnpublishedId, users.filter(_.canShare).map(_.rawlsUser:RawlsUserRef))
      )
    }
  }

  val libraryPermissionTestData = new LibraryPermissionTestData()

  case class TestApiService(dataSource: SlickDataSource, user: String, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectivesWithUser

  def withApiServices[T](dataSource: SlickDataSource, user: String)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withLibraryPermissionTestDataApiServicesAndUser[T](user: String)(testCode: TestApiService => T): T = {
    withCustomTestDatabase(libraryPermissionTestData) { dataSource: SlickDataSource =>
      withApiServices(dataSource, user) { services =>
        libraryPermissionTestData.enableCurators(services.gcsDAO)
        testCode(services)
      }
    }
  }

  trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
    val user: String
    def requireUserInfo(): Directive1[UserInfo] = {
      provide(UserInfo(RawlsUserEmail(user), OAuth2BearerToken("token"), 123, RawlsUserSubjectId(user)))
    }
  }

  // ==========================================
  // payloads to use during tests
  // ==========================================
  val updatePublishValue = AttributeBoolean(true)
  val updatePublish: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(publishedFlag, updatePublishValue))
  val updateDiscoverValue = AttributeValueList(Seq(AttributeString("one"),AttributeString("two")))
  val updateDiscover: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(discoverableWSAttribute, updateDiscoverValue))
  val updateLibName = AttributeName.withLibraryNS("foo")
  val updateLibValue = AttributeString("bar")
  val updateLibAttr: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(updateLibName, updateLibValue))
  val updateLibName2 = AttributeName.withLibraryNS("foo2")
  val updateLibValue2 = AttributeNumber(2)
  val updateLibAttr2: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(updateLibName2, updateLibValue2))
  val updateTagName = AttributeName.withTagsNS()
  val updateTagValue = AttributeValueList(Seq(AttributeString("tag1"),AttributeString("tag2")))
  val updateTagAttr: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(updateTagName, updateTagValue))
  val updateWsName = AttributeName.withDefaultNS("workspaceAttr")
  val updateWsValue = AttributeString("arbitrary workspace value")
  val updateWsAttr: Seq[AttributeUpdateOperation] = Seq(AddUpdateAttribute(updateWsName, updateWsValue))

  // reusable test util - called by every test
  def testUpdateLibraryAttributes(payload: Seq[AttributeUpdateOperation], expectedStatusCode: StatusCode = StatusCodes.OK, ws: WorkspaceName = libraryPermissionTestData.wsUnpublishedName)(implicit services: TestApiService): Option[Workspace] = {
    withStatsD {
      Patch(ws.path + "/library", httpJson(payload)) ~> services.sealedInstrumentedRoutes ~>
        check {
          expectedStatusCode match {
            case StatusCodes.OK =>
              assertResult(StatusCodes.OK) {
                status
              }
              Some(responseAs[Workspace])
            case x =>
              assertResult(expectedStatusCode) {
                status
              }
              None
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.${ws.namespace}.${ws.name}.library"
      val expected = expectedHttpRequestMetrics("patch", wsPathForRequestMetrics, expectedStatusCode.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  case class LibraryPermissionTest(
                user: RawlsUser,
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
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Owner, curator=true),
      curator=true,
      publishedOnly = StatusCodes.OK,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),
    // owner but not curator: everything except change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Owner),
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),
    // owner + catalog: still can't change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Owner, catalog=true),
      catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),
    // owner + grant: still can't change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Owner, canShare=true),
      canShare = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),
    // owner + grant + catalog: still can't change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Owner, canShare=true, catalog=true),
      canShare = true, catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),

    // writer + curator: can only edit library attrs
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Write, curator=true),
      curator=true,
      multiLibrary = StatusCodes.OK),
    // plain-old writer: still can only edit library attributes
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Write),
      multiLibrary = StatusCodes.OK),
    // writer + catalog: everything except change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Write, catalog=true),
      catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),
    // writer + grant: everything except change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Write, canShare=true),
      canShare=true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),
    // writer + grant + catalog: everything except change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Write, canShare=true, catalog=true),
      canShare = true, catalog = true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),

    // reader + curator: nothing
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, curator=true),
      curator=true),
    // plain-old reader: nothing
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read)),
    // reader + catalog: everything except change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read,catalog=true),
      catalog=true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),
    // reader + grant: change discoverability (but not other attrs)
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, canShare=true),
      canShare=true,
      discoverOnly = StatusCodes.OK),
    // reader + grant + catalog: everything except change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, canShare=true, catalog=true),
      canShare=true, catalog=true,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK),
    // reader + curator + catalog: everything except change published
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, curator=true, catalog=true),
      curator=true, catalog=true,
      publishedOnly = StatusCodes.OK,
      discoverOnly = StatusCodes.OK,
      discoverPlus = StatusCodes.OK,
      multiLibrary = StatusCodes.OK)
  )


  tests foreach { testcrit =>
    val user = testcrit.user
    val testws = testcrit.workspaceName

    val testNameSuffix = s"${user.userEmail.value}"

    behavior of s"Library attribute permissions for $testNameSuffix"

    it should s"return ${testcrit.publishedOnly.value} when changing published flag as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updatePublish
      val ws = testUpdateLibraryAttributes(payload, testcrit.publishedOnly)(services)
      if (testcrit.publishedOnly == StatusCodes.OK) {
        assertResult(updatePublishValue) {
          ws.get.attributes(publishedFlag)
        }
      }
    }

    it should s"return ${testcrit.discoverOnly.value} when changing discoverability list as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateDiscover
      val ws = testUpdateLibraryAttributes(payload, testcrit.discoverOnly)(services)
      if (testcrit.discoverOnly == StatusCodes.OK) {
        assertResult(updateDiscoverValue) {
          ws.get.attributes(discoverableWSAttribute)
        }
      }
    }

    it should s"return ${testcrit.publishedPlus.value} when changing published flag with another attr as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updatePublish ++ updateLibAttr
      testUpdateLibraryAttributes(payload, testcrit.publishedPlus)(services)
    }

    it should s"return ${testcrit.discoverPlus.value} when changing discoverability list with another attr as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
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
    it should s"return ${testcrit.multiLibrary.value} when changing multiple standard library attrs as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
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

    it should s"return ${testcrit.libraryPlusTags.value} when changing library attrs and tags as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateLibAttr ++ updateTagAttr
      val ws = testUpdateLibraryAttributes(payload, testcrit.libraryPlusTags)(services)
    }

    it should s"return ${testcrit.libraryPlusWorkspace.value} when changing library attrs and workspace attrs as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateLibAttr ++ updateWsAttr
      val ws = testUpdateLibraryAttributes(payload, testcrit.libraryPlusWorkspace)(services)
    }

    it should s"return ${testcrit.workspaceOnly.value} when changing workspace attrs as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateWsAttr
      val ws = testUpdateLibraryAttributes(payload, testcrit.workspaceOnly)(services)
    }

  }



}
