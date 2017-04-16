package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, TestData, WorkspaceAttributeRecord}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.http.{OAuth2BearerToken, StatusCode, StatusCodes}
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive1

import scala.concurrent.ExecutionContext

/**
  * Created by davidan on 4/15/17.
  */
class WorkspaceApiLibraryPermissionsSpec extends ApiServiceSpec {

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
    RawlsUser(UserInfo(email, OAuth2BearerToken("token"), 123, email))
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
    val wsPublishedId = UUID.randomUUID()

    val wsUnpublishedName = WorkspaceName("myNamespace", "unpublishedWorkspace")
    val wsPublishedName = WorkspaceName("myNamespace", "publishedWorkspace")

    val unpublishedOwnerGroup = makeRawlsGroup(s"${wsUnpublishedName} OWNER", users.filter(_.level == WorkspaceAccessLevels.Owner).map(_.rawlsUser:RawlsUserRef).toSet)
    val unpublishedWriterGroup = makeRawlsGroup(s"${wsUnpublishedName} WRITER", users.filter(_.level == WorkspaceAccessLevels.Write).map(_.rawlsUser:RawlsUserRef).toSet)
    val unpublishedReaderGroup = makeRawlsGroup(s"${wsUnpublishedName} READER", users.filter(_.level == WorkspaceAccessLevels.Read).map(_.rawlsUser:RawlsUserRef).toSet)

    val unpublishedWorkspace = Workspace(wsUnpublishedName.namespace, wsUnpublishedName.name, None, wsUnpublishedId.toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty,
      Map(WorkspaceAccessLevels.Owner -> unpublishedOwnerGroup, WorkspaceAccessLevels.Write -> unpublishedWriterGroup, WorkspaceAccessLevels.Read -> unpublishedReaderGroup),
      Map(WorkspaceAccessLevels.Owner -> unpublishedOwnerGroup, WorkspaceAccessLevels.Write -> unpublishedWriterGroup, WorkspaceAccessLevels.Read -> unpublishedReaderGroup))

    val publishedOwnerGroup = makeRawlsGroup(s"${wsPublishedName} OWNER", users.filter(_.level == WorkspaceAccessLevels.Owner).map(_.rawlsUser:RawlsUserRef).toSet)
    val publishedWriterGroup = makeRawlsGroup(s"${wsPublishedName} WRITER", users.filter(_.level == WorkspaceAccessLevels.Write).map(_.rawlsUser:RawlsUserRef).toSet)
    val publishedReaderGroup = makeRawlsGroup(s"${wsPublishedName} READER", users.filter(_.level == WorkspaceAccessLevels.Read).map(_.rawlsUser:RawlsUserRef).toSet)

    val publishedWorkspace = Workspace(wsPublishedName.namespace, wsPublishedName.name, None, wsPublishedId.toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty,
      Map(WorkspaceAccessLevels.Owner -> publishedOwnerGroup, WorkspaceAccessLevels.Write -> publishedWriterGroup, WorkspaceAccessLevels.Read -> publishedReaderGroup),
      Map(WorkspaceAccessLevels.Owner -> publishedOwnerGroup, WorkspaceAccessLevels.Write -> publishedWriterGroup, WorkspaceAccessLevels.Read -> publishedReaderGroup))

    def enableCurators(gcsDAO: GoogleServicesDAO ) = {
      users.filter(_.curator).map(u=> gcsDAO.addLibraryCurator(u.rawlsUser.userEmail.value))
    }

    override def save(): ReadWriteAction[Unit] = {
      import driver.api._

      DBIO.seq(
        DBIO.sequence(users.map( u => rawlsUserQuery.save(u.rawlsUser) ).toSeq),

        rawlsGroupQuery.save(unpublishedOwnerGroup),
        rawlsGroupQuery.save(unpublishedWriterGroup),
        rawlsGroupQuery.save(unpublishedReaderGroup),
        workspaceQuery.save(unpublishedWorkspace),
        workspaceQuery.insertUserCatalogPermissions(wsUnpublishedId, users.filter(_.catalog).map(_.rawlsUser:RawlsUserRef)),
        workspaceQuery.insertUserSharePermissions(wsUnpublishedId, users.filter(_.canShare).map(_.rawlsUser:RawlsUserRef)),

        rawlsGroupQuery.save(publishedOwnerGroup),
        rawlsGroupQuery.save(publishedWriterGroup),
        rawlsGroupQuery.save(publishedReaderGroup),
        workspaceQuery.save(publishedWorkspace),
        workspaceAttributeQuery.batchInsertAttributes(Seq(WorkspaceAttributeRecord(-1,wsPublishedId,"library","published",None,None,Some(true),None,None,None,None,false))),
        workspaceQuery.insertUserCatalogPermissions(wsPublishedId, users.filter(_.catalog).map(_.rawlsUser:RawlsUserRef)),
        workspaceQuery.insertUserSharePermissions(wsPublishedId, users.filter(_.canShare).map(_.rawlsUser:RawlsUserRef))
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
    def requireUserInfo(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo] = {
      provide(UserInfo(user, OAuth2BearerToken("token"), 123, user))
    }
  }

  // ====================== Library permissions tests
  // awkward, but we copy these next two here to avoid pulling in extraneous traits
  val publishedFlag = AttributeName.withLibraryNS("published")
  val discoverableWSAttribute = AttributeName.withLibraryNS("discoverableByGroups")

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

  def testUpdateLibraryAttributes(testcrit: LibraryPermissionTest, payload: Seq[AttributeUpdateOperation], expectedStatusCode: StatusCode = StatusCodes.OK)(implicit services: TestApiService): Option[Workspace] = {
    Patch(testcrit.workspaceName.path + "/library", httpJson(payload)) ~>
      sealRoute(services.workspaceRoutes) ~>
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
  }

  case class LibraryPermissionTest(
                user: RawlsUser,
                curator: Boolean = false,
                catalog: Boolean = false,
                canShare: Boolean = false,
                workspaceName: WorkspaceName = libraryPermissionTestData.wsUnpublishedName,
                publishedOnly: StatusCode = StatusCodes.OK,
                discoverOnly: StatusCode = StatusCodes.OK,
                publishedPlus: StatusCode = StatusCodes.BadRequest,
                discoverPlus: StatusCode = StatusCodes.OK,
                multiLibrary: StatusCode = StatusCodes.OK,
                libraryPlusTags: StatusCode = StatusCodes.Forbidden,
                libraryPlusWorkspace: StatusCode = StatusCodes.Forbidden,
                workspaceOnly: StatusCode = StatusCodes.Forbidden
              )

  // TODO: additional test cases for more combinations of level/curator/catalog/canShare
  // TODO: test republish (existing tests only check change-publish)
  // TODO: why do certain APIs return 403, when they should return 400?

  val tests = Seq(
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Owner, curator=true),
      curator=true),
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Owner),
      publishedOnly = StatusCodes.Forbidden),
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Write, curator=true),
      curator=true,
      publishedOnly = StatusCodes.Forbidden,
      discoverOnly = StatusCodes.Forbidden,
      discoverPlus = StatusCodes.Forbidden),
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Write),
      publishedOnly = StatusCodes.Forbidden,
      discoverOnly = StatusCodes.Forbidden,
      discoverPlus = StatusCodes.Forbidden),
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, curator=true),
      curator=true,
      publishedOnly = StatusCodes.Forbidden,
      discoverOnly = StatusCodes.Forbidden,
      discoverPlus = StatusCodes.Forbidden,
      multiLibrary = StatusCodes.Forbidden),
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, curator=true, catalog=true),
      curator=true, catalog=true),
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, catalog=true),
      catalog=true,
      publishedOnly = StatusCodes.Forbidden),
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read, canShare=true),
      canShare=true,
      publishedOnly = StatusCodes.Forbidden,
      discoverPlus = StatusCodes.Forbidden,
      multiLibrary = StatusCodes.Forbidden),
    LibraryPermissionTest(makeRawlsUser(WorkspaceAccessLevels.Read),
      publishedOnly = StatusCodes.Forbidden,
      discoverOnly = StatusCodes.Forbidden,
      discoverPlus = StatusCodes.Forbidden,
      multiLibrary = StatusCodes.Forbidden)
  )


  tests foreach { testcrit =>
    val user = testcrit.user
    val testws = testcrit.workspaceName

    val testNameSuffix = s"${user.userEmail.value}"

    behavior of s"Library attribute permissions for $testNameSuffix"

    it should s"return ${testcrit.publishedOnly.value} when updating published flag as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updatePublish
      val ws = testUpdateLibraryAttributes(testcrit, payload, testcrit.publishedOnly)(services)
      if (testcrit.publishedOnly == StatusCodes.OK) {
        assertResult(updatePublishValue) {
          ws.get.attributes(publishedFlag)
        }
      }
    }

    it should s"return ${testcrit.discoverOnly.value} when updating discoverability list as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateDiscover
      val ws = testUpdateLibraryAttributes(testcrit, payload, testcrit.discoverOnly)(services)
      if (testcrit.discoverOnly == StatusCodes.OK) {
        assertResult(updateDiscoverValue) {
          ws.get.attributes(discoverableWSAttribute)
        }
      }
    }

    it should s"return ${testcrit.publishedPlus.value} when updating published flag with another attr as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updatePublish ++ updateLibAttr
      testUpdateLibraryAttributes(testcrit, payload, testcrit.publishedPlus)(services)
    }

    it should s"return ${testcrit.discoverPlus.value} when updating discoverability list with another attr as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateDiscover ++ updateLibAttr
      val ws = testUpdateLibraryAttributes(testcrit, payload, testcrit.discoverPlus)(services)
      if (testcrit.discoverPlus == StatusCodes.OK) {
        assertResult(updateDiscoverValue) {
          ws.get.attributes(discoverableWSAttribute)
        }
        assertResult(updateLibValue) {
          ws.get.attributes(updateLibName)
        }
      }
    }
    it should s"return ${testcrit.multiLibrary.value} when updating multiple standard library attrs as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateLibAttr ++ updateLibAttr2
      val ws = testUpdateLibraryAttributes(testcrit, payload, testcrit.multiLibrary)(services)
      if (testcrit.multiLibrary == StatusCodes.OK) {
        assertResult(updateLibValue) {
          ws.get.attributes(updateLibName)
        }
        assertResult(updateLibValue2) {
          ws.get.attributes(updateLibName2)
        }
      }
    }

    it should s"return ${testcrit.libraryPlusTags.value} when updating library attrs and tags as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateLibAttr ++ updateTagAttr
      val ws = testUpdateLibraryAttributes(testcrit, payload, testcrit.libraryPlusTags)(services)
    }

    it should s"return ${testcrit.libraryPlusWorkspace.value} when updating library attrs and workspace attrs as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateLibAttr ++ updateWsAttr
      val ws = testUpdateLibraryAttributes(testcrit, payload, testcrit.libraryPlusWorkspace)(services)
    }

    it should s"return ${testcrit.workspaceOnly.value} when updating workspace attrs as $testNameSuffix" in withLibraryPermissionTestDataApiServicesAndUser(user.userEmail.value) { services =>
      val payload = updateWsAttr
      val ws = testUpdateLibraryAttributes(testcrit, payload, testcrit.workspaceOnly)(services)
    }

  }



}
