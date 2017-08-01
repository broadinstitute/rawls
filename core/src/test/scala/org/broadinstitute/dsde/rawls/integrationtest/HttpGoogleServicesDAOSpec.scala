package org.broadinstitute.dsde.rawls.integrationtest

import java.io.{ByteArrayInputStream, StringReader}
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import com.google.api.client.auth.oauth2.TokenResponseException
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{HttpResponseException, InputStreamContent}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.admin.directory.model.Group
import com.google.api.services.storage.model.{Bucket, StorageObject}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectOperationRecord, TestDriverComponent}
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.model.CreationStatuses.Ready
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.DeleteBucket
import org.broadinstitute.dsde.rawls.util.{MockitoTestUtils, Retry}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.http.{OAuth2BearerToken, StatusCodes}

import scala.collection.JavaConversions._
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, Future}
import scala.util.Try

class HttpGoogleServicesDAOSpec extends FlatSpec with Matchers with IntegrationTestConfig with Retry with TestDriverComponent with BeforeAndAfterAll with LazyLogging with Eventually with MockitoTestUtils with StatsDTestUtils {

  implicit val system = ActorSystem("HttpGoogleCloudStorageDAOSpec")
  val gcsDAO = new HttpGoogleServicesDAO(
    true, // use service account to manage buckets
    GoogleClientSecrets.load(
      JacksonFactory.getDefaultInstance, new StringReader(gcsConfig.getString("secrets"))),
    gcsConfig.getString("pathToPem"),
    gcsConfig.getString("appsDomain"),
    gcsConfig.getString("groupsPrefix"),
    gcsConfig.getString("appName"),
    gcsConfig.getInt("deletedBucketCheckSeconds"),
    gcsConfig.getString("serviceProject"),
    gcsConfig.getString("tokenEncryptionKey"),
    gcsConfig.getString("tokenSecretsJson"),
    GoogleClientSecrets.load(
      JacksonFactory.getDefaultInstance, new StringReader(gcsConfig.getString("billingSecrets"))),
    gcsConfig.getString("billingPemEmail"),
    gcsConfig.getString("pathToBillingPem"),
    gcsConfig.getString("billingEmail"),
    gcsConfig.getInt("bucketLogsMaxAge"),
    2,
    workbenchMetricBaseName
  )

  slickDataSource.initWithLiquibase(liquibaseChangeLog, Map.empty)
  val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(slickDataSource, gcsDAO))

  val testProject = "broad-dsde-dev"
  val testWorkspaceId = UUID.randomUUID.toString
  val testWorkspace = WorkspaceName(testProject, "someName")
  val testRealm = RawlsGroupRef(RawlsGroupName("test-realm"))

  val testCreator = UserInfo(RawlsUserEmail(gcsDAO.clientSecrets.getDetails.get("client_email").toString), OAuth2BearerToken("testtoken"), 123, RawlsUserSubjectId("123456789876543212345"))
  val testCollaborator = UserInfo(RawlsUserEmail("fake_user_42@broadinstitute.org"), OAuth2BearerToken("testtoken"), 123, RawlsUserSubjectId("123456789876543212345aaa"))

  private def deleteWorkspaceGroupsAndBucket(workspaceId: String) = {
    val bucketName = s"fc-$workspaceId"
    val accessGroups = groupAccessLevelsAscending.map  { accessLevel =>
      val groupName = RawlsGroupName(s"${bucketName}-${accessLevel.toString}")
      val groupEmail = RawlsGroupEmail(gcsDAO.toGoogleGroupName(groupName))
      RawlsGroup(groupName, groupEmail, Set.empty, Set.empty)
    }

    Future.traverse(accessGroups) { gcsDAO.deleteGoogleGroup } map { _ => bucketDeletionMonitor ! DeleteBucket(bucketName) }
  }

  private def deleteWorkspaceGroupsAndBucket(googleWorkspaceInfo: GoogleWorkspaceInfo, bucketDeletionMonitor: ActorRef) = {
    val accessGroups = googleWorkspaceInfo.accessGroupsByLevel.values
    val googleGroups = googleWorkspaceInfo.intersectionGroupsByLevel match {
      case Some(groups) => groups.values ++ accessGroups
      case None => accessGroups
    }
    Future.traverse(googleGroups) { gcsDAO.deleteGoogleGroup } map { _ => bucketDeletionMonitor ! DeleteBucket(googleWorkspaceInfo.bucketName) }
  }

  override def afterAll(): Unit = {
    // one last-gasp attempt at cleaning up
    deleteWorkspaceGroupsAndBucket(testWorkspaceId)
    super.afterAll()
  }

  behavior of "HttpGoogleServicesDAO"

  it should "do all of the things" in {

    val projectOwnerGoogleGroup = Await.result(gcsDAO.createGoogleGroup(RawlsGroupRef(RawlsGroupName(UUID.randomUUID.toString))), Duration.Inf)
    val project = RawlsBillingProject(RawlsBillingProjectName(testProject), Map(ProjectRoles.Owner -> projectOwnerGoogleGroup), "", Ready, None, None)

    val googleWorkspaceInfo = Await.result(gcsDAO.setupWorkspace(testCreator, project, testWorkspaceId, testWorkspace, Set.empty, None), Duration.Inf)

    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    // if this does not throw an exception, then the bucket exists
    val bucketResource = Await.result(retry(when500)(count => Future { storage.buckets.get(googleWorkspaceInfo.bucketName).execute() }), Duration.Inf)

    // check that intersection groups are not present without a realm
    googleWorkspaceInfo.intersectionGroupsByLevel shouldBe None

    val readerGroup = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Read)
    val writerGroup = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Write)
    val ownerGroup = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Owner)
    val projectOwnerGroup = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.ProjectOwner)

    // check that the access level for each group is what we expect
    val readerBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val writerBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val ownerBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val projectOwnerBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(projectOwnerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val svcAcctBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerBAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    ownerBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    projectOwnerBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    svcAcctBAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the access level for each group is what we expect
    val readerDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val writerDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val ownerDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val projectOwnerDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(projectOwnerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val svcAcctDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    ownerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    projectOwnerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    svcAcctDOAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the groups exist (i.e. that this doesn't throw exceptions)
    val directory = gcsDAO.getGroupDirectory
    Await.result(retry(when500)(count => Future { directory.groups.get(readerGroup.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.groups.get(writerGroup.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.groups.get(ownerGroup.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.groups.get(projectOwnerGroup.groupEmail.value).execute() }), Duration.Inf)

    val ownerMembers = Await.result(retry(when500)(count => Future { directory.members().list(ownerGroup.groupEmail.value).execute() }), Duration.Inf)
    ownerMembers.getMembers.map(_.getEmail) should be { Seq(gcsDAO.toProxyFromUser(testCreator)) }

    // should return 0, not raise an error due to storage logs not being written yet
    val usage = Await.result(gcsDAO.getBucketUsage(RawlsBillingProjectName(testProject), s"fc-$testWorkspaceId"), Duration.Inf)

    // delete the workspace bucket and groups. confirm that the corresponding groups are deleted
    Await.result(deleteWorkspaceGroupsAndBucket(googleWorkspaceInfo, bucketDeletionMonitor), Duration.Inf)
    Await.result(Future.traverse(project.groups.values) { group => gcsDAO.deleteGoogleGroup(group) }, Duration.Inf)
    intercept[GoogleJsonResponseException] { directory.groups.get(readerGroup.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerGroup.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerGroup.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(projectOwnerGroup.groupEmail.value).execute() }

    Await.result(gcsDAO.deleteGoogleGroup(projectOwnerGoogleGroup), Duration.Inf)
  }

  it should "do all of the things with a realm" in {
    val projectOwnerGoogleGroup = Await.result(gcsDAO.createGoogleGroup(RawlsGroupRef(RawlsGroupName(UUID.randomUUID.toString))), Duration.Inf)
    val project = RawlsBillingProject(RawlsBillingProjectName(testProject), Map(ProjectRoles.Owner -> projectOwnerGoogleGroup), "", Ready, None, None)

    val googleWorkspaceInfo = Await.result(gcsDAO.setupWorkspace(testCreator, project, testWorkspaceId, testWorkspace, Set(ManagedGroupRef(testRealm.groupName)), Option(Set(RawlsUserRef(testCreator.userSubjectId)))), Duration.Inf)

    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    // if this does not throw an exception, then the bucket exists
    val bucketResource = Await.result(retry(when500)(count => Future { storage.buckets.get(googleWorkspaceInfo.bucketName).execute() }), Duration.Inf)

    val readerAG = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Read)
    val writerAG = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Write)
    val ownerAG = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Owner)
    val projectOwnerAG = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.ProjectOwner)

    val readerIG = googleWorkspaceInfo.intersectionGroupsByLevel.get(WorkspaceAccessLevels.Read)
    val writerIG = googleWorkspaceInfo.intersectionGroupsByLevel.get(WorkspaceAccessLevels.Write)
    val ownerIG = googleWorkspaceInfo.intersectionGroupsByLevel.get(WorkspaceAccessLevels.Owner)
    val projectOwnerIG = googleWorkspaceInfo.intersectionGroupsByLevel.get(WorkspaceAccessLevels.ProjectOwner)

    // check that the access level for each group is what we expect
    intercept[GoogleJsonResponseException] { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(projectOwnerAG.groupEmail.value)).execute() }
    val readerIGBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerIG.groupEmail.value)).execute() }), Duration.Inf)
    val writerIGBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerIG.groupEmail.value)).execute() }), Duration.Inf)
    val ownerIGBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerIG.groupEmail.value)).execute() }), Duration.Inf)
    val projectOwnerIGBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(projectOwnerIG.groupEmail.value)).execute() }), Duration.Inf)
    val svcAcctBAC = Await.result(retry(when500)(count => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerIGBAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerIGBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    ownerIGBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    projectOwnerIGBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    svcAcctBAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the access level for each group is what we expect
    intercept[GoogleJsonResponseException] { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(projectOwnerAG.groupEmail.value)).execute() }
    val readerIGDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerIG.groupEmail.value)).execute() }), Duration.Inf)
    val writerIGDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerIG.groupEmail.value)).execute() }), Duration.Inf)
    val ownerIGDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerIG.groupEmail.value)).execute() }), Duration.Inf)
    val projectOwnerIGDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(projectOwnerIG.groupEmail.value)).execute() }), Duration.Inf)
    val svcAcctDOAC = Await.result(retry(when500)(count => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerIGDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerIGDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    ownerIGDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    projectOwnerIGDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    svcAcctDOAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the groups exist (i.e. that this doesn't throw exceptions)
    val directory = gcsDAO.getGroupDirectory
    Await.result(retry(when500)(count => Future { directory.groups.get(readerAG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.groups.get(writerAG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.groups.get(ownerAG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.groups.get(readerIG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.groups.get(writerIG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.groups.get(ownerIG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.groups.get(projectOwnerIG.groupEmail.value).execute() }), Duration.Inf)

    //check that the owner is a part of both the access group and the intersection group
    Await.result(retry(when500)(count => Future { directory.members.get(ownerAG.groupEmail.value, gcsDAO.toProxyFromUser(testCreator)).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.members.get(ownerIG.groupEmail.value, gcsDAO.toProxyFromUser(testCreator)).execute() }), Duration.Inf)
    Await.result(retry(when500)(count => Future { directory.members.get(projectOwnerIG.groupEmail.value, gcsDAO.toProxyFromUser(testCreator)).execute() }), Duration.Inf)

    // delete the workspace bucket and groups. confirm that the corresponding groups are deleted
    Await.result(deleteWorkspaceGroupsAndBucket(googleWorkspaceInfo, bucketDeletionMonitor), Duration.Inf)
    Await.result(Future.traverse(project.groups.values) { group => gcsDAO.deleteGoogleGroup(group) }, Duration.Inf)

    intercept[GoogleJsonResponseException] { directory.groups.get(readerAG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerAG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerAG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(projectOwnerAG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(readerIG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerIG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerIG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(projectOwnerIG.groupEmail.value).execute() }

    Await.result(gcsDAO.deleteGoogleGroup(projectOwnerGoogleGroup), Duration.Inf)
  }

  it should "handle failures setting up workspace" in {
    val project = RawlsBillingProject(RawlsBillingProjectName("not a project"), Map(ProjectRoles.Owner -> RawlsGroup(RawlsGroupName("foo"), RawlsGroupEmail("foo"), Set.empty, Set.empty)), "", Ready, None, None)
    intercept[GoogleJsonResponseException] {
      Await.result(gcsDAO.setupWorkspace(testCreator, project, testWorkspaceId, testWorkspace, Set(ManagedGroupRef(testRealm.groupName)), None), Duration.Inf)
    }

    val groups: Seq[RawlsGroup] = groupAccessLevelsAscending flatMap { accessLevel =>
      val accessGroupName = RawlsGroupName(gcsDAO.workspaceAccessGroupName(testWorkspaceId, accessLevel))
      val accessGroup = RawlsGroup(accessGroupName, RawlsGroupEmail(gcsDAO.toGoogleGroupName(accessGroupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])

      val intersectionGroupName = RawlsGroupName(gcsDAO.intersectionGroupName(testWorkspaceId, accessLevel))
      val intersectionGroup = RawlsGroup(intersectionGroupName, RawlsGroupEmail(gcsDAO.toGoogleGroupName(intersectionGroupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
      Seq(accessGroup, intersectionGroup)
    }

    val directory = gcsDAO.getGroupDirectory
    groups.foreach { group =>
      intercept[GoogleJsonResponseException] { directory.groups.get(group.groupEmail.value).execute() }
    }
  }

  it should "handle failures getting workspace bucket" in {
    val projectOwnerGroup = Await.result(gcsDAO.createGoogleGroup(RawlsGroupRef(RawlsGroupName(UUID.randomUUID.toString))), Duration.Inf)
    val project = RawlsBillingProject(RawlsBillingProjectName(testProject), Map(ProjectRoles.Owner -> projectOwnerGroup), "", Ready, None, None)
    val random = scala.util.Random
    val testUser = testCreator.copy(userSubjectId = RawlsUserSubjectId(random.nextLong().toString))
    val googleWorkspaceInfo = Await.result(gcsDAO.setupWorkspace(testUser, project, testWorkspaceId, testWorkspace, Set.empty, None), Duration.Inf)

    val user = RawlsUser(UserInfo(RawlsUserEmail("foo@bar.com"), null, 0, testUser.userSubjectId))

    Await.result(gcsDAO.createProxyGroup(user), Duration.Inf)
    assert(! Await.result(gcsDAO.isUserInProxyGroup(user), Duration.Inf)) //quickest way to remove access to a workspace bucket

    assert(Await.result(gcsDAO.diagnosticBucketRead(userInfo.copy(userSubjectId = testUser.userSubjectId), googleWorkspaceInfo.bucketName), Duration.Inf).get.statusCode.get == StatusCodes.Unauthorized)
    Await.result(gcsDAO.deleteProxyGroup(user), Duration.Inf)
    Await.result(deleteWorkspaceGroupsAndBucket(googleWorkspaceInfo, bucketDeletionMonitor), Duration.Inf)
    Await.result(Future.traverse(project.groups.values) { group => gcsDAO.deleteGoogleGroup(group) }, Duration.Inf)
  }

  it should "crud tokens" in {
    val userInfo = UserInfo(null, null, 0, RawlsUserSubjectId(UUID.randomUUID().toString))
    val rawlsUser = RawlsUser(userInfo)
    assertResult(None) { Await.result(gcsDAO.getToken(rawlsUser), Duration.Inf) }
    assertResult(None) { Await.result(gcsDAO.getTokenDate(rawlsUser), Duration.Inf) }
    Await.result(gcsDAO.storeToken(userInfo, "testtoken"), Duration.Inf)
    assertResult(Some("testtoken")) { Await.result(gcsDAO.getToken(rawlsUser), Duration.Inf) }
    intercept[TokenResponseException] { Await.result(gcsDAO.getTokenDate(rawlsUser), Duration.Inf).get }

    Thread.sleep(100)

    val credential = gcsDAO.getBucketServiceAccountCredential
    credential.refreshToken()
    val testToken = credential.getAccessToken
    Await.result(gcsDAO.storeToken(userInfo, testToken), Duration.Inf)
    assertResult(Some(testToken)) { Await.result(gcsDAO.getToken(rawlsUser), Duration.Inf) }

    Await.result(gcsDAO.revokeToken(rawlsUser), Duration.Inf)
    Await.result(gcsDAO.deleteToken(rawlsUser), Duration.Inf)
    assertResult(None) { Await.result(gcsDAO.getToken(rawlsUser), Duration.Inf) }
    assertResult(None) { Await.result(gcsDAO.getTokenDate(rawlsUser), Duration.Inf) }
  }

  it should "test get token date" in {
    // this RawlsUser must be a real user in the Dev environment with an up-to-date refresh token
    val rawlsUser = RawlsUser(RawlsUserSubjectId("110101671348597476266"), RawlsUserEmail("joel.broad.dev@gmail.com"))
    val credential = Await.result(gcsDAO.getUserCredentials(rawlsUser), Duration.Inf).get

    assert(Option(credential.getRefreshToken).isDefined)
    assert(Await.result(gcsDAO.getTokenDate(rawlsUser), Duration.Inf).isDefined)
  }

  it should "crud proxy groups" in {
    val user = RawlsUser(UserInfo(RawlsUserEmail("foo@bar.com"), null, 0, RawlsUserSubjectId(UUID.randomUUID().toString)))
    Await.result(gcsDAO.createProxyGroup(user), Duration.Inf)
    assert(! Await.result(gcsDAO.isUserInProxyGroup(user), Duration.Inf))
    Await.result(gcsDAO.addUserToProxyGroup(user), Duration.Inf)
    assert(Await.result(gcsDAO.isUserInProxyGroup(user), Duration.Inf))
    Await.result(gcsDAO.removeUserFromProxyGroup(user), Duration.Inf)
    assert(! Await.result(gcsDAO.isUserInProxyGroup(user), Duration.Inf))
    Await.result(gcsDAO.deleteProxyGroup(user), Duration.Inf)
    assert(! Await.result(gcsDAO.isUserInProxyGroup(user), Duration.Inf))
  }

  it should "correctly compose rawls and user billing account access lists" in {
    val jsonFactory = JacksonFactory.getDefaultInstance
    val mockGcsDAO = new MockBillingHttpGoogleServicesDAO(
      true, // use service account to manage buckets
      GoogleClientSecrets.load(jsonFactory, new StringReader(gcsConfig.getString("secrets"))),
      gcsConfig.getString("pathToPem"),
      gcsConfig.getString("appsDomain"),
      gcsConfig.getString("groupsPrefix"),
      gcsConfig.getString("appName"),
      gcsConfig.getInt("deletedBucketCheckSeconds"),
      gcsConfig.getString("serviceProject"),
      gcsConfig.getString("tokenEncryptionKey"),
      gcsConfig.getString("tokenSecretsJson"),
      GoogleClientSecrets.load(jsonFactory, new StringReader(gcsConfig.getString("billingSecrets"))),
      gcsConfig.getString("billingPemEmail"),
      gcsConfig.getString("pathToBillingPem"),
      gcsConfig.getString("billingEmail"),
      gcsConfig.getInt("bucketLogsMaxAge")
      )

    val userInfo = UserInfo(RawlsUserEmail("owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345"))
    val user = RawlsUser(userInfo)
    Await.result(mockGcsDAO.createProxyGroup(user), Duration.Inf)

    assertResult(Seq(
      RawlsBillingAccount(RawlsBillingAccountName("billingAccounts/firecloudHasThisOne"), true, "testBillingAccount"),
      RawlsBillingAccount(RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne"), false, "testBillingAccount")
    )){
      Await.result(mockGcsDAO.listBillingAccounts(userInfo), Duration.Inf)
    }
  }

  it should "create a project" in {

    val projectName = RawlsBillingProjectName("dsde-test-" + UUID.randomUUID().toString.take(8))
    val ownerGroup = Await.result(gcsDAO.createGoogleGroup(RawlsGroupRef(RawlsGroupName(s"${projectName.value}_owner"))), Duration.Inf)
    try {
      val billingAccount = RawlsBillingAccount(RawlsBillingAccountName("billingAccounts/0089F0-98A321-679BA7"), true, " This is A test ___-444")

      val projectOwners = gcsConfig.getStringList("projectTemplate.owners")
      val projectEditors = gcsConfig.getStringList("projectTemplate.editors")
      val projectServices = gcsConfig.getStringList("projectTemplate.services")
      val projectTemplate: ProjectTemplate = ProjectTemplate( Map("roles/owner" -> projectOwners, "roles/editor" -> projectEditors), projectServices)
      val project = RawlsBillingProject(projectName, Map(ProjectRoles.Owner -> ownerGroup), "", CreationStatuses.Creating, Option(billingAccount.accountName), None)

      val createOp = Await.result(gcsDAO.createProject(projectName, billingAccount), Duration.Inf)

      val doneCreateOp = Await.result(retryUntilSuccessOrTimeout(always)(10 seconds, 1 minutes) { count =>
        gcsDAO.pollOperation(createOp) map {
          case op@RawlsBillingProjectOperationRecord(_, _, _, true, _, _) => op
          case _ => throw new RuntimeException("not done")
        }
      }, 1 minutes)

      assert(doneCreateOp.errorMessage.isEmpty, createOp.errorMessage)

      val servicesOps = Await.result(gcsDAO.beginProjectSetup(project, projectTemplate, Map.empty), Duration.Inf).get

      val doneServicesOps = Await.result(retryUntilSuccessOrTimeout(always)(10 seconds, 6 minutes) { count =>
        Future.traverse(servicesOps) { serviceOp =>
          gcsDAO.pollOperation(serviceOp) map {
            case op@RawlsBillingProjectOperationRecord(_, _, _, true, _, _) => op
            case _ => throw new RuntimeException("not done")
          }
        }
      }, 6 minutes)

      assert(doneServicesOps.forall(_.errorMessage.isEmpty), doneServicesOps.collect { case op@RawlsBillingProjectOperationRecord(_, _, _, _, Some(_), _) => op })

      Await.result(gcsDAO.completeProjectSetup(project), Duration.Inf).get

      val bucket = Await.result(gcsDAO.getBucket(gcsDAO.getStorageLogsBucketName(projectName)), Duration.Inf).get
      bucket.getLifecycle.getRule.length should be (1)
      val rule = bucket.getLifecycle.getRule.head
      rule.getAction.getType should be ("Delete")
      rule.getCondition.getAge should be (180)
      val bucketAcl = Await.result(gcsDAO.getBucketACL(gcsDAO.getStorageLogsBucketName(projectName)), Duration.Inf).get
      bucketAcl.count(bac =>
        bac.getEntity == "group-cloud-storage-analytics@google.com" && bac.getRole == "WRITER") should be (1)
    } finally {
      gcsDAO.deleteGoogleGroup(ownerGroup)
      gcsDAO.deleteProject(projectName)
    }
  }

  it should "set lifecycle policy when deleting non-empty bucket" in {
    val bucketName = "services-dao-spec-" + UUID.randomUUID.toString
    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)
    val bucket = new Bucket().setName(bucketName)
    val bucketInserter = storage.buckets.insert(testProject, bucket)
    Await.result(retry(when500)(count => Future { bucketInserter.execute() }), Duration.Inf)
    insertObject(bucketName, s"HttpGoogleServiceDAOSpec-object-${UUID.randomUUID().toString}", "delete me")

    Await.result(gcsDAO.deleteBucket(bucketName, bucketDeletionMonitor), Duration.Inf)

    /*
     * There will be 2 Google calls in this case: one to delete the bucket (which will fail) and one to set the
     * lifecycle rule. deleteBucket doesn't wait for delete to fail before returning and the call to set the lifecycle
     * rule gets a separate Future. Therefore, waiting on deleteBucket doesn't guarantee that the lifecycle rule has
     * been set. However, it should happen within a couple of seconds so this test will just wait a little bit.
     */
    val rule = Await.result(retryUntilSuccessOrTimeout(failureLogMessage = "Bucket has no lifecycle rules")(1 second, 10 seconds) { count =>
      val fetchedBucket = Await.result(gcsDAO.getBucket(bucketName), Duration.Inf)
      val lifecycle = fetchedBucket.get.getLifecycle
      Option(lifecycle) match {
        case Some(l) =>
          l.getRule.length should be (1)
          Future.successful(l.getRule.head)
        case None =>
          Future.failed(new NullPointerException("No lifecycle rules"))
      }
    }, Duration.Inf)
    rule.getAction.getType should be ("Delete")
    rule.getCondition.getAge should be (0)

    // Final clean-up to make sure that the test bucket will eventually be deleted
    bucketDeletionMonitor ! DeleteBucket(bucketName)
  }

  it should "determine bucket usage" in {
    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    val projectName = RawlsBillingProjectName(testProject)
    val logBucketName = gcsDAO.getStorageLogsBucketName(projectName)
    val bucketName = "services-dao-spec-" + UUID.randomUUID.toString

    try {

      // add some log files for the test bucket, including storage and usage logs
      for (i <- 1 to 2) {
        // storage logs
        insertObject(logBucketName, s"${bucketName}_storage_2016_11_0${i}_08_00_00_0a1b2_v0", bucketStorageFileContent(bucketName, 240 * i))
        // usage logs
        insertObject(logBucketName, s"${bucketName}_usage_2016_11_0${i}_08_00_00_0a1b2_v0", "BOOM!")
        // storage logs for some other bucket that has a name that will come after the requested bucket
        insertObject(logBucketName, s"z${bucketName}_storage_2016_11_0${i}_08_00_00_0a1b2_v0", bucketStorageFileContent(bucketName, 0))
      }

      val usage = Await.result(gcsDAO.getBucketUsage(projectName, bucketName, Some(1L)), Duration.Inf)

      // 240 bytes per day, 2 days of logs, 24 hours per day
      // 240 * 2 / 24 = 20
      usage should be (20)

    } finally {
      try {
        for (i <- 1 to 2) {
          Await.result(retry(when500)(count => Future {
            storage.objects().delete(logBucketName, s"${bucketName}_storage_2016_11_0${i}_08_00_00_0a1b2_v0").execute() }), Duration.Inf)
          Await.result(retry(when500)(count => Future {
            storage.objects().delete(logBucketName, s"${bucketName}_usage_2016_11_0${i}_08_00_00_0a1b2_v0").execute() }), Duration.Inf)
          Await.result(retry(when500)(count => Future {
            storage.objects().delete(logBucketName, s"z${bucketName}_storage_2016_11_0${i}_08_00_00_0a1b2_v0").execute() }), Duration.Inf)
        }
      } catch {
        case ignore: Throwable => Unit
      }
    }
  }

  /* Google does not write storage logs for empty buckets. If a bucket is empty for long enough, all storage logs
   * (including the initial log) will expire. If a quick check shows that the bucket is actually empty, the lack of
   * storage logs can be assumed to be because the bucket has been empty for a long time.
   */
  it should "return 0 usage when there are no storage logs and the bucket is empty" in {
    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    val projectName = RawlsBillingProjectName(testProject)
    val logBucketName = gcsDAO.getStorageLogsBucketName(projectName)
    val bucketName = "services-dao-spec-" + UUID.randomUUID.toString
    val bucket = new Bucket().setName(bucketName)
    val bucketInserter = storage.buckets.insert(testProject, bucket)
    Await.result(retry(when500)(count => Future { bucketInserter.execute() }), Duration.Inf)

    try {
      val usage = Await.result(gcsDAO.getBucketUsage(projectName, bucketName), Duration.Inf)
      usage should be (0)
    } finally {
      Await.result(gcsDAO.deleteBucket(bucketName, bucketDeletionMonitor), Duration.Inf)
    }
  }

  /* Google does not write storage logs for empty buckets. If a bucket is empty for long enough, all storage logs
   * (including the initial log) will expire.
   *
   * HOWEVER, if a quick check shows that the bucket is NOT actually empty, the lack of storage logs can mean that
   * either storage logs are not being correctly written or something has been added to the bucket after being empty for
   * a long time. Since we can't tell the difference between these 2 cases, we show a "Not Available" message. If there
   * is not a problem with storage log writing, "Not Available" will be replaced with a real value in the next day or
   * two.
   */
  it should "return 'Not Available' when there are no storage logs and the bucket is NOT empty" in {
    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    val projectName = RawlsBillingProjectName(testProject)
    val logBucketName = gcsDAO.getStorageLogsBucketName(projectName)
    val bucketName = "services-dao-spec-" + UUID.randomUUID.toString
    val bucket = new Bucket().setName(bucketName)
    val bucketInserter = storage.buckets.insert(testProject, bucket)
    Await.result(retry(when500)(count => Future { bucketInserter.execute() }), Duration.Inf)
    insertObject(bucketName, "test-object", "test")

    val caught = intercept[GoogleStorageLogException] {
      try {
        val usage = Await.result(gcsDAO.getBucketUsage(projectName, bucketName), Duration.Inf)
        usage should be(0)
      } finally {
        Await.result(retry(when500)(count => Future { storage.objects().delete(bucketName, "test-object").execute() }), Duration.Inf)
        Await.result(gcsDAO.deleteBucket(bucketName, bucketDeletionMonitor), Duration.Inf)
      }
    }
    caught.getMessage should be("Not Available")
  }

  it should "recursively list group members" in {
    val directory = gcsDAO.getGroupDirectory
    val groupEmail = s"recursivetest@${gcsConfig.getString("appsDomain")}"
    val groupEmailSingle = s"recursivetestsingle@${gcsConfig.getString("appsDomain")}"

    Try { directory.groups().insert(new Group().setEmail(groupEmail)).execute() }
    Try { directory.groups().insert(new Group().setEmail(groupEmailSingle)).execute() }

    val emails = for (x <- 1 to 20) yield {
      val userEmail = s"foo$x@bar.com"
      Await.ready(gcsDAO.addEmailToGoogleGroup(groupEmail, userEmail), Duration.Inf)
      userEmail
    }

    val singleUserEmail = "foo@bar.com"
    Await.ready(gcsDAO.addEmailToGoogleGroup(groupEmailSingle, singleUserEmail), Duration.Inf)

    assertResult(emails.toSet) {
      Await.result(gcsDAO.listGroupMembers(RawlsGroup(null, RawlsGroupEmail(groupEmail), Set.empty, Set.empty)), Duration.Inf).get.keySet
    }

    assertResult(Set(singleUserEmail)) {
      Await.result(gcsDAO.listGroupMembers(RawlsGroup(null, RawlsGroupEmail(groupEmailSingle), Set.empty, Set.empty)), Duration.Inf).get.keySet
    }

    assertResult(None) {
      Await.result(gcsDAO.listGroupMembers(RawlsGroup(null, RawlsGroupEmail(s"doesnotexist@${gcsConfig.getString("appsDomain")}"), Set.empty, Set.empty)), Duration.Inf)
    }
  }


  private def insertObject(bucketName: String, objectName: String, content: String): String = {
    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)
    val o = new StorageObject().setName(objectName)
    val stream: InputStreamContent = new InputStreamContent("text/plain", new ByteArrayInputStream(content.getBytes))
    val inserter = storage.objects().insert(bucketName, o, stream)
    inserter.getMediaHttpUploader.setDirectUploadEnabled(true)
    Await.result(retry(when500)(count => Future { inserter.execute() }), Duration.Inf)
    objectName
  }

  private def bucketStorageFileContent(bucketName: String, storageByteHours: Int): String = {
    return s""""bucket","storage_byte_hours"
               |"$bucketName","$storageByteHours"
               |""".stripMargin
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => {
        ((t.getStatusCode == 403 || t.getStatusCode == 429) && t.getDetails.getErrors.head.getDomain.equalsIgnoreCase("usageLimits")) ||
          (t.getStatusCode == 400 && t.getDetails.getErrors.head.getReason.equalsIgnoreCase("invalid")) ||
          (t.getStatusCode == 404)
      }
      case t: HttpResponseException => t.getStatusCode/100 == 5
      case _ => false
    }
  }

//  it should "foo" in {
//    val credential = GoogleCredential.getApplicationDefault();
//    val cloudresourcemanagerService =
//      new CloudResourceManager.Builder(gcsDAO.httpTransport, gcsDAO.jsonFactory, credential)
//        .setApplicationName("Google Cloud Platform Sample")
//        .build();
//
//    val execute1 = cloudresourcemanagerService.operations().get("operations/pc.2305654183227435173").execute()
//    println(execute1)
//  }
}
