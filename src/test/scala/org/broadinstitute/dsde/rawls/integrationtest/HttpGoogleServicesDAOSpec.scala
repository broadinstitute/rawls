package org.broadinstitute.dsde.rawls.integrationtest

import java.io.StringReader
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import com.google.api.client.http.{HttpHeaders, HttpResponseException}
import com.google.api.client.http.HttpResponseException
import com.google.api.services.cloudbilling.Cloudbilling
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.DeleteBucket

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.json.jackson2.JacksonFactory
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.http.{OAuth2BearerToken, StatusCodes}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent

import scala.collection.JavaConversions._
import scala.util.Try

class HttpGoogleServicesDAOSpec extends FlatSpec with Matchers with IntegrationTestConfig with Retry with TestDriverComponent with BeforeAndAfterAll {

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
    gcsConfig.getString("billingEmail")

  )

  slickDataSource.initWithSlick()
  val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(slickDataSource, gcsDAO))

  val testProject = "broad-dsde-dev"
  val testWorkspaceId = UUID.randomUUID.toString
  val testWorkspace = WorkspaceName(testProject, "someName")
  val testRealm = RawlsGroupRef(RawlsGroupName("test-realm"))

  val testCreator = UserInfo(gcsDAO.clientSecrets.getDetails.get("client_email").toString, OAuth2BearerToken("testtoken"), 123, "123456789876543212345")
  val testCollaborator = UserInfo("fake_user_42@broadinstitute.org", OAuth2BearerToken("testtoken"), 123, "123456789876543212345aaa")

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

  "HttpGoogleServicesDAO" should "do all of the things" in {
    val googleWorkspaceInfo = Await.result(gcsDAO.setupWorkspace(testCreator, testProject, testWorkspaceId, testWorkspace, None), Duration.Inf)

    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    // if this does not throw an exception, then the bucket exists
    val bucketResource = Await.result(retry(when500)(() => Future { storage.buckets.get(googleWorkspaceInfo.bucketName).execute() }), Duration.Inf)

    // check that intersection groups are not present without a realm
    googleWorkspaceInfo.intersectionGroupsByLevel shouldBe None

    val readerGroup = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Read)
    val writerGroup = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Write)
    val ownerGroup = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Owner)

    // check that the access level for each group is what we expect
    val readerBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val writerBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val ownerBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val svcAcctBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerBAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    ownerBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    svcAcctBAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the access level for each group is what we expect
    val readerDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val writerDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val ownerDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerGroup.groupEmail.value)).execute() }), Duration.Inf)
    val svcAcctDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    ownerDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    svcAcctDOAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the groups exist (i.e. that this doesn't throw exceptions)
    val directory = gcsDAO.getGroupDirectory
    Await.result(retry(when500)(() => Future { directory.groups.get(readerGroup.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(() => Future { directory.groups.get(writerGroup.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(() => Future { directory.groups.get(ownerGroup.groupEmail.value).execute() }), Duration.Inf)

    // delete the workspace bucket and groups. confirm that the corresponding groups are deleted
    Await.result(deleteWorkspaceGroupsAndBucket(googleWorkspaceInfo, bucketDeletionMonitor), Duration.Inf)
    intercept[GoogleJsonResponseException] { directory.groups.get(readerGroup.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerGroup.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerGroup.groupEmail.value).execute() }
  }

  it should "do all of the things with a realm" in {
    val googleWorkspaceInfo = Await.result(gcsDAO.setupWorkspace(testCreator, testProject, testWorkspaceId, testWorkspace, Option(testRealm)), Duration.Inf)

    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    // if this does not throw an exception, then the bucket exists
    val bucketResource = Await.result(retry(when500)(() => Future { storage.buckets.get(googleWorkspaceInfo.bucketName).execute() }), Duration.Inf)

    val readerAG = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Read)
    val writerAG = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Write)
    val ownerAG = googleWorkspaceInfo.accessGroupsByLevel(WorkspaceAccessLevels.Owner)

    val readerIG = googleWorkspaceInfo.intersectionGroupsByLevel.get(WorkspaceAccessLevels.Read)
    val writerIG = googleWorkspaceInfo.intersectionGroupsByLevel.get(WorkspaceAccessLevels.Write)
    val ownerIG = googleWorkspaceInfo.intersectionGroupsByLevel.get(WorkspaceAccessLevels.Owner)

    // check that the access level for each group is what we expect
    intercept[GoogleJsonResponseException] { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerAG.groupEmail.value)).execute() }
    val readerIGBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerIG.groupEmail.value)).execute() }), Duration.Inf)
    val writerIGBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerIG.groupEmail.value)).execute() }), Duration.Inf)
    val ownerIGBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerIG.groupEmail.value)).execute() }), Duration.Inf)
    val svcAcctBAC = Await.result(retry(when500)(() => Future { storage.bucketAccessControls.get(googleWorkspaceInfo.bucketName, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerIGBAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerIGBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    ownerIGBAC.getRole should be (WorkspaceAccessLevels.Write.toString)
    svcAcctBAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the access level for each group is what we expect
    intercept[GoogleJsonResponseException] { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerAG.groupEmail.value)).execute() }
    intercept[GoogleJsonResponseException] { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerAG.groupEmail.value)).execute() }
    val readerIGDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(readerIG.groupEmail.value)).execute() }), Duration.Inf)
    val writerIGDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(writerIG.groupEmail.value)).execute() }), Duration.Inf)
    val ownerIGDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, gcsDAO.makeGroupEntityString(ownerIG.groupEmail.value)).execute() }), Duration.Inf)
    val svcAcctDOAC = Await.result(retry(when500)(() => Future { storage.defaultObjectAccessControls.get(googleWorkspaceInfo.bucketName, "user-" + gcsDAO.serviceAccountClientId).execute() }), Duration.Inf)

    readerIGDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    writerIGDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    ownerIGDOAC.getRole should be (WorkspaceAccessLevels.Read.toString)
    svcAcctDOAC.getRole should be (WorkspaceAccessLevels.Owner.toString)

    // check that the groups exist (i.e. that this doesn't throw exceptions)
    val directory = gcsDAO.getGroupDirectory
    Await.result(retry(when500)(() => Future { directory.groups.get(readerAG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(() => Future { directory.groups.get(writerAG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(() => Future { directory.groups.get(ownerAG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(() => Future { directory.groups.get(readerIG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(() => Future { directory.groups.get(writerIG.groupEmail.value).execute() }), Duration.Inf)
    Await.result(retry(when500)(() => Future { directory.groups.get(ownerIG.groupEmail.value).execute() }), Duration.Inf)

    //check that the owner is a part of both the access group and the intersection group
    Await.result(retry(when500)(() => Future { directory.members.get(ownerAG.groupEmail.value, gcsDAO.toProxyFromUser(testCreator)).execute() }), Duration.Inf)
    Await.result(retry(when500)(() => Future { directory.members.get(ownerIG.groupEmail.value, gcsDAO.toProxyFromUser(testCreator)).execute() }), Duration.Inf)

    // delete the workspace bucket and groups. confirm that the corresponding groups are deleted
    Await.result(deleteWorkspaceGroupsAndBucket(googleWorkspaceInfo, bucketDeletionMonitor), Duration.Inf)

    intercept[GoogleJsonResponseException] { directory.groups.get(readerAG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerAG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerAG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(readerIG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerIG.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerIG.groupEmail.value).execute() }
  }

  it should "handle failures setting up workspace" in {
    intercept[GoogleJsonResponseException] {
      Await.result(gcsDAO.setupWorkspace(testCreator, "not a project", testWorkspaceId, testWorkspace, Option(testRealm)), Duration.Inf)
    }

    val groups: Seq[RawlsGroup] = groupAccessLevelsAscending flatMap { accessLevel =>
      val accessGroupName = RawlsGroupName(gcsDAO.workspaceAccessGroupName(testWorkspaceId, accessLevel))
      val accessGroup = RawlsGroup(accessGroupName, RawlsGroupEmail(gcsDAO.toGoogleGroupName(accessGroupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])

      val intersectionGroupName = RawlsGroupName(gcsDAO.intersectionGroupName(testWorkspaceId, testRealm, accessLevel))
      val intersectionGroup = RawlsGroup(intersectionGroupName, RawlsGroupEmail(gcsDAO.toGoogleGroupName(intersectionGroupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
      Seq(accessGroup, intersectionGroup)
    }

    val directory = gcsDAO.getGroupDirectory
    groups.foreach { group =>
      intercept[GoogleJsonResponseException] { directory.groups.get(group.groupEmail.value).execute() }
    }
  }

  it should "handle failures getting workspace bucket" in {
    val random = scala.util.Random
    val testUser = testCreator.copy(userSubjectId = random.nextLong().toString)
    val googleWorkspaceInfo = Await.result(gcsDAO.setupWorkspace(testUser, testProject, testWorkspaceId, testWorkspace, None), Duration.Inf)

    val user = RawlsUser(UserInfo("foo@bar.com", null, 0, testUser.userSubjectId))

    Await.result(gcsDAO.createProxyGroup(user), Duration.Inf)
    assert(! Await.result(gcsDAO.isUserInProxyGroup(user), Duration.Inf)) //quickest way to remove access to a workspace bucket

    assert(Await.result(gcsDAO.diagnosticBucketRead(userInfo.copy(userSubjectId = testUser.userSubjectId), s"fc-${testWorkspaceId}"), Duration.Inf).get.statusCode.get == StatusCodes.Unauthorized)
    Await.result(gcsDAO.deleteProxyGroup(user), Duration.Inf)
  }

  it should "crud tokens" in {
    val userInfo = UserInfo(null, null, 0, UUID.randomUUID().toString)
    assertResult(None) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    assertResult(None) { Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf) }
    Await.result(gcsDAO.storeToken(userInfo, "testtoken"), Duration.Inf)
    assertResult(Some("testtoken")) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    val storeTime = Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf).get

    Thread.sleep(100)

    val credential = gcsDAO.getBucketServiceAccountCredential
    credential.refreshToken()
    val testToken = credential.getAccessToken
    Await.result(gcsDAO.storeToken(userInfo, testToken), Duration.Inf)
    assertResult(Some(testToken)) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    assert(Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf).get.isAfter(storeTime))

    Await.result(gcsDAO.revokeToken(RawlsUser(userInfo)), Duration.Inf)
    Await.result(gcsDAO.deleteToken(RawlsUser(userInfo)), Duration.Inf)
    assertResult(None) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    assertResult(None) { Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf) }
  }

  it should "crud proxy groups" in {
    val user = RawlsUser(UserInfo("foo@bar.com", null, 0, UUID.randomUUID().toString))
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
      gcsConfig.getString("billingEmail")
      )

    val userInfo = UserInfo("foo@bar.com", null, 0, testCreator.userSubjectId)
    val user = RawlsUser(userInfo)
    Await.result(mockGcsDAO.createProxyGroup(user), Duration.Inf)

    assertResult(Seq(
      RawlsBillingAccount(RawlsBillingAccountName("billingAccounts/firecloudHasThisOne"), firecloudHasAccess = true),
      RawlsBillingAccount(RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne"), firecloudHasAccess = false)
    )){
      Await.result(mockGcsDAO.listBillingAccounts(userInfo), Duration.Inf)
    }
  }

  it should "extract error messages from Google HttpResponseExceptions" in {
    val exc = new HttpResponseException.Builder(403, "PERMISSION_DENIED", new HttpHeaders())
      .setContent(
        """{
                      |      "code" : 403,
                      |      "errors" : [ {
                      |        "domain" : "global",
                      |        "message" : "Request had insufficient authentication scopes.",
                      |        "reason" : "forbidden"
                      |      } ],
                      |      "message" : "Request had insufficient authentication scopes.",
                      |      "status" : "PERMISSION_DENIED"
                      |    }""")
      .build()
    assertResult("Request had insufficient authentication scopes.") {
      gcsDAO.
        getGoogleErrorMessage(exc)
    }
  }

  it should "create a project" in {
    val projectName = RawlsBillingProjectName("dsde-test-" + UUID.randomUUID().toString.take(8))
    try {
      val billingAccount = RawlsBillingAccountName("billingAccounts/0089F0-98A321-679BA7")

      val projectOwners = gcsConfig.getStringList("projectTemplate.owners")
      val projectServices = gcsConfig.getStringList("projectTemplate.services")

      Await.result(gcsDAO.createProject(projectName, billingAccount, ProjectTemplate( Map("roles/owner" -> projectOwners), projectServices)), Duration.Inf)
    } finally {
      deleteProject(projectName)
    }
  }

  def deleteProject(projectName: RawlsBillingProjectName): Unit = {
    val resMgr = gcsDAO.getCloudResourceManager(gcsDAO.getBillingServiceAccountCredential)
    val billingManager = gcsDAO.getCloudBillingManager(gcsDAO.getBillingServiceAccountCredential)
    val projectNameString = projectName.value
    println(s"disabling billing for project $projectNameString: ${Try(billingManager.projects().updateBillingInfo(s"projects/${projectName.value}", new ProjectBillingInfo().setBillingEnabled(false)).execute())}")
    println(s"deleting project $projectNameString: ${Try(resMgr.projects().delete(projectNameString).execute())}")
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
}
