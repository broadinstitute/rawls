package org.broadinstitute.dsde.rawls.integrationtest

import java.util.UUID
import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor

import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scala.util.Try
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, Retry, HttpGoogleServicesDAO}
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import spray.http.OAuth2BearerToken

import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext

class HttpGoogleServicesDAOSpec extends FlatSpec with Matchers with IntegrationTestConfig with BeforeAndAfterAll with Retry with OrientDbTestFixture {
  implicit val system = ActorSystem("HttpGoogleCloudStorageDAOSpec")
  val gcsDAO = new HttpGoogleServicesDAO(
    true, // use service account to manage buckets
    gcsConfig.getString("secrets"),
    gcsConfig.getString("pathToPem"),
    gcsConfig.getString("appsDomain"),
    gcsConfig.getString("groupsPrefix"),
    gcsConfig.getString("appName"),
    gcsConfig.getInt("deletedBucketCheckSeconds"),
    gcsConfig.getString("serviceProject"),
    gcsConfig.getString("tokenEncryptionKey"),
    gcsConfig.getString("tokenSecretsJson")
  )
  val mockDataSource = DataSource("memory:12345", "admin", "admin")
  val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(mockDataSource, containerDAO, gcsDAO))

  val testProject = "broad-dsde-dev"
  val testWorkspaceId = UUID.randomUUID.toString
  val testWorkspace = WorkspaceName(testProject, "someName")

  val testCreator = UserInfo(gcsDAO.clientSecrets.getDetails.get("client_email").toString, OAuth2BearerToken("testtoken"), 123, "123456789876543212345")
  val testCollaborator = UserInfo("fake_user_42@broadinstitute.org", OAuth2BearerToken("testtoken"), 123, "123456789876543212345aaa")

  override def afterAll() = {
    // one last-gasp attempt at cleaning up
    val bucketName = "fc-" + testWorkspaceId
    Try(gcsDAO.deleteWorkspace(
      bucketName,
      groupAccessLevelsAscending.map { accessLevel =>
        val groupName = RawlsGroupName(s"${bucketName}-${accessLevel.toString}")
        RawlsGroup(groupName, RawlsGroupEmail(gcsDAO.toGoogleGroupName(groupName)), Set.empty, Set.empty)
      },
      bucketDeletionMonitor))
  }

  "HttpGoogleServicesDAO" should "do all of the things" in {
    val googleWorkspaceInfo = Await.result(gcsDAO.setupWorkspace(testCreator, testProject, testWorkspaceId, testWorkspace), Duration.Inf)

    val storage = gcsDAO.getStorage(gcsDAO.getBucketServiceAccountCredential)

    // if this does not throw an exception, then the bucket exists
    val bucketResource = Await.result(retry(when500)(() => Future { storage.buckets.get(googleWorkspaceInfo.bucketName).execute() }), Duration.Inf)

    val readerGroup = googleWorkspaceInfo.groupsByAccessLevel(WorkspaceAccessLevels.Read)
    val writerGroup = googleWorkspaceInfo.groupsByAccessLevel(WorkspaceAccessLevels.Write)
    val ownerGroup = googleWorkspaceInfo.groupsByAccessLevel(WorkspaceAccessLevels.Owner)

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
    val readerResource = Await.result(retry(when500)(() => Future { directory.groups.get(readerGroup.groupEmail.value).execute() }), Duration.Inf)
    val writerResource = Await.result(retry(when500)(() => Future { directory.groups.get(writerGroup.groupEmail.value).execute() }), Duration.Inf)
    val ownerResource = Await.result(retry(when500)(() => Future { directory.groups.get(ownerGroup.groupEmail.value).execute() }), Duration.Inf)

    // delete the workspace bucket and groups. confirm that the corresponding groups are deleted
    Await.result(gcsDAO.deleteWorkspace(googleWorkspaceInfo.bucketName, googleWorkspaceInfo.groupsByAccessLevel.values.toSeq, bucketDeletionMonitor), Duration.Inf)
    intercept[GoogleJsonResponseException] { directory.groups.get(readerGroup.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(writerGroup.groupEmail.value).execute() }
    intercept[GoogleJsonResponseException] { directory.groups.get(ownerGroup.groupEmail.value).execute() }
  }

  it should "handle failures setting up workspace" in {
    intercept[GoogleJsonResponseException] {
      Await.result(gcsDAO.setupWorkspace(testCreator, "not a project", testWorkspaceId, testWorkspace), Duration.Inf)
    }

    val groupRefs = groupAccessLevelsAscending.map { accessLevel =>
      val groupName = RawlsGroupName(gcsDAO.workspaceAccessGroupName(testWorkspaceId, accessLevel))
      RawlsGroup(groupName, RawlsGroupEmail(gcsDAO.toGoogleGroupName(groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    }

    val directory = gcsDAO.getGroupDirectory
    groupRefs.foreach { groupRef =>
      intercept[GoogleJsonResponseException] { directory.groups.get(groupRef.groupEmail.value).execute() }
    }
  }

  it should "crud tokens" in {
    val userInfo = UserInfo(null, null, 0, UUID.randomUUID().toString)
    assertResult(None) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    assertResult(None) { Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf) }
    Await.result(gcsDAO.storeToken(userInfo, "testtoken"), Duration.Inf)
    assertResult(Some("testtoken")) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    val storeTime = Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf).get

    Thread.sleep(100)

    Await.result(gcsDAO.storeToken(userInfo, "testtoken2"), Duration.Inf)
    assertResult(Some("testtoken2")) { Await.result(gcsDAO.getToken(RawlsUser(userInfo)), Duration.Inf) }
    assert(Await.result(gcsDAO.getTokenDate(userInfo), Duration.Inf).get.isAfter(storeTime))

    Await.result(gcsDAO.deleteToken(userInfo), Duration.Inf)
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

    gcsDAO.getGroupDirectory.groups().delete(gcsDAO.toProxyFromUser(user.userSubjectId)).execute()
  }

  private def when500( throwable: Throwable ): Boolean = {
    throwable match {
      case gjre: GoogleJsonResponseException => gjre.getStatusCode/100 == 5
      case _ => false
    }
  }
}