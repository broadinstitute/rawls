package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future

class RequesterPaysSetupServiceSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with TestDriverComponent {
  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(1, Seconds)))

  private def setupServices(dataSource: SlickDataSource) = {
    val mockBondApiDAO = mock[BondApiDAO](RETURNS_SMART_NULLS)
    val gcsDAO = new MockGoogleServicesDAO("foo")
    new RequesterPaysSetupService(dataSource, gcsDAO, mockBondApiDAO, "rp/role")
  }

  private def withMinimalTestDatabaseAndServices[T](testCode: RequesterPaysSetupService => T): T =
    withMinimalTestDatabase { dataSource =>
      testCode(setupServices(dataSource))
    }

  "getBondProviderServiceAccountEmails" should "get emails" in withMinimalTestDatabaseAndServices { service =>
    val expectedEmail = BondServiceAccountEmail("bondSA")

    when(service.bondApiDAO.getBondProviders()).thenReturn(Future.successful(List("p1", "p2")))
    when(service.bondApiDAO.getServiceAccountKey("p1", userInfo)).thenReturn(Future.successful(None))
    when(service.bondApiDAO.getServiceAccountKey("p2", userInfo))
      .thenReturn(Future.successful(Some(BondResponseData(expectedEmail))))

    service.getBondProviderServiceAccountEmails(userInfo).futureValue shouldBe List(expectedEmail)
  }

  it should "get empty list when no providers" in withMinimalTestDatabaseAndServices { service =>
    when(service.bondApiDAO.getBondProviders()).thenReturn(Future.successful(List.empty))

    service.getBondProviderServiceAccountEmails(userInfo).futureValue shouldBe List.empty
  }

  it should "get empty list when no linked accounts" in withMinimalTestDatabaseAndServices { service =>
    when(service.bondApiDAO.getBondProviders()).thenReturn(Future.successful(List("p1", "p2")))
    when(service.bondApiDAO.getServiceAccountKey("p1", userInfo)).thenReturn(Future.successful(None))
    when(service.bondApiDAO.getServiceAccountKey("p2", userInfo)).thenReturn(Future.successful(None))

    service.getBondProviderServiceAccountEmails(userInfo).futureValue shouldBe List.empty
  }

  "grantRequesterPaysToLinkedSAs" should "link" in withMinimalTestDatabaseAndServices { service =>
    val expectedEmail = BondServiceAccountEmail("bondSA")

    when(service.bondApiDAO.getBondProviders()).thenReturn(Future.successful(List("p1", "p2")))
    when(service.bondApiDAO.getServiceAccountKey("p1", userInfo)).thenReturn(Future.successful(None))
    when(service.bondApiDAO.getServiceAccountKey("p2", userInfo))
      .thenReturn(Future.successful(Some(BondResponseData(expectedEmail))))

    service.googleServicesDAO
      .asInstanceOf[MockGoogleServicesDAO]
      .policies
      .get(minimalTestData.workspace.googleProjectId) shouldBe None
    service.grantRequesterPaysToLinkedSAs(userInfo, minimalTestData.workspace).futureValue shouldBe List(expectedEmail)
    service.googleServicesDAO
      .asInstanceOf[MockGoogleServicesDAO]
      .policies
      .get(minimalTestData.workspace.googleProjectId) shouldBe Some(
      Map(service.requesterPaysRole -> Set("serviceAccount:" + expectedEmail.client_email))
    )

    // second call should not fail
    service.grantRequesterPaysToLinkedSAs(userInfo, minimalTestData.workspace).futureValue shouldBe List(expectedEmail)
  }

  "revokeUserFromWorkspace" should "unlink" in withMinimalTestDatabaseAndServices { service =>
    val expectedEmail = BondServiceAccountEmail("bondSA")

    // add user to 2 workspaces in same namespace
    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace.toWorkspaceName,
                                                   userInfo.userEmail,
                                                   Set(expectedEmail)
      )
    )
    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace2.toWorkspaceName,
                                                   userInfo.userEmail,
                                                   Set(expectedEmail)
      )
    )

    // add user to mock google bindings
    val initialBindings = Map(service.requesterPaysRole -> Set("serviceAccount:" + expectedEmail.client_email))
    service.googleServicesDAO
      .asInstanceOf[MockGoogleServicesDAO]
      .policies
      .put(minimalTestData.workspace.googleProjectId, initialBindings)

    service.revokeUserFromWorkspace(userInfo.userEmail, minimalTestData.workspace).futureValue shouldBe List(
      expectedEmail
    )
    service.googleServicesDAO
      .asInstanceOf[MockGoogleServicesDAO]
      .policies
      .get(minimalTestData.workspace.googleProjectId) shouldBe Some(Map(service.requesterPaysRole -> Set.empty))

    // remove user from other workspace and check that it did get removed from google bindings
    service.revokeUserFromWorkspace(userInfo.userEmail, minimalTestData.workspace2).futureValue shouldBe List(
      expectedEmail
    )
    service.googleServicesDAO
      .asInstanceOf[MockGoogleServicesDAO]
      .policies
      .get(minimalTestData.workspace2.googleProjectId) shouldBe Some(Map(service.requesterPaysRole -> Set.empty))
  }

  "revokeAllUsersFromWorkspace" should "unlink all in workspace" in withMinimalTestDatabaseAndServices { service =>
    val user1SA = BondServiceAccountEmail("bondSA1")
    val user2SA = BondServiceAccountEmail("bondSA2")
    val expectedEmails = Set(user1SA, user2SA)

    // add users to 2 workspaces in same namespace
    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace.toWorkspaceName,
                                                   userInfo.userEmail,
                                                   Set(user1SA)
      )
    )
    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace2.toWorkspaceName,
                                                   userInfo.userEmail,
                                                   Set(user1SA)
      )
    )
    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace.toWorkspaceName,
                                                   minimalTestData.userReader.userEmail,
                                                   Set(user2SA)
      )
    )
    runAndWait(
      workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace2.toWorkspaceName,
                                                   minimalTestData.userReader.userEmail,
                                                   Set(user2SA)
      )
    )

    // add 2 users to mock google bindings
    val initialBindings = Map(
      service.requesterPaysRole -> expectedEmails.map(expectedEmail => "serviceAccount:" + expectedEmail.client_email)
    )
    service.googleServicesDAO
      .asInstanceOf[MockGoogleServicesDAO]
      .policies
      .put(minimalTestData.workspace.googleProjectId, initialBindings)

    service
      .revokeAllUsersFromWorkspace(minimalTestData.workspace)
      .futureValue should contain theSameElementsAs expectedEmails
    service.googleServicesDAO
      .asInstanceOf[MockGoogleServicesDAO]
      .policies
      .get(minimalTestData.workspace.googleProjectId) shouldBe Some(Map(service.requesterPaysRole -> Set.empty))

    service
      .revokeAllUsersFromWorkspace(minimalTestData.workspace2)
      .futureValue should contain theSameElementsAs expectedEmails
    service.googleServicesDAO
      .asInstanceOf[MockGoogleServicesDAO]
      .policies
      .get(minimalTestData.workspace2.googleProjectId) shouldBe Some(Map(service.requesterPaysRole -> Set.empty))
  }

  "revokeAllUsersFromWorkspace" should "unlink all in workspace without preemptive unlinking in V1 Workspace" in withMinimalTestDatabaseAndServices {
    service =>
      val user1SA = BondServiceAccountEmail("bondSA1")
      val user2SA = BondServiceAccountEmail("bondSA2")
      val expectedEmails = Set(user1SA, user2SA)

      // add users to 2 workspaces in same namespace

      runAndWait(
        workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace.toWorkspaceName,
                                                     userInfo.userEmail,
                                                     Set(user1SA, user2SA)
        )
      )
      runAndWait(
        workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace2.toWorkspaceName,
                                                     userInfo.userEmail,
                                                     Set(user1SA, user2SA)
        )
      )
      /*
      runAndWait(
        workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace.toWorkspaceName,
                                                     minimalTestData.userReader.userEmail,
                                                     Set(user2SA)
        )
      )
      runAndWait(
        workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace2.toWorkspaceName,
                                                     minimalTestData.userReader.userEmail,
                                                     Set(user2SA)
        )
      )*/

      // add 2 users to mock google bindings
      val initialBindings = Map(
        service.requesterPaysRole -> expectedEmails.map(expectedEmail => "serviceAccount:" + expectedEmail.client_email)
      )
      service.googleServicesDAO
        .asInstanceOf[MockGoogleServicesDAO]
        .policies
        .put(minimalTestData.workspace.googleProjectId, initialBindings)

      // remove users from 1 workspace and check that it did not get removed from google bindings
      service
        .revokeAllUsersFromWorkspace(minimalTestData.workspace)
        .futureValue should contain theSameElementsAs expectedEmails
      service.googleServicesDAO
        .asInstanceOf[MockGoogleServicesDAO]
        .policies
        .get(minimalTestData.workspace.googleProjectId) shouldBe Some(initialBindings)

      // remove users from other workspace and check that it did get removed from google bindings
      service
        .revokeAllUsersFromWorkspace(minimalTestData.workspace2)
        .futureValue should contain theSameElementsAs expectedEmails
      service.googleServicesDAO
        .asInstanceOf[MockGoogleServicesDAO]
        .policies
        .get(minimalTestData.workspace2.googleProjectId) shouldBe Some(Map(service.requesterPaysRole -> Set.empty))
  }


  "revokeAllUsersFromWorkspace" should " not unlink all in namespace if all workspaces are no longer requester pays" in withMinimalTestDatabaseAndServices {
    service =>
      val user1SA = BondServiceAccountEmail("bondSA1")
      val user2SA = BondServiceAccountEmail("bondSA2")
      val expectedEmails = Set(user1SA, user2SA)

      // add users to 2 workspaces in same namespace
      runAndWait(
        workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace.toWorkspaceName,
                                                     userInfo.userEmail,
                                                     Set(user1SA)
        )
      )
      runAndWait(
        workspaceRequesterPaysQuery.insertAllForUser(minimalTestData.workspace.toWorkspaceName,
                                                     minimalTestData.userReader.userEmail,
                                                     Set(user2SA)
        )
      )

      // add 2 users to mock google bindings
      val initialBindings = Map(
        service.requesterPaysRole -> expectedEmails.map(expectedEmail => "serviceAccount:" + expectedEmail.client_email)
      )
      service.googleServicesDAO
        .asInstanceOf[MockGoogleServicesDAO]
        .policies
        .put(minimalTestData.workspace.googleProjectId, initialBindings)

      service
        .revokeAllUsersFromWorkspace(minimalTestData.workspace)
        .futureValue should contain theSameElementsAs expectedEmails
      service.googleServicesDAO
        .asInstanceOf[MockGoogleServicesDAO]
        .policies
        .get(minimalTestData.workspace.googleProjectId) shouldBe Some(initialBindings)

      service
        .revokeAllUsersFromWorkspace(minimalTestData.workspace2)
        .futureValue should contain theSameElementsAs expectedEmails
      service.googleServicesDAO
        .asInstanceOf[MockGoogleServicesDAO]
        .policies
        .get(minimalTestData.workspace2.googleProjectId) shouldBe Some(Map(service.requesterPaysRole -> Set.empty))
  }
}
