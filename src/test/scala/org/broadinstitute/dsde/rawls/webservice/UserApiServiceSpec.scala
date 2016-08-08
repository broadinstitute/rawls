package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import spray.http._

import scala.concurrent.{Await, ExecutionContext}

/**
 * Created by dvoet on 4/24/15.
 */
class UserApiServiceSpec extends ApiServiceSpec {
  case class TestApiService(dataSource: SlickDataSource, user: String, gcsDAO: MockGoogleServicesDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, user: String = "test_token")(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"))
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def userFromId(subjectId: String) =
    RawlsUser(RawlsUserSubjectId(subjectId), RawlsUserEmail("dummy@example.com"))

  def loadUser(user: RawlsUser) = runAndWait(rawlsUserQuery.load(user))


  "UserApi" should "put token and get date" in withTestDataApiServices { services =>
    Put("/user/refreshToken", httpJson(UserRefreshToken("gobblegobble"))) ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }

    Get("/user/refreshTokenDate") ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.OK) {status} }
  }

  it should "get 404 when token is not set" in withTestDataApiServices { services =>
    Get("/user/refreshTokenDate") ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  def assertUserMissing(services: TestApiService, user: RawlsUser): Unit = {
    assert {
      loadUser(user).isEmpty
    }
    assert {
      val group = runAndWait(rawlsGroupQuery.load(UserService.allUsersGroupRef))
      group.isEmpty || ! group.get.users.contains(user)
    }

    assert {
      !services.gcsDAO.containsProxyGroup(user)
    }
    assert {
      !services.directoryDAO.exists(user.userSubjectId)
    }
  }

  def assertUserExists(services: TestApiService, user: RawlsUser): Unit = {
    assert {
      loadUser(user).nonEmpty
    }
    assert {
      val group = runAndWait(rawlsGroupQuery.load(UserService.allUsersGroupRef))
      group.isDefined && group.get.users.contains(user)
    }

    assert {
      services.gcsDAO.containsProxyGroup(user)
    }
    assert {
      services.directoryDAO.exists(user.userSubjectId)
    }
  }

  it should "create a DB user, user proxy group, ldap entry, and add them to all users group, and enable them" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // values from MockUserInfoDirectives
      val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("test_token"))

      assertUserMissing(services, user)

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      assertUserExists(services, user)

      Get(s"/admin/user/${user.userSubjectId.value}") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(UserStatus(user, Map("google" -> true, "ldap" -> true, "allUsersGroup" -> true))) {
            responseAs[UserStatus]
          }
        }
    }
  }



  it should "get a users own status" in withTestDataApiServices { services =>

    val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

    runAndWait(rawlsUserQuery.save(user))

    Get("/user") ~>
      sealRoute(services.getUserStatusRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(UserStatus(user, Map("google" -> false, "ldap" -> false, "allUsersGroup" -> false))) {
          responseAs[UserStatus]
        }
      }
  }

  it should "list a user's billing projects" in withTestDataApiServices { services =>

    // first add the project and user to the DB

    val billingUser = RawlsUser(RawlsUserSubjectId("nothing"), RawlsUserEmail("test_token"))
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), Set.empty, Set.empty, "mockBucketUrl")

    runAndWait(rawlsUserQuery.save(billingUser))

    Put(s"/admin/billing/${project1.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
    Put(s"/admin/billing/${project1.projectName.value}/user/${billingUser.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Get("/user/billing") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.User))) {
          import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
          responseAs[Seq[RawlsBillingProjectMembership]].toSet
        }
      }
  }

  it should "get details of a group a user is a member of" in withTestDataApiServices { services =>
    val group3 = RawlsGroup(RawlsGroupName("testgroupname3"), RawlsGroupEmail("testgroupname3@foo.bar"), Set[RawlsUserRef](RawlsUser(userInfo)), Set.empty[RawlsGroupRef])
    val group2 = RawlsGroup(RawlsGroupName("testgroupname2"), RawlsGroupEmail("testgroupname2@foo.bar"), Set.empty[RawlsUserRef], Set[RawlsGroupRef](group3))
    val group1 = RawlsGroup(RawlsGroupName("testgroupname1"), RawlsGroupEmail("testgroupname1@foo.bar"), Set.empty[RawlsUserRef], Set[RawlsGroupRef](group2))

    runAndWait(rawlsGroupQuery.save(group3))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group1))

    import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
    Get(s"/user/group/${group3.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(group3.toRawlsGroupShort) { responseAs[RawlsGroupShort] }
      }
    Get(s"/user/group/${group2.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(group2.toRawlsGroupShort) { responseAs[RawlsGroupShort] }
      }
    Get(s"/user/group/${group1.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(group1.toRawlsGroupShort) { responseAs[RawlsGroupShort] }
      }
  }

  it should "not get details of a group a user is not a member of" in withTestDataApiServices { services =>
    val group3 = RawlsGroup(RawlsGroupName("testgroupname3"), RawlsGroupEmail("testgroupname3@foo.bar"), Set[RawlsUserRef](RawlsUser(userInfo)), Set.empty[RawlsGroupRef])
    val group2 = RawlsGroup(RawlsGroupName("testgroupname2"), RawlsGroupEmail("testgroupname2@foo.bar"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    val group1 = RawlsGroup(RawlsGroupName("testgroupname1"), RawlsGroupEmail("testgroupname1@foo.bar"), Set.empty[RawlsUserRef], Set[RawlsGroupRef](group2))

    runAndWait(rawlsGroupQuery.save(group3))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group1))

    import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
    Get(s"/user/group/${group3.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(group3.toRawlsGroupShort) { responseAs[RawlsGroupShort] }
      }
    Get(s"/user/group/${group2.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
    Get(s"/user/group/${group1.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "get not details of a group that does not exist" in withTestDataApiServices { services =>
    Get("/user/group/blarg") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }


}
