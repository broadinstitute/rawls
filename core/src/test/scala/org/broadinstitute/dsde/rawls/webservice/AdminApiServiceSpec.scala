package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.ProjectOwner
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ActiveSubmissionFormat

import spray.http._
import spray.json._

/**
 * Created by tsharpe on 9/28/15.
 */
class AdminApiServiceSpec extends ApiServiceSpec {

  import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService =>  T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService =>  T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def withConstantTestDataApiServices[T](testCode: TestApiService =>  T): T = {
    withConstantTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def getBillingProject(dataSource: SlickDataSource, project: RawlsBillingProject) = runAndWait(rawlsBillingProjectQuery.load(project.projectName))

  def loadUser(user: RawlsUser) = runAndWait(rawlsUserQuery.load(user))

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

  "AdminApi" should "return 200 when listing active submissions" in withConstantTestDataApiServices { services =>
    val expected = Seq(
      ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submissionNoWorkflows),
      ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission1),
      ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission2))

    import spray.json.DefaultJsonProtocol._
    Get(s"/admin/submissions") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertSameElements(expected, responseAs[Seq[ActiveSubmission]])
      }
  }

  // NOTE: we no longer support deleting entities that are tied to an existing submission - this will cause a
  // Referential integrity constraint violation - if we change that behavior we need to fix this test
  ignore should "*DISABLED* return 200 when listing active submissions and some entities are missing" in withTestDataApiServices { services =>
    import spray.json.DefaultJsonProtocol._
    Delete(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/entities/${testData.indiv1.entityType}/${testData.indiv1.name}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
    Delete(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/entities/${testData.sample2.entityType}/${testData.sample2.name}") ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }

    Get(s"/admin/submissions") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = Array(ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submission1),
          ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submission2),
          ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submissionTerminateTest))
        assertSameElements(expected, responseAs[Array[ActiveSubmission]])
      }
  }

  it should "return 204 when aborting an active submission" in withTestDataApiServices { services =>
    Delete(s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/${testData.submissionTerminateTest.submissionId}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "return 404 when aborting a bogus active submission" in withTestDataApiServices { services =>
    Delete(s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/fake") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }
  
  it should "return 200 when deleting a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")
    assert {
      getBillingProject(services.dataSource, project).isEmpty
    }

    Post("/billing", CreateRawlsBillingProjectFullRequest(project.projectName, services.gcsDAO.accessibleBillingAccountName)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }

        assert {
          getBillingProject(services.dataSource, project).nonEmpty
        }
      }

    Delete(s"/admin/billing/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 404 when deleting a nonexistent billing project" in withTestDataApiServices { services =>
    Delete(s"/admin/billing/missing_project") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 201 when registering a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    assert {
      getBillingProject(services.dataSource, project).isEmpty
    }

    Put(s"/admin/billing/register/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        assert {
          getBillingProject(services.dataSource, project).nonEmpty
        }
      }
  }

  it should "return 409 when attempting to re-register an existing billing project" in withTestDataApiServices { services =>
    Put(s"/admin/billing/register/duplicated_project") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        Put(s"/admin/billing/register/duplicated_project") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.Conflict) {
              status
            }
          }
      }
  }

  it should "return 200 when unregistering a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/admin/billing/register/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assert {
          getBillingProject(services.dataSource, project).nonEmpty
        }

        Delete(s"/admin/billing/unregister/${project.projectName.value}") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            assert {
              getBillingProject(services.dataSource, project).isEmpty
            }
          }
      }
  }

  it should "return 404 when unregistering a nonexistent billing project" in withTestDataApiServices { services =>
    Delete(s"/admin/billing/unregister/missing_project") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 when adding a user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/admin/billing/register/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assert {
          ! runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.groups(ProjectRoles.User).users.contains(testData.userOwner)
        }

        Put(s"/admin/billing/${project.projectName.value}/user/${testData.userOwner.userEmail.value}") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            assert {
              val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get
              loadedProject.groups(ProjectRoles.User).users.contains(testData.userOwner) && !loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userOwner)
            }
          }

        Put(s"/admin/billing/${project.projectName.value}/owner/${testData.userOwner.userEmail.value}") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            assert {
              val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get
              loadedProject.groups(ProjectRoles.User).users.contains(testData.userOwner) && loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userOwner)
            }
          }
      }
  }

  it should "return 404 when adding a nonexistent user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/admin/billing/register/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        Put(s"/admin/billing/${project.projectName.value}/nobody") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.NotFound) {
              status
            }
          }
      }
  }

  it should "return 404 when adding a user to a nonexistent project" in withTestDataApiServices { services =>
    Put(s"/admin/billing/missing_project/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 when removing a user from a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/admin/billing/register/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        Put(s"/admin/billing/${project.projectName.value}/user/${testData.userOwner.userEmail.value}") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assert {
              runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.groups(ProjectRoles.User).users.contains(testData.userOwner)
            }

            Delete(s"/admin/billing/${project.projectName.value}/user/${testData.userOwner.userEmail.value}") ~>
              sealRoute(services.adminRoutes) ~>
              check {
                assertResult(StatusCodes.OK) {
                  status
                }
                assert {
                  ! runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.groups(ProjectRoles.User).users.contains(testData.userOwner)
                }
              }
          }
      }
  }

  it should "return 404 when removing a nonexistent user from a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/admin/billing/register/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        Delete(s"/admin/billing/${project.projectName.value}/nobody") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.NotFound) {
              status
            }
          }
      }
  }

  it should "return 404 when removing a user from a nonexistent billing project" in withTestDataApiServices { services =>
    Delete(s"/admin/billing/missing_project/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 200 when listing a user's billing projects" in withTestDataApiServices { services =>
    import spray.json.DefaultJsonProtocol._
    val testUser = RawlsUser(RawlsUserSubjectId("test_subject_id"), RawlsUserEmail("test_user_email"))
    val project1 = billingProjectFromName("project1")

    runAndWait(rawlsUserQuery.save(testUser))

    Get(s"/admin/billing/list/${testUser.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Set.empty) {
          responseAs[Seq[RawlsBillingProjectName]].toSet
        }
      }

    Put(s"/admin/billing/register/${project1.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
    Put(s"/admin/billing/${project1.projectName.value}/user/${testUser.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Get(s"/admin/billing/list/${testUser.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.User, CreationStatuses.Ready))) {
          responseAs[Seq[RawlsBillingProjectMembership]].toSet
        }
      }
  }

  it should "return 201 when creating a new group" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))

    Post(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) { status }
      }
  }

  it should "return 201 when creating a new realm" in withTestDataApiServices { services =>
    val group = new RawlsRealmRef(RawlsGroupName("test_realm"))
    import spray.json.DefaultJsonProtocol._

    Get(s"/admin/realms") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        responseAs[Seq[RawlsRealmRef]] should not contain(group)
      }

    Post(s"/admin/realms", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) { status }
      }

    //check that the realm is actually there and categorized as a realm
    Get(s"/admin/realms") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        responseAs[Seq[RawlsRealmRef]] should contain(group)
      }
  }

  it should "return 201 when deleting a realm" in withTestDataApiServices { services =>
    val group = new RawlsRealmRef(RawlsGroupName("test_realm"))
    import spray.json.DefaultJsonProtocol._

    Post(s"/admin/realms", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) { status }
      }

    //check that the realm is actually there and categorized as a realm
    Get(s"/admin/realms") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        responseAs[Seq[RawlsRealmRef]] should contain(group)
      }

    Delete(s"/admin/realms", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
      }

    //check that the realm is no longer there
    Get(s"/admin/realms") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        responseAs[Seq[RawlsRealmRef]] should not contain(group)
      }
  }

  it should "return 201 when listing all realms" in withTestDataApiServices { services =>
    import spray.json.DefaultJsonProtocol._
    val realmRefs: Seq[RawlsRealmRef] = Seq(testData.dbGapAuthorizedUsersGroup, testData.realm, testData.realm2).map(group => RawlsRealmRef(group.groupName))

    Get(s"/admin/realms") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        responseAs[Seq[RawlsRealmRef]] should contain theSameElementsAs(realmRefs)
      }
  }

  it should "not return regular groups in the list of all realms" in withTestDataApiServices { services =>
    import spray.json.DefaultJsonProtocol._

    val realmRefs: Seq[RawlsRealmRef] = Seq(testData.dbGapAuthorizedUsersGroup, testData.realm, testData.realm2).map(group => RawlsRealmRef(group.groupName))
    val group = new RawlsGroupRef(RawlsGroupName("test_realm"))

    Get(s"/admin/realms") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        responseAs[Seq[RawlsRealmRef]] should contain theSameElementsAs(realmRefs)
      }

    Post(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) { status }
      }

    //check that the regular group that was just created is not returned as a realm
    Get(s"/admin/realms") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        responseAs[Seq[RawlsRealmRef]] should not contain group
      }
  }

  it should "return 409 when trying to create a realm that already exists" in withTestDataApiServices { services =>
    val group = new RawlsRealmRef(RawlsGroupName("test_realm"))

    Post(s"/admin/realms", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Post(s"/admin/realms", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) { status }
      }
  }

  it should "return 409 when trying to delete a realm that has workspaces in it" in withTestDataApiServices { services =>
    val realm: RawlsRealmRef = RawlsRealmRef(testData.dbGapAuthorizedUsersGroup.groupName)

    Delete(s"/admin/realms", httpJson(realm)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) { status }
      }
  }

  it should "return 409 when trying to create a group that already exists" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))

    Post(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Post(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) { status }
      }
  }

  it should "return 200 when deleting a group that exists" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))

    Post(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Delete(s"/admin/groups/${group.groupName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when trying to delete a group that does not exist" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("dbgap"))

    Delete(s"/admin/groups/${group.groupName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }


  it should "return 200 when adding a library curator" in withTestDataApiServices { services =>
    val testUser = "foo@bar.com"
    Put(s"/admin/user/role/curator/${testUser}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 200 when removing a library curator" in withTestDataApiServices { services =>
    val testUser = "foo@bar.com"
    Put(s"/admin/user/role/curator/${testUser}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Delete(s"/admin/user/role/curator/${testUser}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return a list of members only one level down in the group hierarchy" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))
    val subGroup = new RawlsGroupRef(RawlsGroupName("test_subGroup"))
    val subSubGroup = new RawlsGroupRef(RawlsGroupName("test_subSubGroup"))

    Post(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Post(s"/admin/groups", httpJson(subGroup)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Post(s"/admin/groups/${group.groupName.value}/members", httpJson(RawlsGroupMemberList(subGroupEmails = Option(Seq(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org"))))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Get(s"/admin/groups/${group.groupName.value}/members") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(UserList(List(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org"))) {
          responseAs[UserList]
        }
      }
  }

  it should "delete a DB user, user proxy group, ldap entry, and remove them from all users group" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // values from MockUserInfoDirectives
      val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created, response.entity.asString) {
            status
          }
        }

      assertUserExists(services, user)

      Delete(s"/admin/user/${user.userSubjectId.value}") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent, response.entity.asString) {
            status
          }
        }

      assertUserMissing(services, user)
    }
  }

  it should "safely delete a user twice" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // values from MockUserInfoDirectives
      val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      assertUserExists(services, user)

      Delete(s"/admin/user/${user.userSubjectId.value}") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent, response.entity.asString) {
            status
          }
        }

      assertUserMissing(services, user)

      // scenario: re-trying a deletion after something failed.  The DB user remains but not necessarily anything else

      runAndWait(rawlsUserQuery.save(user))

      Delete(s"/admin/user/${user.userSubjectId.value}") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }

      assertUserMissing(services, user)
    }
  }

  def saveGroupToDbAndGoogle(services: TestApiService, group: RawlsGroup) = {
    import driver.api._

    val action = rawlsGroupQuery.save(group) flatMap { grp =>
      DBIO.from(services.gcsDAO.createGoogleGroup(grp))
    }

    runAndWait(action)
  }

  it should "delete a DB user and remove them from groups and billing projects" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // values from MockUserInfoDirectives
      val testUser = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      assertUserExists(services, testUser)

      val user2 = RawlsUser(RawlsUserSubjectId("some_other_subject"), RawlsUserEmail("some_other_email"))
      runAndWait(rawlsUserQuery.save(user2))

      val group = RawlsGroup(RawlsGroupName("groupName"), RawlsGroupEmail("groupEmail"), Set(testUser, user2), Set.empty)
      saveGroupToDbAndGoogle(services, group)

      assertResult(Some(group)) {
        runAndWait(rawlsGroupQuery.load(group))
      }

      val project = RawlsBillingProject(RawlsBillingProjectName("project"), generateBillingGroups(RawlsBillingProjectName("project"), Map(ProjectRoles.Owner -> Set(testUser, user2)), Map.empty), "mock cromwell URL", CreationStatuses.Ready, None, None)

      project.groups.map { case (_,g) =>
        runAndWait(rawlsGroupQuery.save(g))
      }

      runAndWait(rawlsBillingProjectQuery.create(project))

      assertResult(Some(project)) {
        runAndWait(rawlsBillingProjectQuery.load(project.projectName))
      }

      Delete(s"/admin/user/${testUser.userSubjectId.value}") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent, response.entity.asString) {
            status
          }
        }

      assertUserMissing(services, testUser)

      assertResult(project.groups(ProjectRoles.Owner).copy(users = Set(user2))) {
        runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.groups(ProjectRoles.Owner)
      }

      assertResult(Some(group.copy(users = Set(user2)))) {
        runAndWait(rawlsGroupQuery.load(group))
      }
    }
  }

  val testWorkspace = new EmptyWorkspace
  it should "not delete a DB user when they have a submission" in withCustomTestDatabase(testWorkspace) { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // save groups to Mock Google so it doesn't complain about deleting them later

      import scala.concurrent.duration._
      Await.result(services.gcsDAO.createGoogleGroup(testWorkspace.ownerGroup), 10.seconds)
      Await.result(services.gcsDAO.createGoogleGroup(testWorkspace.writerGroup), 10.seconds)
      Await.result(services.gcsDAO.createGoogleGroup(testWorkspace.readerGroup), 10.seconds)

      // values from MockUserInfoDirectives
      val testUser = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      assertUserExists(services, testUser)

      val sub = createTestSubmission(testWorkspace.workspace, testData.methodConfig, testData.indiv2, testUser, Seq.empty, Map.empty, Seq.empty, Map.empty)

      withWorkspaceContext(testWorkspace.workspace) { context =>
        // these are from DefaultTestData, but we're using EmptyWorkspace to init the DB, so save them now
        runAndWait(methodConfigurationQuery.save(context, testData.methodConfig))
        runAndWait(entityQuery.save(context, testData.sample2))
        runAndWait(entityQuery.save(context, testData.sset2))
        runAndWait(entityQuery.save(context, testData.indiv2))
        runAndWait(submissionQuery.create(context, sub))
      }

      Delete(s"/admin/user/${testData.userOwner.userSubjectId.value}") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
          assert {
            responseAs[ErrorReport].message.contains("Cannot delete a user with submissions")
          }

        }

    }
  }

  it should "enable/disable user" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // values from MockUserInfoDirectives
      val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
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
      Post(s"/admin/user/${user.userSubjectId.value}/enable") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
      // OK to enable already-enabled user
      Post(s"/admin/user/${user.userSubjectId.value}/disable") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
      Get(s"/admin/user/${user.userSubjectId.value}") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(UserStatus(user, Map("google" -> false, "ldap" -> false, "allUsersGroup" -> true))) {
            responseAs[UserStatus]
          }
        }
      Post(s"/admin/user/${user.userSubjectId.value}/disable") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
      // OK to disable already-disabled user
      Post(s"/admin/user/${user.userSubjectId.value}/enable") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
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

  it should "add and remove users from ldap" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      Put(s"/admin/user/ldap/12345") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      assert {
        services.directoryDAO.exists(RawlsUserSubjectId("12345"))
      }

      Put(s"/admin/user/ldap/12345") ~>
      //should conflict a second time
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.Conflict) {
            status
          }
        }
      Delete(s"/admin/user/ldap/12345") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
    }
  }

  val userNoBilling = RawlsUser(RawlsUserSubjectId("4637649"), RawlsUserEmail("no-billing-projects@example.com"))
  val testDataUsers = Seq(testData.userProjectOwner, testData.userOwner, testData.userWriter, testData.userReader, testData.userReaderViaGroup, userNoBilling)

  it should "return 200 when listing users" in withTestDataApiServices { services =>
    runAndWait(rawlsUserQuery.save(userNoBilling))

    Get("/admin/users") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertSameElements(testDataUsers, responseAs[RawlsUserInfoList].userInfoList.map(_.user))
      }
  }

  it should "return 404 when adding a member that doesn't exist" in withTestDataApiServices { services =>
    val group = RawlsGroupRef(RawlsGroupName("test_group"))

    Post(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Post(s"/admin/groups/${group.groupName.value}/members", httpJson(RawlsGroupMemberList(subGroupEmails = Option(Seq(s"GROUP_blahhh@dev.firecloud.org"))))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 when removing a member from a group" in withTestDataApiServices { services =>
    val group = RawlsGroupRef(RawlsGroupName("test_group"))
    val subGroup = RawlsGroupRef(RawlsGroupName("test_subGroup"))

    //make main group
    Post(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    //make subgroup
    Post(s"/admin/groups", httpJson(subGroup)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    //put subgroup into main group
    Post(s"/admin/groups/${group.groupName.value}/members", httpJson(RawlsGroupMemberList(subGroupEmails = Option(Seq(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org"))))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    //verify subgroup was put into main group
    Get(s"/admin/groups/${group.groupName.value}/members") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(UserList(List(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org"))) {
          responseAs[UserList]
        }
      }
    //remove subgroup from main group
    Delete(s"/admin/groups/${group.groupName.value}/members", httpJson(RawlsGroupMemberList(subGroupEmails = Option(Seq(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org"))))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    //verify that subgroup was removed from main group
    Get(s"/admin/groups/${group.groupName.value}/members") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(UserList(List.empty)) {
          responseAs[UserList]
        }
      }
  }

  it should "return 200 when overwriting group membership" in withTestDataApiServices { services =>
    val user1 = RawlsUser(RawlsUserSubjectId(UUID.randomUUID().toString), RawlsUserEmail(s"${UUID.randomUUID().toString}@foo.com"))
    val user2 = RawlsUser(RawlsUserSubjectId(UUID.randomUUID().toString), RawlsUserEmail(s"${UUID.randomUUID().toString}@foo.com"))
    val user3 = RawlsUser(RawlsUserSubjectId(UUID.randomUUID().toString), RawlsUserEmail(s"${UUID.randomUUID().toString}@foo.com"))

    val subGroup1 = RawlsGroup(RawlsGroupName(UUID.randomUUID().toString), RawlsGroupEmail(s"${UUID.randomUUID().toString}@foo.com"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    val subGroup2 = RawlsGroup(RawlsGroupName(UUID.randomUUID().toString), RawlsGroupEmail(s"${UUID.randomUUID().toString}@foo.com"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    val subGroup3 = RawlsGroup(RawlsGroupName(UUID.randomUUID().toString), RawlsGroupEmail(s"${UUID.randomUUID().toString}@foo.com"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])

    val testGroup = RawlsGroup(RawlsGroupName(UUID.randomUUID().toString), RawlsGroupEmail(s"${UUID.randomUUID().toString}@foo.com"), Set[RawlsUserRef](user1, user2), Set[RawlsGroupRef](subGroup1, subGroup2))

    runAndWait(rawlsUserQuery.save(user1))
    runAndWait(rawlsUserQuery.save(user2))
    runAndWait(rawlsUserQuery.save(user3))

    runAndWait(rawlsGroupQuery.save(subGroup1))
    runAndWait(rawlsGroupQuery.save(subGroup2))
    runAndWait(rawlsGroupQuery.save(subGroup3))

    runAndWait(rawlsGroupQuery.save(testGroup))

    services.gcsDAO.createGoogleGroup(testGroup)

    Put(s"/admin/groups/${testGroup.groupName.value}/members", httpJson(RawlsGroupMemberList(userEmails = Option(Seq(user2.userEmail.value, user3.userEmail.value)), subGroupEmails = Option(Seq(subGroup2.groupEmail.value, subGroup3.groupEmail.value))))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent, response.entity.asString) { status }
      }

    assertResult(Option(testGroup.copy(users = Set[RawlsUserRef](user2, user3), subGroups = Set[RawlsGroupRef](subGroup2, subGroup3)))) { runAndWait(rawlsGroupQuery.load(testGroup)) }

    // put it back the way it was this time using subject ids and group names
    Put(s"/admin/groups/${testGroup.groupName.value}/members", httpJson(RawlsGroupMemberList(userSubjectIds = Option(Seq(user2.userSubjectId.value, user1.userSubjectId.value)), subGroupNames = Option(Seq(subGroup2.groupName.value, subGroup1.groupName.value))))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }

    assertResult(Option(testGroup)) { runAndWait(rawlsGroupQuery.load(testGroup)) }
  }

  it should "get, grant, revoke all user read access to workspace" in withTestDataApiServices { services =>
    runAndWait(rawlsGroupQuery.save(RawlsGroup(UserService.allUsersGroupRef.groupName, RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(UserService.allUsersGroupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])))

    testData.workspace.accessLevels.values.foreach(services.gcsDAO.createGoogleGroup)

    Get(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound, response.entity.asString) { status }
      }

    services.gpsDAO.messageLog.clear()
    Put(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) { status }
      }
    Get(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent, response.entity.asString) { status }
      }

    val group = runAndWait(rawlsGroupQuery.load(testData.workspace.accessLevels(WorkspaceAccessLevels.Read))).get
    assert(services.gpsDAO.receivedMessage(services.googleGroupSyncTopic, RawlsGroup.toRef(group).toJson.compactPrint, 1))

    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }

    Delete(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
      }
    Get(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound, response.entity.asString) { status }
      }
    assert(services.gpsDAO.receivedMessage(services.googleGroupSyncTopic, RawlsGroup.toRef(group).toJson.compactPrint, 2))

    Get(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }

  }

  it should "sync group membership" in withTestDataApiServices { services =>
    val inGoogleGroup = RawlsGroup(
      RawlsGroupName("google"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("google"))),
      Set.empty[RawlsUserRef],
      Set.empty[RawlsGroupRef])
    val inBothGroup = RawlsGroup(
      RawlsGroupName("both"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("both"))),
      Set.empty[RawlsUserRef],
      Set.empty[RawlsGroupRef])
    val inDbGroup = RawlsGroup(
      RawlsGroupName("db"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("db"))),
      Set.empty[RawlsUserRef],
      Set.empty[RawlsGroupRef])

    val inGoogleUser = RawlsUser(RawlsUserSubjectId("google"), RawlsUserEmail("google@fc.org"))
    val inBothUser = RawlsUser(RawlsUserSubjectId("both"), RawlsUserEmail("both@fc.org"))
    val inDbUser = RawlsUser(RawlsUserSubjectId("db"), RawlsUserEmail("db@fc.org"))
    val unknownUserInGoogle = RawlsUser(RawlsUserSubjectId("unknown"), RawlsUserEmail("unknown@fc.org"))

    val topGroup = RawlsGroup(
      RawlsGroupName("synctest"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("synctest"))),
      Set[RawlsUserRef](inBothUser, inDbUser),
      Set[RawlsGroupRef](inBothGroup, inDbGroup))

    Await.result(services.gcsDAO.createGoogleGroup(topGroup), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Right(inGoogleGroup)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Right(inBothGroup)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Left(inGoogleUser)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Left(inBothUser)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Left(unknownUserInGoogle)), Duration.Inf)

    runAndWait(rawlsUserQuery.save(inGoogleUser))
    runAndWait(rawlsUserQuery.save(inBothUser))
    runAndWait(rawlsUserQuery.save(inDbUser))

    runAndWait(rawlsGroupQuery.save(inGoogleGroup))
    runAndWait(rawlsGroupQuery.save(inBothGroup))
    runAndWait(rawlsGroupQuery.save(inDbGroup))

    runAndWait(rawlsGroupQuery.save(topGroup))

    Post(s"/admin/groups/${topGroup.groupName.value}/sync") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }

        val expected = Seq(
          SyncReportItem("added", inDbUser.userEmail.value, None),
          SyncReportItem("added", inDbGroup.groupEmail.value, None),
          SyncReportItem("removed", inGoogleUser.userEmail.value, None),
          SyncReportItem("removed", inGoogleGroup.groupEmail.value, None),
          SyncReportItem("removed", unknownUserInGoogle.userEmail.value, None)
        )
        assertSameElements(expected, responseAs[SyncReport].items)
      }
  }

  it should "get the status of a workspace" in withTestDataApiServices { services =>
    val testUser = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

    runAndWait(rawlsUserQuery.save(testUser))

    Get(s"/admin/validate/${testData.workspace.namespace}/${testData.workspace.name}?userSubjectId=${testUser.userSubjectId.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val responseStatus = responseAs[WorkspaceStatus]
        assertResult(WorkspaceName(testData.workspace.namespace, testData.workspace.name)) {
          responseStatus.workspaceName
        }

        val expected = Map("GOOGLE_BUCKET_WRITE: aBucket" -> "USER_CAN_WRITE",
            "WORKSPACE_ACCESS_GROUP: PROJECT_myNamespace-Owner" -> "FOUND",
            "WORKSPACE_ACCESS_GROUP: myNamespace/myWorkspace OWNER" -> "FOUND",
            "FIRECLOUD_USER_PROXY: aBucket" -> "NOT_FOUND",
            "WORKSPACE_USER_ACCESS_LEVEL" -> "PROJECT_OWNER",
            "GOOGLE_ACCESS_GROUP: GROUP_PROJECT_myNamespace-Owner@dev.firecloud.org" -> "FOUND",
            "GOOGLE_ACCESS_GROUP: myNamespace/myWorkspace OWNER@example.com" -> "FOUND",
            "GOOGLE_ACCESS_GROUP: myNamespace/myWorkspace WRITER@example.com" -> "FOUND",
            "GOOGLE_ACCESS_GROUP: myNamespace/myWorkspace READER@example.com" -> "FOUND",
            "GOOGLE_BUCKET: aBucket" -> "FOUND",
            "GOOGLE_USER_ACCESS_LEVEL: GROUP_PROJECT_myNamespace-Owner@dev.firecloud.org" -> "FOUND",
            "FIRECLOUD_USER: 123456789876543212345" -> "FOUND",
            "WORKSPACE_ACCESS_GROUP: myNamespace/myWorkspace WRITER" -> "FOUND",
            "WORKSPACE_ACCESS_GROUP: myNamespace/myWorkspace READER" -> "FOUND",
            "WORKSPACE_INTERSECTION_GROUP: myNamespace/myWorkspace READER" -> "FOUND",
            "WORKSPACE_INTERSECTION_GROUP: myNamespace/myWorkspace WRITER" -> "FOUND",
            "WORKSPACE_INTERSECTION_GROUP: myNamespace/myWorkspace OWNER" -> "FOUND",
            "WORKSPACE_INTERSECTION_GROUP: PROJECT_myNamespace-Owner" -> "FOUND",
            "GOOGLE_INTERSECTION_GROUP: GROUP_PROJECT_myNamespace-Owner@dev.firecloud.org" -> "FOUND",
            "GOOGLE_INTERSECTION_GROUP: myNamespace/myWorkspace OWNER@example.com" -> "FOUND",
            "GOOGLE_INTERSECTION_GROUP: myNamespace/myWorkspace WRITER@example.com" -> "FOUND",
            "GOOGLE_INTERSECTION_GROUP: myNamespace/myWorkspace READER@example.com" -> "FOUND"
          )
        assertSameElements(expected, responseStatus.statuses)
      }
  }

  it should "return 200 when listing all workspaces" in withTestDataApiServices { services =>
    import spray.json.DefaultJsonProtocol._
    Get(s"/admin/workspaces") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        // TODO: why is this result returned out of order?
        sortAndAssertWorkspaceResult(testData.allWorkspaces) { responseAs[Seq[Workspace]] }
      }
  }

  it should "return 200 when getting workspaces by a string attribute" in withConstantTestDataApiServices { services =>
    import spray.json.DefaultJsonProtocol._
    Get(s"/admin/workspaces?attributeName=string&valueString=yep%2C%20it's%20a%20string") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertWorkspaceResult(Seq(constantData.workspace)) { responseAs[Seq[Workspace]] }
      }
  }

  it should "return 200 when getting workspaces by a numeric attribute" in withConstantTestDataApiServices { services =>
    import spray.json.DefaultJsonProtocol._
    Get(s"/admin/workspaces?attributeName=number&valueNumber=10") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertWorkspaceResult(Seq(constantData.workspace)) { responseAs[Seq[Workspace]] }
      }
  }

  it should "return 200 when getting workspaces by a boolean attribute" in withTestDataApiServices { services =>
    import spray.json.DefaultJsonProtocol._
    Get(s"/admin/workspaces?attributeName=library%3Apublished&valueBoolean=true") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertWorkspaceResult(Seq(testData.workspacePublished)) { responseAs[Seq[Workspace]] }
      }
  }

  it should "delete a workspace" in withTestDataApiServices { services =>
    Delete(s"/admin/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted, response.entity.asString) {
          status
        }
      }
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.workspace.toWorkspaceName))
    }
  }

  it should "delete workspace groups when deleting a workspace" in withTestDataApiServices { services =>
    val workspaceGroupRefs = (testData.workspace.accessLevels.values.toSet ++ testData.workspace.realmACLs.values) - testData.workspace.accessLevels(ProjectOwner)
    workspaceGroupRefs foreach { case groupRef =>
      assertResult(Option(groupRef)) {
        runAndWait(rawlsGroupQuery.load(groupRef)) map RawlsGroup.toRef
      }
    }

    Delete(s"/admin/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }

    workspaceGroupRefs foreach { case groupRef =>
      assertResult(None) {
        runAndWait(rawlsGroupQuery.load(groupRef))
      }
    }

  }

  it should "return 200 when reading a Google Genomics operation" in withTestDataApiServices { services => {
    import spray.json.DefaultJsonProtocol._
    Get("/admin/genomics/operations/dummy-job-id") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult("""{"foo":"bar"}""".parseJson.asJsObject) {
          responseAs[JsObject]
        }
      }
  }}

  it should "return 200 when querying firecloud statistics with valid dates" in withTestDataApiServices { services =>
    Get("/admin/statistics?startDate=2010-10-10&endDate=2011-10-10") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 400 when querying firecloud statistics with invalid (equal) dates" in withTestDataApiServices { services =>
    Get("/admin/statistics?startDate=2010-10-10&endDate=2010-10-10") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 when querying firecloud statistics with invalid dates" in withTestDataApiServices { services =>
    Get("/admin/statistics?startDate=2011-10-10&endDate=2010-10-10") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 500 when querying firecloud statistics illformed dates" in withTestDataApiServices { services =>
    Get("/admin/statistics?startDate=foo&endDate=bar") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.InternalServerError) {
          status
        }
      }
  }
}
