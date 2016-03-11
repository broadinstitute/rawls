package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.tinkerpop.blueprints.Vertex
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import spray.http.StatusCodes
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ActiveSubmissionFormat

/**
 * Created by tsharpe on 9/28/15.
 */
class AdminApiServiceSpec extends ApiServiceSpec {
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices(dataSource: SlickDataSource)(testCode: TestApiService => Any): Unit = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"))
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices(testCode: TestApiService => Any): Unit = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  import scala.collection.JavaConversions._

  def getMatchingBillingProjectVertices(dataSource: SlickDataSource, project: RawlsBillingProject) = runAndWait(rawlsBillingProjectQuery.load(project.projectName))

  def billingProjectFromName(name: String) = RawlsBillingProject(RawlsBillingProjectName(name), Set.empty, "mockBucketUrl")

  "AdminApi" should "return 200 when listing active submissions" in withTestDataApiServices { services =>
    Get(s"/admin/submissions") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        responseAs[Array[ActiveSubmission]] should contain
          theSameElementsAs(Array(ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submission1),
                                  ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submission2),
                                  ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submissionTerminateTest)))
      }
  }

  it should "return 200 when listing active submissions and some entities are missing" in withTestDataApiServices { services =>
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
        responseAs[Array[ActiveSubmission]] should contain
        theSameElementsAs(Array(ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submission1),
          ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submission2),
          ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submissionTerminateTest)))
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

  it should "return 201 when creating a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    assert {
      getMatchingBillingProjectVertices(services.dataSource, project).isEmpty
    }

    Put(s"/admin/billing/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        assert {
          getMatchingBillingProjectVertices(services.dataSource, project).nonEmpty
        }
      }
  }

  it should "return 409 when attempting to recreate an existing billing project" in withTestDataApiServices { services =>
    Put(s"/admin/billing/duplicated_project") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        Put(s"/admin/billing/duplicated_project") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.Conflict) {
              status
            }
          }
      }
  }

  it should "return 200 when deleting a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/admin/billing/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assert {
          getMatchingBillingProjectVertices(services.dataSource, project).nonEmpty
        }

        Delete(s"/admin/billing/${project.projectName.value}") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            assert {
              getMatchingBillingProjectVertices(services.dataSource, project).isEmpty
            }
          }
      }
  }

  it should "return 404 when deleting a nonexistent billing project" in withTestDataApiServices { services =>
    Delete(s"/admin/billing/missing_project") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 when adding a user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/admin/billing/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assert {
          ! runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.users.contains(testData.userOwner)
        }

        Put(s"/admin/billing/${project.projectName.value}/${testData.userOwner.userEmail.value}") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            assert {
              runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.users.contains(testData.userOwner)
            }
          }
      }
  }

  it should "return 404 when adding a nonexistent user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/admin/billing/${project.projectName.value}") ~>
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

    Put(s"/admin/billing/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        Put(s"/admin/billing/${project.projectName.value}/${testData.userOwner.userEmail.value}") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assert {
              runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.users.contains(testData.userOwner)
            }

            Delete(s"/admin/billing/${project.projectName.value}/${testData.userOwner.userEmail.value}") ~>
              sealRoute(services.adminRoutes) ~>
              check {
                assertResult(StatusCodes.OK) {
                  status
                }
                assert {
                  ! runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.users.contains(testData.userOwner)
                }
              }
          }
      }
  }

  it should "return 404 when removing a nonexistent user from a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/admin/billing/${project.projectName.value}") ~>
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

    Put(s"/admin/billing/${project1.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
    Put(s"/admin/billing/${project1.projectName.value}/${testUser.userEmail.value}") ~>
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
        assertResult(Set(project1.projectName)) {
          responseAs[Seq[RawlsBillingProjectName]].toSet
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
    Delete(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when trying to delete a group that does not exist" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("dbgap"))

    Delete(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
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

  it should "return 200 when listing users" in withTestDataApiServices { services =>
    val userOwner = RawlsUserInfo(testData.userOwner, Seq(RawlsBillingProjectName("myNamespace")))
    val userWriter = RawlsUserInfo(testData.userWriter, Seq.empty)
    val userReader = RawlsUserInfo(testData.userReader, Seq.empty)

    Get("/admin/users") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        responseAs[RawlsUserInfoList].userInfoList contains theSameElementsAs(Seq(userOwner, userWriter, userReader))
      }
  }

  it should "return 200 when importing users" in withTestDataApiServices { services =>
    val userOwner = RawlsUserInfo(testData.userOwner, Seq(RawlsBillingProjectName("myNamespace")))
    val userWriter = RawlsUserInfo(testData.userWriter, Seq.empty)
    val userReader = RawlsUserInfo(testData.userReader, Seq.empty)
    val user1 = RawlsUserInfo(RawlsUser(RawlsUserSubjectId("1"), RawlsUserEmail("owner-access2")), Seq(RawlsBillingProjectName("myNamespace")))
    val user2 = RawlsUserInfo(RawlsUser(RawlsUserSubjectId("2"), RawlsUserEmail("writer-access2")), Seq.empty)
    val user3 = RawlsUserInfo(RawlsUser(RawlsUserSubjectId("3"), RawlsUserEmail("reader-access2")), Seq.empty)

    val userInfoList = RawlsUserInfoList(Seq(user1, user2, user3))

    Post("/admin/users", httpJson(userInfoList)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Get("/admin/users") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        responseAs[RawlsUserInfoList].userInfoList contains theSameElementsAs(Seq(userOwner, userWriter, userReader, user1, user2, user3))
      }
  }

  it should "return 404 when adding a member that doesn't exist" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))

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
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))
    val subGroup = new RawlsGroupRef(RawlsGroupName("test_subGroup"))

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

    assertResult(Some(true)) {
      Await.result(services.gcsDAO.listGroupMembers(group), Duration.Inf).map { members =>
        members.contains(Right(UserService.allUsersGroupRef))
      }
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
      
    val group2 = runAndWait(rawlsGroupQuery.load(testData.workspace.accessLevels(WorkspaceAccessLevels.Read))).get

      assertResult(Some(false)) {
        Await.result(services.gcsDAO.listGroupMembers(group2), Duration.Inf).map { members =>
          members.contains(Right(UserService.allUsersGroupRef))
        }
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
    val inGraphGroup = RawlsGroup(
      RawlsGroupName("graph"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("graph"))),
      Set.empty[RawlsUserRef],
      Set.empty[RawlsGroupRef])

    val inGoogleUser = RawlsUser(RawlsUserSubjectId("google"), RawlsUserEmail("google@fc.org"))
    val inBothUser = RawlsUser(RawlsUserSubjectId("both"), RawlsUserEmail("both@fc.org"))
    val inGraphUser = RawlsUser(RawlsUserSubjectId("graph"), RawlsUserEmail("graph@fc.org"))

    val topGroup = RawlsGroup(
      RawlsGroupName("synctest"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("synctest"))),
      Set[RawlsUserRef](inBothUser, inGraphUser),
      Set[RawlsGroupRef](inBothGroup, inGraphGroup))

    Await.result(services.gcsDAO.createGoogleGroup(topGroup), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Right(inGoogleGroup)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Right(inBothGroup)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Left(inGoogleUser)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Left(inBothUser)), Duration.Inf)

    runAndWait(rawlsUserQuery.save(inGoogleUser))
    runAndWait(rawlsUserQuery.save(inBothUser))
    runAndWait(rawlsUserQuery.save(inGraphUser))

    runAndWait(rawlsGroupQuery.save(inGoogleGroup))
    runAndWait(rawlsGroupQuery.save(inBothGroup))
    runAndWait(rawlsGroupQuery.save(inGraphGroup))

    runAndWait(rawlsGroupQuery.save(topGroup))

    Post(s"/admin/groups/${topGroup.groupName.value}/sync") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
        responseAs[SyncReport].items should contain theSameElementsAs
          Seq(
            SyncReportItem("added", Option(inGraphUser), None, None),
            SyncReportItem("added", None, Option(inGraphGroup.toRawlsGroupShort), None),
            SyncReportItem("removed", Option(inGoogleUser), None, None),
            SyncReportItem("removed", None, Option(inGoogleGroup.toRawlsGroupShort), None)
          )
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
        responseStatus.statuses should contain theSameElementsAs
          Map(
            "FIRECLOUD_USER_PROXY: aBucket" -> "NOT_FOUND",
            "FIRECLOUD_USER: 123456789876543212345" -> "FOUND",
            "GOOGLE_BUCKET_WRITE: aBucket" -> "USER_CAN_WRITE",
            "GOOGLE_BUCKET: aBucket" -> "FOUND",
            "GOOGLE_GROUP: myNamespace/myWorkspace OWNER@example.com" -> "FOUND",
            "GOOGLE_GROUP: myNamespace/myWorkspace READER@example.com" -> "FOUND",
            "GOOGLE_GROUP: myNamespace/myWorkspace WRITER@example.com" -> "FOUND",
            "GOOGLE_USER_ACCESS_LEVEL: myNamespace/myWorkspace OWNER@example.com" -> "FOUND",
            "WORKSPACE_GROUP: myNamespace/myWorkspace OWNER" -> "FOUND",
            "WORKSPACE_GROUP: myNamespace/myWorkspace READER" -> "FOUND",
            "WORKSPACE_GROUP: myNamespace/myWorkspace WRITER" -> "FOUND",
            "WORKSPACE_USER_ACCESS_LEVEL" -> "OWNER"
          )
      }
  }
}
