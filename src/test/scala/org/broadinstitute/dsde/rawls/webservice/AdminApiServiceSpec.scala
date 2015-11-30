package org.broadinstitute.dsde.rawls.webservice

import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.tinkerpop.blueprints.Vertex
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.http.StatusCodes
import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ActiveSubmissionFormat

/**
 * Created by tsharpe on 9/28/15.
 */
class AdminApiServiceSpec extends ApiServiceSpec {
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  case class TestApiService(dataSource: DataSource, gcsDAO: MockGoogleServicesDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices(dataSource: DataSource)(testCode: TestApiService => Any): Unit = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices(testCode: TestApiService => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  import scala.collection.JavaConversions._

  def getMatchingBillingProjectVertices(dataSource: DataSource, project: RawlsBillingProject): Iterable[Vertex] =
    dataSource.inTransaction() { txn =>
      txn.withGraph { graph =>
        graph.getVertices.filter(v => {
          v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.BillingProject) &&
            v.getProperty[String]("projectName") == project.projectName.value
        })
      }
    }

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
        services.dataSource.inTransaction() { txn =>
          assert {
            ! containerDAO.billingDAO.loadProject(project.projectName, txn).get.users.contains(testData.userOwner)
          }
        }

        Put(s"/admin/billing/${project.projectName.value}/${testData.userOwner.userEmail.value}") ~>
          sealRoute(services.adminRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            services.dataSource.inTransaction() { txn =>
              assert {
                containerDAO.billingDAO.loadProject(project.projectName, txn).get.users.contains(testData.userOwner)
              }
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
            services.dataSource.inTransaction() { txn =>
              assert {
                containerDAO.billingDAO.loadProject(project.projectName, txn).get.users.contains(testData.userOwner)
              }
            }

            Delete(s"/admin/billing/${project.projectName.value}/${testData.userOwner.userEmail.value}") ~>
              sealRoute(services.adminRoutes) ~>
              check {
                assertResult(StatusCodes.OK) {
                  status
                }
                services.dataSource.inTransaction() { txn =>
                  assert {
                    ! containerDAO.billingDAO.loadProject(project.projectName, txn).get.users.contains(testData.userOwner)
                  }
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

    services.dataSource.inTransaction() { txn =>
      containerDAO.authDAO.saveUser(testUser, txn)
    }

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
        assertResult(StatusCodes.Created) { status }
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
    Post(s"/admin/groups/${group.groupName.value}/members", httpJson(RawlsGroupMemberList(Seq.empty, Seq(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org")))) ~>
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

  it should "return 404 when adding a member that doesn't exist" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))

    Post(s"/admin/groups", httpJson(group)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Post(s"/admin/groups/${group.groupName.value}/members", httpJson(RawlsGroupMemberList(Seq.empty, Seq(s"GROUP_blahhh@dev.firecloud.org")))) ~>
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
    Post(s"/admin/groups/${group.groupName.value}/members", httpJson(RawlsGroupMemberList(Seq.empty, Seq(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org")))) ~>
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
    Delete(s"/admin/groups/${group.groupName.value}/members", httpJson(RawlsGroupMemberList(Seq.empty, Seq(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org")))) ~>
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
}
