package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.tinkerpop.blueprints.{Vertex, Graph}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ActiveSubmissionFormat
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectNameFormat
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{Matchers, FlatSpec}
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Created by tsharpe on 9/28/15.
 */
class AdminApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture with SprayJsonSupport {
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  def actorRefFactory = system

  val mockServer = RemoteServicesMockServer()

  override def beforeAll() = {
    super.beforeAll
    mockServer.startServer
  }

  override def afterAll() = {
    super.afterAll
    mockServer.stopServer
  }

  case class TestApiService(dataSource: DataSource)(implicit val executionContext: ExecutionContext) extends AdminApiService with MockUserInfoDirectives {
    def actorRefFactory = system

    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource,
      gcsDAO
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")

    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, containerDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), gcsDAO, submissionSupervisor)_
    val directoryDAO = new MockUserDirectoryDAO
    val userServiceConstructor = UserService.constructor(dataSource, gcsDAO, containerDAO, directoryDAO)_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  def withApiServices(dataSource: DataSource)(testCode: TestApiService => Any): Unit = {
    val apiService = new TestApiService(dataSource)
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
}
