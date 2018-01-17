package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectOperationRecord, RawlsBillingProjectRecord, ReadAction}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ActiveSubmissionFormat
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.scalatest.path
import spray.http.{HttpMethods, StatusCodes, Uri}
import spray.json.DefaultJsonProtocol._
import spray.routing.Route

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class BillingApiServiceSpec extends ApiServiceSpec {
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test"))(testCode: TestApiService =>  T): T = {
    val apiService = new TestApiService(dataSource, gcsDAO, new MockGooglePubSubDAO)
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

  def createProject(project: RawlsBillingProject, owner: RawlsUser = testData.userOwner): Unit = {
    import driver.api._
    val projectWithOwner = project.copy()

    runAndWait(rawlsBillingProjectQuery.create(projectWithOwner))
  }

  "BillingApiService" should "return 200 when adding a user to a billing project" in withTestDataApiServices { services =>
    val (project, projectGroups) = billingProjectFromName("new_project")
    Await.result(samDataSaver.savePolicyGroups(projectGroups.values.flatten, SamResourceTypeNames.billingProject.value, project.projectName.value), Duration.Inf)

    val createRequest = CreateRawlsBillingProjectFullRequest(project.projectName, services.gcsDAO.accessibleBillingAccountName)

    import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

    Post(s"/billing", httpJson(createRequest)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Put(s"/billing/${project.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Put(s"/billing/${project.projectName.value}/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 403 when adding a user to a non-owned billing project" in withTestDataApiServices { services =>
    val (project, projectGroups) = billingProjectFromName("no_access")

    withStatsD {
      Put(s"/billing/${project.projectName.value}/user/${testData.userReader.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }

      Put(s"/billing/${project.projectName.value}/owner/${testData.userReader.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("put", s"billing.redacted.redacted.redacted", StatusCodes.Forbidden.intValue, 2)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 404 when adding a nonexistent user to a billing project" in withTestDataApiServices { services =>
    val (project, projectGroups) = billingProjectFromName("no_access")

    Put(s"/billing/${project.projectName.value}/nobody") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 403 when adding a user to a nonexistent project" in withTestDataApiServices { services =>
    Put(s"/billing/no_access/user/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 200 when removing a user from a billing project" in withTestDataApiServices { services =>
    val (project, projectGroups) = billingProjectFromName("new_project")
    Await.result(samDataSaver.savePolicyGroups(projectGroups.values.flatten, SamResourceTypeNames.billingProject.value, project.projectName.value), Duration.Inf)

    withStatsD {
      val createRequest = CreateRawlsBillingProjectFullRequest(project.projectName, services.gcsDAO.accessibleBillingAccountName)

      import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

      Post(s"/billing", httpJson(createRequest)) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      Put(s"/billing/${project.projectName.value}/user/${testData.userWriter.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
        }

      Delete(s"/billing/${project.projectName.value}/user/${testData.userWriter.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("put", s"billing.redacted.redacted.redacted", StatusCodes.OK.intValue, 1) ++
        expectedHttpRequestMetrics("delete", s"billing.redacted.redacted.redacted", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 403 when removing a user from a non-owned billing project" in withTestDataApiServices { services =>
    val (project, projectGroups) = billingProjectFromName("no_access")

    Delete(s"/billing/${project.projectName.value}/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 400 when removing a nonexistent user from a billing project" in withTestDataApiServices { services =>
    val (project, projectGroups) = billingProjectFromName("test_good")

    Delete(s"/billing/${project.projectName.value}/user/nobody") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 403 when removing a user from a nonexistent billing project" in withTestDataApiServices { services =>
    Delete(s"/billing/missing_project/user/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 204 when creating a project with accessible billing account" in withTestDataApiServices { services =>
    val projectName = RawlsBillingProjectName("test_good")

    Post("/billing", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }

        import driver.api._
        val query: ReadAction[Seq[RawlsBillingProjectRecord]] = rawlsBillingProjectQuery.filter(_.projectName === projectName.value).result
        val projects: Seq[RawlsBillingProjectRecord] = runAndWait(query)
        projects match {
          case Seq() => fail("project does not exist in db")
          case Seq(project) =>
            assertResult("gs://" + services.gcsDAO.getCromwellAuthBucketName(projectName)) {
              project.cromwellAuthBucketUrl
            }
          case _ => fail("too many projects")
        }
      }
  }

  it should "rollback billing project inserts when there is a google error" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource, new MockGoogleServicesDAO("test") {
      override def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount): Future[RawlsBillingProjectOperationRecord] = {
        Future.failed(new Exception("test exception"))
      }
    }) { services =>
      val projectName = RawlsBillingProjectName("test_good2")

      Post("/billing", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName)) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.InternalServerError) {
            status
          }
          runAndWait(rawlsBillingProjectQuery.load(projectName)) map { p => fail("did not rollback project inserts: " + p) }
        }
    }
  }

  it should "return 400 when creating a project with inaccessible to firecloud billing account" in withTestDataApiServices { services =>
    Post("/billing", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_bad1"), services.gcsDAO.inaccessibleBillingAccountName)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 403 when creating a project with inaccessible to user billing account" in withTestDataApiServices { services =>
    Post("/billing", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_bad1"), RawlsBillingAccountName("this does not exist"))) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 200 when listing billing project members as owner" in withTestDataApiServices { services =>
    val (project, projectGroups) = billingProjectFromName("test_good")

    Get(s"/billing/${project.projectName.value}/members") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Seq(RawlsBillingProjectMember(testData.userOwner.userEmail, ProjectRoles.Owner))) {
          responseAs[Seq[RawlsBillingProjectMember]]
        }
      }
  }

  it should "return 403 when listing billing project members as non-owner" in withTestDataApiServices { services =>
    val (project, projectGroups) = billingProjectFromName("no_access")

    Get(s"/billing/${project.projectName.value}/members") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }
}
