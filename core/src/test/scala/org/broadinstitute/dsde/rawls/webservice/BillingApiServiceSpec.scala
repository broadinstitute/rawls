package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ActiveSubmissionFormat
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import spray.http.StatusCodes
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class BillingApiServiceSpec extends ApiServiceSpec {
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

  private def createProject(services: TestApiService, project: RawlsBillingProject, owner: RawlsUser = testData.userOwner): Unit = {
    Put(s"/admin/billing/register/${project.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created, response.entity.asString) {
          status
        }
      }

    Put(s"/admin/billing/${project.projectName.value}/owner/${owner.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
      }
  }

  "BillingApiService" should "return 200 when adding a user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    createProject(services, project)

    Put(s"/billing/${project.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assert {
          val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get
          loadedProject.groups(ProjectRoles.User).users.contains(testData.userWriter) && !loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userWriter)
        }
      }

    Put(s"/billing/${project.projectName.value}/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assert {
          val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get
          loadedProject.groups(ProjectRoles.User).users.contains(testData.userWriter) && loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userWriter)
        }
      }
  }

  it should "return 403 when adding a user to a non-owned billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    createProject(services, project, testData.userWriter)

    Put(s"/billing/${project.projectName.value}/user/${testData.userReader.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
        assert {
          val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get
          !loadedProject.groups(ProjectRoles.User).users.contains(testData.userReader) && !loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userReader)
        }
      }

    Put(s"/billing/${project.projectName.value}/owner/${testData.userReader.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
        assert {
          val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get
          !loadedProject.groups(ProjectRoles.User).users.contains(testData.userReader) && !loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userReader)
        }
      }
  }

  it should "return 404 when adding a nonexistent user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    createProject(services, project)

    Put(s"/billing/${project.projectName.value}/nobody") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 403 when adding a user to a nonexistent project" in withTestDataApiServices { services =>
    Put(s"/billing/missing_project/user/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 200 when removing a user from a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")
    createProject(services, project)

    Put(s"/billing/${project.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assert {
          runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.groups(ProjectRoles.User).users.contains(testData.userWriter)
        }
      }

    Delete(s"/billing/${project.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assert {
          ! runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.groups(ProjectRoles.User).users.contains(testData.userWriter)
        }
      }
  }

  it should "return 403 when removing a user from a non-owned billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")
    createProject(services, project, testData.userWriter)

    Delete(s"/billing/${project.projectName.value}/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
        assert {
          runAndWait(rawlsBillingProjectQuery.load(project.projectName)).get.groups(ProjectRoles.Owner).users.contains(testData.userWriter)
        }
      }
  }

  it should "return 404 when removing a nonexistent user from a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")
    createProject(services, project)

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

        runAndWait(rawlsBillingProjectQuery.load(projectName)) match {
          case None => fail("project does not exist in db")
          case Some(project) =>
            assert(project.groups(ProjectRoles.User).users.isEmpty && project.groups(ProjectRoles.Owner).users.size == 1 && project.groups(ProjectRoles.Owner).users.head.userSubjectId.value == "123456789876543212345")
            assertResult("gs://" + services.gcsDAO.getCromwellAuthBucketName(projectName)) {
              project.cromwellAuthBucketUrl
            }
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
    val project = billingProjectFromName("new_project")

    createProject(services, project)

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
    val project = billingProjectFromName("new_project")

    createProject(services, project, testData.userWriter)

    Get(s"/billing/${project.projectName.value}/members") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }
}
