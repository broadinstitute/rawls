package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

/**
 * Created by dvoet on 4/24/15.
 */
class UserApiServiceSpec extends ApiServiceSpec {
  case class TestApiService(dataSource: SlickDataSource,
                            user: RawlsUser,
                            gcsDAO: MockGoogleServicesDAO,
                            gpsDAO: MockGooglePubSubDAO
  )(implicit val executionContext: ExecutionContext)
      extends ApiServices
      with MockUserInfoDirectives {
    override def userInfo = UserInfo(user.userEmail, OAuth2BearerToken("token"), 0, user.userSubjectId)
  }

  def withApiServices[T](dataSource: SlickDataSource, user: RawlsUser = RawlsUser(userInfo))(
    testCode: TestApiService => T
  ): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  val testConf = ConfigFactory.load()

  "UserApi" should "get a valid billing project status" in withTestDataApiServices { services =>
    val projectStatus = RawlsBillingProjectStatus(testData.billingProject.projectName, CreationStatuses.Ready)
    Get(s"/user/billing/${projectStatus.projectName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
        import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectStatusFormat
        assertResult(projectStatus)(responseAs[RawlsBillingProjectStatus])
      }
  }

  it should "return the list of billing accounts the user has access to" in withTestDataApiServices { services =>
    Get("/user/billingAccounts") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)

        import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingAccountFormat
        responseAs[List[RawlsBillingAccount]] should contain theSameElementsInOrderAs List(
          RawlsBillingAccount(services.gcsDAO.accessibleBillingAccountName, true, "testBillingAccount"),
          RawlsBillingAccount(services.gcsDAO.inaccessibleBillingAccountName, false, "testBillingAccount")
        )
      }
  }

  it should "filter billing accounts when firecloudHasAccess is specified as false " in withTestDataApiServices {
    services =>
      Get("/user/billingAccounts?firecloudHasAccess=false") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK)(status)

          import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingAccountFormat
          responseAs[List[RawlsBillingAccount]] should contain theSameElementsInOrderAs List(
            RawlsBillingAccount(services.gcsDAO.inaccessibleBillingAccountName, false, "testBillingAccount")
          )
        }
  }

  it should "filter billing accounts when firecloudHasAccess is specified as true " in withTestDataApiServices {
    services =>
      Get("/user/billingAccounts?firecloudHasAccess=true") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK)(status)

          import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingAccountFormat
          responseAs[List[RawlsBillingAccount]] should contain theSameElementsInOrderAs List(
            RawlsBillingAccount(services.gcsDAO.accessibleBillingAccountName, true, "testBillingAccount")
          )
        }
  }

  it should "fail to get an invalid billing project status" in withTestDataApiServices { services =>
    Get("/user/billing/not-found-project-name") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound)(status)
      }
  }

  it should "list a user's billing projects ordered a-z" in withTestDataApiServices { services =>
    Get("/user/billing") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
        responseAs[List[RawlsBillingProjectMembership]] should contain theSameElementsInOrderAs List(
          RawlsBillingProjectMembership(testData.testProject1.projectName, ProjectRoles.Owner, CreationStatuses.Ready),
          RawlsBillingProjectMembership(testData.billingProject.projectName,
                                        ProjectRoles.Owner,
                                        CreationStatuses.Ready
          ),
          RawlsBillingProjectMembership(testData.testProject2.projectName, ProjectRoles.Owner, CreationStatuses.Ready),
          RawlsBillingProjectMembership(testData.testProject3.projectName, ProjectRoles.Owner, CreationStatuses.Ready)
        )
      }
  }

  it should "get 400 when workspace exists" in withTestDataApiServices { services =>
    // Before test, verify there is a workspace exists for this billing project
    runAndWait(workspaceQuery.countByNamespace(testData.billingProject.projectName)) should be > 0

    Delete(s"/user/billing/${testData.billingProject.projectName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest)(status)
        val responseString = Unmarshal(response.entity).to[String].futureValue
        assert(responseString.contains("Project cannot be deleted because it contains workspaces."))
      }
  }

  it should "delete the billing project successfully" in withTestDataApiServices { services =>
    // No workspace exists for this billing project
    runAndWait(workspaceQuery.countByNamespace(testData.testProject1.projectName)) shouldEqual 0

    Delete(s"/user/billing/${testData.testProject1.projectName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent)(status)
        runAndWait(rawlsBillingProjectQuery.load(testData.testProject1.projectName)) shouldBe empty
      }
  }

  it should "return OK for a user who is an admin" in withTestDataApiServices { services =>
    Get("/user/role/admin") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return Not Found for a user who is not an admin" in withTestDataApiServices { services =>
    assertResult(())(Await.result(services.gcsDAO.removeAdmin(services.user.userEmail.value), Duration.Inf))
    Get("/user/role/admin") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return OK for a user who is a curator" in withTestDataApiServices { services =>
    Get("/user/role/curator") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return Not Found for a user who is not a curator" in withTestDataApiServices { services =>
    Delete(s"/admin/user/role/curator/owner-access") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK)(status)
      }
    Get("/user/role/curator") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }
}
