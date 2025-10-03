package org.broadinstitute.dsde.rawls.dataaccess.leonardo

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, MockLeonardoDAO}
import org.broadinstitute.dsde.rawls.model.{
  GoogleProjectId,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AppStatus,
  CloudContext,
  CloudProvider,
  ClusterStatus,
  DiskStatus,
  ListAppResponse,
  ListPersistentDiskResponse,
  ListRuntimeResponse
}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{doThrow, times, verify, when, RETURNS_SMART_NULLS}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class LeonardoServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  implicit val actorSystem: ActorSystem = ActorSystem("LeonardoAppDeletionActionSpec")

  private val userInfo = UserInfo(RawlsUserEmail("owner-access"),
                                  OAuth2BearerToken("token"),
                                  123,
                                  RawlsUserSubjectId("123456789876543212345")
  )

  private val ctx = RawlsRequestContext(userInfo)

  private val googleWorkspace: Workspace = Workspace(
    "test-namespace",
    "test-name",
    UUID.randomUUID().toString,
    "aBucket",
    Some("workflow-collection"),
    new DateTime(),
    new DateTime(),
    "test",
    Map.empty
  )

  behavior of "cleanupResources"

  it should "complete successfully" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    val action = new LeonardoService(leoDAO)

    Await.result(action.cleanupResources(googleWorkspace.googleProjectId, googleWorkspace.workspaceIdAsUUID, ctx),
                 Duration.Inf
    )

    verify(leoDAO).cleanupAllResources(anyString(), ArgumentMatchers.eq(googleWorkspace.googleProjectId))
  }

  it should "retry on 5xx" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    val action = new LeonardoService(leoDAO)

    doThrow(new ApiException(StatusCodes.BadGateway.intValue, "failed"))
      .doNothing()
      .when(leoDAO)
      .cleanupAllResources(anyString(), any[GoogleProjectId])

    Await.result(action.cleanupResources(googleWorkspace.googleProjectId, googleWorkspace.workspaceIdAsUUID, ctx),
                 Duration.Inf
    )

    verify(leoDAO, times(2)).cleanupAllResources(anyString(), ArgumentMatchers.eq(googleWorkspace.googleProjectId))
  }

  it should "complete successfully on 404" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    val action = new LeonardoService(leoDAO)

    doThrow(new ApiException(StatusCodes.NotFound.intValue, "not found"))
      .when(leoDAO)
      .cleanupAllResources(anyString(), any[GoogleProjectId])

    Await.result(action.cleanupResources(googleWorkspace.googleProjectId, googleWorkspace.workspaceIdAsUUID, ctx),
                 Duration.Inf
    )

    verify(leoDAO).cleanupAllResources(anyString(), ArgumentMatchers.eq(googleWorkspace.googleProjectId))
  }

  it should "fail on other 4xx" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    val action = new LeonardoService(leoDAO)

    doThrow(new ApiException(StatusCodes.ImATeapot.intValue, "teapot"))
      .when(leoDAO)
      .cleanupAllResources(anyString(), any[GoogleProjectId])

    intercept[ApiException] {
      Await.result(action.cleanupResources(googleWorkspace.googleProjectId, googleWorkspace.workspaceIdAsUUID, ctx),
                   Duration.Inf
      )
    }
  }

  behavior of "listRunningApps"

  it should "list running apps" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listApps(token: String, googleProjectId: GoogleProjectId): Seq[ListAppResponse] =
        Seq(
          new ListAppResponse().status(AppStatus.PROVISIONING),
          new ListAppResponse().status(AppStatus.STARTING),
          new ListAppResponse().status(AppStatus.RUNNING),
          new ListAppResponse().status(AppStatus.DELETING),
          new ListAppResponse().status(AppStatus.DELETED)
        )
    })

    val action = new LeonardoService(leoDAO)
    val result = Await.result(action.listRunningApps(googleWorkspace, ctx), Duration.Inf)
    result.size shouldBe 4
    result.map(_.getStatus) shouldBe Seq(AppStatus.PROVISIONING,
                                         AppStatus.STARTING,
                                         AppStatus.RUNNING,
                                         AppStatus.DELETING
    )
    verify(leoDAO).listApps(anyString(), ArgumentMatchers.eq(googleWorkspace.googleProjectId))
  }

  it should "not list stopped or deleted apps" in {
    val runningAppResponse = new ListAppResponse().status(AppStatus.DELETED)
    val stoppedAppResponse = new ListAppResponse().status(AppStatus.STOPPED)
    val statusUnspecifiedAppResponse = new ListAppResponse().status(AppStatus.STATUS_UNSPECIFIED)
    val errorAppResponse = new ListAppResponse().status(AppStatus.ERROR)
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listApps(token: String, googleProjectId: GoogleProjectId): Seq[ListAppResponse] =
        Seq(runningAppResponse, stoppedAppResponse, statusUnspecifiedAppResponse, errorAppResponse)
    })

    val action = new LeonardoService(leoDAO)
    val result = Await.result(action.listRunningApps(googleWorkspace, ctx), Duration.Inf)
    result.size shouldBe 0
    verify(leoDAO).listApps(anyString(), ArgumentMatchers.eq(googleWorkspace.googleProjectId))
  }

  behavior of "listRunningRuntimes"

  it should "list running runtimes" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listRuntimes(token: String, googleProjectId: GoogleProjectId): Seq[ListRuntimeResponse] = {
        val statuses = Seq(
          ClusterStatus.CREATING,
          ClusterStatus.RUNNING,
          ClusterStatus.UPDATING,
          ClusterStatus.STARTING,
          ClusterStatus.STOPPING,
          ClusterStatus.DELETING,
          ClusterStatus.DELETED
        )
        statuses.map { status =>
          new ListRuntimeResponse()
            .status(status)
            .workspaceId(googleWorkspace.workspaceId)
        }
      }
    })

    val action = new LeonardoService(leoDAO)
    val result = Await.result(action.listRunningRuntimes(googleWorkspace, ctx), Duration.Inf)
    result.size shouldBe 6
    result.map(_.getStatus) shouldBe Seq(ClusterStatus.CREATING,
                                         ClusterStatus.RUNNING,
                                         ClusterStatus.UPDATING,
                                         ClusterStatus.STARTING,
                                         ClusterStatus.STOPPING,
                                         ClusterStatus.DELETING
    )
    verify(leoDAO).listRuntimes(anyString(), ArgumentMatchers.eq(googleWorkspace.googleProjectId))
  }

  it should "not list stopped or deleted runtimes" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listRuntimes(token: String, googleProjectId: GoogleProjectId): Seq[ListRuntimeResponse] = {
        val statuses = Seq(ClusterStatus.ERROR, ClusterStatus.STOPPED, ClusterStatus.DELETED, ClusterStatus.UNKNOWN)
        statuses.map { status =>
          new ListRuntimeResponse()
            .status(status)
            .workspaceId(googleWorkspace.workspaceId)
        }
      }
    })
    val action = new LeonardoService(leoDAO)
    val result = Await.result(action.listRunningRuntimes(googleWorkspace, ctx), Duration.Inf)
    result.size shouldBe 0
    verify(leoDAO).listRuntimes(anyString(), ArgumentMatchers.eq(googleWorkspace.googleProjectId))
  }

  behavior of "listRunningDisks"

  it should "list running disks" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listDisks(token: String, googleProjectId: GoogleProjectId): Seq[ListPersistentDiskResponse] = {
        val cloudContext = new CloudContext()
          .cloudProvider(CloudProvider.GCP)
          .cloudResource(googleWorkspace.googleProjectId.value);
        val statuses = Seq(
          DiskStatus.CREATING,
          DiskStatus.READY,
          DiskStatus.RESTORING,
          DiskStatus.DELETING,
          DiskStatus.DELETED
        )
        statuses.map { status =>
          new ListPersistentDiskResponse()
            .status(status)
            .cloudContext(cloudContext)
        }
      }
    })

    val action = new LeonardoService(leoDAO)
    val result = Await.result(action.listRunningDisks(googleWorkspace, ctx), Duration.Inf)
    result.size shouldBe 4
    result.map(_.getStatus) shouldBe Seq(DiskStatus.CREATING,
                                         DiskStatus.READY,
                                         DiskStatus.RESTORING,
                                         DiskStatus.DELETING
    )
    verify(leoDAO).listDisks(any[String], any[GoogleProjectId]);
  }

  it should "not list stopped or deleted disks" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listDisks(token: String, googleProjectId: GoogleProjectId): Seq[ListPersistentDiskResponse] = {
        val cloudContext = new CloudContext()
          .cloudProvider(CloudProvider.GCP)
          .cloudResource(googleWorkspace.googleProjectId.value);
        val statuses = Seq(DiskStatus.FAILED, DiskStatus.DELETED)
        statuses.map { status =>
          new ListPersistentDiskResponse()
            .status(status)
            .cloudContext(cloudContext)
        }
      }
    })
    val action = new LeonardoService(leoDAO)
    val result = Await.result(action.listRunningDisks(googleWorkspace, ctx), Duration.Inf)
    result.size shouldBe 0
    verify(leoDAO).listDisks(any[String], any[GoogleProjectId])
  }

}
