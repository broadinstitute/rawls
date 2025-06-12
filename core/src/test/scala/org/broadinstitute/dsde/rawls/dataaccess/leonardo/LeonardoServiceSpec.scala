package org.broadinstitute.dsde.rawls.dataaccess.leonardo

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.google.gson.Gson
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
import org.broadinstitute.dsde.workbench.client.leonardo.{ApiException, JSON}
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AppStatus,
  CloudContext,
  ClusterStatus,
  ListAppResponse,
  ListPersistentDiskResponse,
  ListRuntimeResponse,
  UpdateDiskRequest,
  UpdateRuntimeRequest
}
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{atLeastOnce, doNothing, doThrow, spy, times, verify, when, RETURNS_SMART_NULLS}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class LeonardoServiceSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext
  implicit val actorSystem: ActorSystem = ActorSystem("LeonardoAppDeletionActionSpec")

  private val userInfo = UserInfo(RawlsUserEmail("owner-access"),
                                  OAuth2BearerToken("token"),
                                  123,
                                  RawlsUserSubjectId("123456789876543212345")
  )

  private val ctx = RawlsRequestContext(userInfo)

  private val azureWorkspace: Workspace = Workspace.buildReadyMcWorkspace(
    "fake_azure_bp",
    "fake_ws",
    UUID.randomUUID().toString,
    DateTime.now(),
    DateTime.now(),
    "example@example.com",
    Map.empty
  )

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

  val workspaceId: UUID = googleWorkspace.workspaceIdAsUUID

  val googleProjectId = "terra-12345"

  val oldNamespace: String = googleWorkspace.namespace

  val newNamespace = "new-ns"

  behavior of "deleteApps"

  it should "start app deletion" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    val action = new LeonardoService(leoDAO)

    Await.result(action.deleteApps(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO).deleteApps(anyString(), any[UUID], ArgumentMatchers.eq(true))
  }

  it should "retry on 5xx from leo on app deletion" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      var times = 0

      override def deleteApps(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit = {
        times = times + 1

        if (times <= 1) {
          throw new ApiException(StatusCodes.InternalServerError.intValue, "failed")
        }
      }
    })
    val action = new LeonardoService(leoDAO)

    Await.result(action.deleteApps(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO, times(2)).deleteApps(anyString(), any[UUID], ArgumentMatchers.eq(true))
  }

  behavior of "deleteRuntimes"

  it should "start runtime deletion" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    val action = new LeonardoService(leoDAO)

    Await.result(action.deleteRuntimes(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO).deleteAzureRuntimes(anyString(), any[UUID], ArgumentMatchers.eq(true))
  }

  it should "retry on 5xx from leo on runtime deletion" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      var times = 0

      override def deleteAzureRuntimes(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit = {
        times = times + 1

        if (times <= 1) {
          throw new ApiException(StatusCodes.InternalServerError.intValue, "failed")
        }
      }
    })
    val action = new LeonardoService(leoDAO)

    Await.result(action.deleteRuntimes(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO, times(2)).deleteAzureRuntimes(anyString(), any[UUID], ArgumentMatchers.eq(true))
  }

  behavior of "pollAppDeletion"

  it should "poll and return false when apps have not finished deleting and are not all in the error state" in {
    val deletingAppResponse = new ListAppResponse()
    deletingAppResponse.setStatus(AppStatus.DELETING)
    val erroredAppResponse = new ListAppResponse()
    erroredAppResponse.setStatus(AppStatus.ERROR)
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse] =
        Seq(deletingAppResponse, erroredAppResponse)
    })

    val action = new LeonardoService(leoDAO)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf) shouldBe false
    verify(leoDAO).listApps(anyString(), any[UUID])
  }

  it should "poll and return true when all apps are in the error state" in {
    val erroredAppResponse = new ListAppResponse()
    erroredAppResponse.setStatus(AppStatus.ERROR)
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse] =
        Seq(erroredAppResponse, erroredAppResponse)
    })

    val action = new LeonardoService(leoDAO)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf) shouldBe true
    verify(leoDAO).listApps(anyString(), any[UUID])
  }

  it should "poll and return true when apps have finished deleting" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse] = Seq.empty
    })

    val action = new LeonardoService(leoDAO)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf) shouldBe true
    verify(leoDAO).listApps(anyString(), any[UUID])
  }

  it should "retry on 5xx from listapps" in {
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      var times = 0

      override def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse] = {
        times = times + 1
        if (times > 1) {
          Seq.empty
        } else {
          throw new ApiException(StatusCodes.InternalServerError.intValue, "failed")
        }
      }
    })
    val action = new LeonardoService(leoDAO)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)

    verify(leoDAO, times(2)).listApps(anyString(), any[UUID])
  }

  it should "complete successfully on 403 forbidden when listing apps" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    when(leoDAO.listApps(anyString(), any[UUID])).thenAnswer(_ =>
      throw new ApiException(StatusCodes.Forbidden.intValue, "forbidden")
    )
    val action = new LeonardoService(leoDAO)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)
  }

  it should "complete successfully on 404 not found when listing apps" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    when(leoDAO.listApps(anyString(), any[UUID])).thenAnswer(_ =>
      throw new ApiException(StatusCodes.NotFound.intValue, "not found")
    )
    val action = new LeonardoService(leoDAO)

    Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)
  }

  it should "fail on other 4xx when listing apps" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    when(leoDAO.listApps(anyString(), any[UUID])).thenAnswer(_ =>
      throw new ApiException(StatusCodes.ImATeapot.intValue, "teapot")
    )
    val action = new LeonardoService(leoDAO)

    intercept[ApiException] {
      Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)
    }
  }

  it should "not retry on an unrelated exception" in {
    val leoDAO = mock[LeonardoDAO](RETURNS_SMART_NULLS)
    when(leoDAO.listApps(anyString(), any[UUID])).thenAnswer(_ => throw new IllegalStateException("exception"))
    val action = new LeonardoService(leoDAO)

    intercept[IllegalStateException] {
      Await.result(action.pollAppDeletion(azureWorkspace, ctx), Duration.Inf)
    }
  }

  it should "poll and return false when runtimes have not finished deleting and are not all in the error state" in {
    val deletingRuntimeResponse = new ListRuntimeResponse()
    deletingRuntimeResponse.setStatus(ClusterStatus.DELETING)
    val erroredRuntimeResponse = new ListRuntimeResponse()
    erroredRuntimeResponse.setStatus(ClusterStatus.ERROR)
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listAzureRuntimes(token: String, workspaceId: UUID): Seq[ListRuntimeResponse] =
        Seq(deletingRuntimeResponse, erroredRuntimeResponse)
    })

    val action = new LeonardoService(leoDAO)

    Await.result(action.pollRuntimeDeletion(azureWorkspace, ctx), Duration.Inf) shouldBe false
    verify(leoDAO).listAzureRuntimes(anyString(), any[UUID])
  }

  it should "poll and return true when all runtimes are in the error state" in {
    val erroredRuntimeResponse = new ListRuntimeResponse()
    erroredRuntimeResponse.setStatus(ClusterStatus.ERROR)
    val leoDAO: MockLeonardoDAO = Mockito.spy(new MockLeonardoDAO() {
      override def listAzureRuntimes(token: String, workspaceId: UUID): Seq[ListRuntimeResponse] =
        Seq(erroredRuntimeResponse, erroredRuntimeResponse)
    })

    val action = new LeonardoService(leoDAO)

    Await.result(action.pollRuntimeDeletion(azureWorkspace, ctx), Duration.Inf) shouldBe true
    verify(leoDAO).listAzureRuntimes(anyString(), any[UUID])
  }

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

  behavior of "update labels"

  "updateRuntimeLabels" should "update workspaceNamespace labels for all workspace runtimes" in {
    val dao = mock[LeonardoDAO]
    val runtime = new ListRuntimeResponse()
    runtime.setGoogleProject(googleProjectId)
    runtime.setRuntimeName("runtime1")
    when(dao.listRuntimesByWorkspace(anyString(), any[UUID])).thenReturn(Seq(runtime))
    val updateRuntimeRequest = new UpdateRuntimeRequest()
    updateRuntimeRequest.setLabelsToUpsert(Map("saturnWorkspaceNamespace" -> newNamespace))
    doNothing().when(dao).updateRuntimeConfig("token", googleProjectId, "runtime1", updateRuntimeRequest)

    val service = new LeonardoService(dao)
    service.updateRuntimeLabels(workspaceId, newNamespace, ctx)
    verify(dao).listRuntimesByWorkspace("token", workspaceId)
    verify(dao).updateRuntimeConfig("token", googleProjectId, "runtime1", updateRuntimeRequest)
  }

  "updateDiskLabels" should "update workspaceNamespace labels for all workspace disks" in {
    JSON.setGson(new Gson())
    val dao = mock[LeonardoDAO]
    val disk = new ListPersistentDiskResponse()
    disk.setName("disk1")
    val cloudContext = new CloudContext()
    cloudContext.setCloudResource(googleProjectId)
    disk.setCloudContext(cloudContext)
    when(dao.listDisksByWorkspaceNamespace(anyString(), anyString())).thenReturn(Seq(disk))
    doNothing().when(dao).updateDiskConfig(anyString(), anyString(), anyString(), any[UpdateDiskRequest])

    val service = new LeonardoService(dao)
    service.updateDiskLabels(workspaceId, oldNamespace, newNamespace, ctx)

    verify(dao).listDisksByWorkspaceNamespace("token", oldNamespace)
    verify(dao).updateDiskConfig(ArgumentMatchers.eq("token"),
                                 ArgumentMatchers.eq(googleProjectId),
                                 ArgumentMatchers.eq("disk1"),
                                 any[UpdateDiskRequest]
    )
  }

  "updateResourceLabelsOrRevert" should "update labels successfully" in {
    val dao = mock[LeonardoDAO]
    val service = spy(new LeonardoService(dao))
    doNothing().when(service).updateRuntimeLabels(workspaceId, newNamespace, ctx)
    doNothing().when(service).updateDiskLabels(workspaceId, oldNamespace, newNamespace, ctx)

    Await.result(service.updateResourceLabelsOrRevert(workspaceId, oldNamespace, newNamespace, ctx), Duration.Inf)

    verify(service).updateRuntimeLabels(workspaceId, newNamespace, ctx)
    verify(service).updateDiskLabels(workspaceId, oldNamespace, newNamespace, ctx)
  }

  it should "revert labels if update fails" in {
    JSON.setGson(new Gson())
    val dao = mock[LeonardoDAO]
    val service = spy(new LeonardoService(dao))
    when(dao.listRuntimesByWorkspace(anyString(), any[UUID])).thenAnswer(_ =>
      throw new ApiException(StatusCodes.Forbidden.intValue, "failed to list runtimes")
    )
    when(dao.listDisksByWorkspaceNamespace(anyString(), anyString())).thenAnswer(_ => Seq.empty)
    when(service.updateDiskLabels(workspaceId, oldNamespace, newNamespace, ctx)(executionContext))
      .thenReturn(Future.successful(()))
    when(service.updateDiskLabels(workspaceId, newNamespace, oldNamespace, ctx)(executionContext))
      .thenReturn(Future.successful(()))

    intercept[ApiException] {
      Await.result(service.updateResourceLabelsOrRevert(workspaceId, oldNamespace, newNamespace, ctx), Duration.Inf)
    }
    verify(service).updateRuntimeLabels(workspaceId, newNamespace, ctx)
    verify(service, atLeastOnce()).updateDiskLabels(workspaceId, oldNamespace, newNamespace, ctx)

    // Revert call
    verify(service).updateRuntimeLabels(workspaceId, oldNamespace, ctx)
    verify(service, atLeastOnce()).updateDiskLabels(workspaceId, newNamespace, oldNamespace, ctx)
  }

}
