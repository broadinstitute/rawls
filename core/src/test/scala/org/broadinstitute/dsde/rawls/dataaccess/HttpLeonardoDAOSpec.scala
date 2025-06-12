package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AppAccessScope,
  AppType,
  CreateAppRequest,
  ListPersistentDiskResponse,
  ListRuntimeResponse,
  UpdateDiskRequest,
  UpdateRuntimeRequest
}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.UUID
import scala.util.Try
import scala.jdk.CollectionConverters._

class HttpLeonardoDAOSpec extends TestKit(ActorSystem("HttpLeonardoDAOSpec")) with AnyFlatSpecLike {

  val testConf: Config =
    ConfigFactory
      .parseMap(Map("wdsType" -> "WDS", "server" -> "http://localhost").asJava)
      .resolve()
  val leonardoConfig: LeonardoConfig = LeonardoConfig.apply(testConf)

  val token: String = "my-token"

  behavior of "HttpLeonardoDAO.createWDSInstance()"

  it should "create the correct app name and type" in {
    val workspaceId: UUID = UUID.randomUUID()

    val leonardoDAO = Mockito.spy(
      new HttpLeonardoDAO(leonardoConfig)
    )

    // this call will fail in createApp() because it's looking for a server running on localhost.
    // but we can still verify the arguments it passes to createApp()
    // alternative is to spin up a mockserver we can hit for a 2xx response.
    Try(leonardoDAO.createWDSInstance(token, workspaceId))

    Mockito
      .verify(leonardoDAO)
      .createApp(
        ArgumentMatchers.eq(token),
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(s"wds-$workspaceId"),
        ArgumentMatchers.eq("WDS"),
        ArgumentMatchers.eq(None)
      )
  }

  it should "call the createApp API with source workspace id" in {
    val sourceWorkspaceId: UUID = UUID.randomUUID()

    val leonardoDAO = new HttpLeonardoDAO(leonardoConfig)

    val createAppRequest: CreateAppRequest = leonardoDAO.buildAppRequest("CROMWELL", Some(sourceWorkspaceId))
    val expectedAppRequest: CreateAppRequest = new CreateAppRequest()
    expectedAppRequest.setAppType(AppType.CROMWELL)
    expectedAppRequest.setSourceWorkspaceId(sourceWorkspaceId.toString)
    expectedAppRequest.setAccessScope(AppAccessScope.WORKSPACE_SHARED)

    assertResult(expectedAppRequest)(createAppRequest)

  }

  it should "call the listApps API with source workspace id" in {
    val workspaceId = UUID.randomUUID()
    val leonardoDAO = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))

    Try(leonardoDAO.listApps(token, workspaceId))

    Mockito
      .verify(leonardoDAO)
      .listApps(ArgumentMatchers.eq(token), ArgumentMatchers.eq(workspaceId))

  }

  it should "call the deleteApps API with source workspace id" in {
    val workspaceId = UUID.randomUUID()
    val leonardoDAO = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))

    Try(leonardoDAO.deleteApps(token, workspaceId, deleteDisk = true))

    Mockito
      .verify(leonardoDAO)
      .deleteApps(ArgumentMatchers.eq(token), ArgumentMatchers.eq(workspaceId), ArgumentMatchers.eq(true))

  }

  it should "call the listAzureRuntimes API with source workspace id" in {
    val workspaceId = UUID.randomUUID()
    val leonardoDAO = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))

    Try(leonardoDAO.listAzureRuntimes(token, workspaceId))

    Mockito
      .verify(leonardoDAO)
      .listAzureRuntimes(ArgumentMatchers.eq(token), ArgumentMatchers.eq(workspaceId))

  }

  it should "call the deleteAzureRuntimes API with source workspace id" in {
    val workspaceId = UUID.randomUUID()
    val leonardoDAO = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))

    Try(leonardoDAO.deleteAzureRuntimes(token, workspaceId, deleteDisk = true))

    Mockito
      .verify(leonardoDAO)
      .deleteAzureRuntimes(ArgumentMatchers.eq(token), ArgumentMatchers.eq(workspaceId), ArgumentMatchers.eq(true))

  }

  "HttpLeonardoDAO.listRuntimesByWorkspace" should "call the correct API with workspaceId" in {
    val workspaceId = UUID.randomUUID()
    val dao = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))
    Mockito.doReturn(Seq.empty[ListRuntimeResponse]).when(dao).listRuntimesByWorkspace(token, workspaceId)
    dao.listRuntimesByWorkspace(token, workspaceId)
    Mockito.verify(dao).listRuntimesByWorkspace(ArgumentMatchers.eq(token), ArgumentMatchers.eq(workspaceId))
  }

  "HttpLeonardoDAO.updateRuntimeConfig" should "call the correct API with arguments" in {
    val googleProject = "terra-12345"
    val name = "runtime"
    val req = new UpdateRuntimeRequest()
    val dao = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))
    Mockito.doNothing().when(dao).updateRuntimeConfig(token, googleProject, name, req)
    dao.updateRuntimeConfig(token, googleProject, name, req)
    Mockito.verify(dao).updateRuntimeConfig(token, googleProject, name, req)
  }

  "HttpLeonardoDAO.listDisksByWorkspaceNamespace" should "call the correct API with workspaceNamespace" in {
    val workspaceNamespace = "namespace"
    val dao = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))
    Mockito
      .doReturn(Seq.empty[ListPersistentDiskResponse])
      .when(dao)
      .listDisksByWorkspaceNamespace(token, workspaceNamespace)
    dao.listDisksByWorkspaceNamespace(token, workspaceNamespace)
    Mockito
      .verify(dao)
      .listDisksByWorkspaceNamespace(ArgumentMatchers.eq(token), ArgumentMatchers.eq(workspaceNamespace))
  }

  "HttpLeonardoDAO.updateDiskConfig" should "call the correct API with arguments" in {
    val googleProject = "terra-12345"
    val name = "disk"
    val req = new UpdateDiskRequest()
    val dao = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))
    Mockito.doNothing().when(dao).updateDiskConfig(token, googleProject, name, req)
    dao.updateDiskConfig(token, googleProject, name, req)
    Mockito.verify(dao).updateDiskConfig(token, googleProject, name, req)
  }

}
