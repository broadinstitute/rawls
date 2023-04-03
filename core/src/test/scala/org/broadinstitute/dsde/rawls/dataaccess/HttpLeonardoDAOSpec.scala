package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class HttpLeonardoDAOSpec
  extends TestKit(ActorSystem("HttpLeonardoDAOSpec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val mockServer = RemoteServicesMockServer()

  val apiClient = new ApiClient()

  val workspaceId: UUID = UUID.randomUUID()

  val testConf: Config = ConfigFactory.load()
  val leonardoConfig: LeonardoConfig = LeonardoConfig.apply(testConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    Await.result(system.terminate(), Duration.Inf)
    super.afterAll()
  }

  "HttpLeonardoDAO" should "get an AppsV2Api object during app creation" in {
    val dao = new MockLeonardoDAO(leonardoConfig.baseUrl, "CROMWELL")
    assertResult(new AppsV2Api(apiClient)) {
      dao.getAppsV2leonardoApi("token")
    }
  }

  it should "call Leonardo createAppV2 endpoint during app creation" in {
    val dao = new MockLeonardoDAO(mockServer.mockServerBaseUrl, "CROMWELL")
    // This isn't a real unit test, just an example to try to mock out a Leo call...
    assertResult() {
      dao.createApp(
        "token",
        workspaceId,
        "something-fun", "CROMWELL")
    }
  }

    it should "call createApp when createWDSInstance is called" in {
      val dao = new MockLeonardoDAO(mockServer.mockServerBaseUrl, "CROMWELL")
      assertResult() {
        dao.createWDSInstance("token", workspaceId, "hello-app-name")
      }
    }

}