package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.UUID
import scala.language.postfixOps

class HttpLeonardoDAOSpec extends TestKit(ActorSystem("HttpLeonardoDAOSpec")) with AnyFlatSpecLike {

  val testConf: Config =
    ConfigFactory.parseMap(java.util.Map.of("wdsType", "test-wds-type", "server", "test-leo-server"))
  val leonardoConfig: LeonardoConfig = LeonardoConfig.apply(testConf)

  val token: String = "my-token"

  behavior of "HttpLeonardoDAO.createWDSInstance()"

  it should "create the correct app name and type" in {
    val workspaceId: UUID = UUID.randomUUID()

    val leonardoDAO = Mockito.spy(
      new HttpLeonardoDAO(leonardoConfig)
    )
    leonardoDAO.createWDSInstance(token, workspaceId)
    Mockito
      .verify(leonardoDAO)
      .createApp(
        ArgumentMatchers.eq(token),
        ArgumentMatchers.eq(workspaceId),
        ArgumentMatchers.eq(s"wds-$workspaceId"),
        ArgumentMatchers.eq("test-wds-type")
      )
  }

}
