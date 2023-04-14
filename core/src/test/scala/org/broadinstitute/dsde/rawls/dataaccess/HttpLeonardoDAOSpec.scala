package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.UUID
import scala.util.Try

import scala.jdk.CollectionConverters._

class HttpLeonardoDAOSpec extends TestKit(ActorSystem("HttpLeonardoDAOSpec")) with AnyFlatSpecLike {

  val testConf: Config =
    ConfigFactory
      .parseMap(Map("wdsType" -> "CROMWELL", "server" -> "http://localhost").asJava)
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
        ArgumentMatchers.eq("CROMWELL")
      )
  }

}
