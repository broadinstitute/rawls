package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.util.Try
import scala.jdk.CollectionConverters._

class HttpLeonardoDAOSpec extends TestKit(ActorSystem("HttpLeonardoDAOSpec")) with AnyFlatSpecLike {

  val testConf: Config =
    ConfigFactory
      .parseMap(Map("wdsType" -> "WDS", "server" -> "http://localhost").asJava)
      .resolve()
  val leonardoConfig: LeonardoConfig = LeonardoConfig.apply(testConf)

  val token: String = "my-token"

  val googleProjectId: GoogleProjectId = GoogleProjectId("fake-google-project")

  it should "call the listApps API with source workspace id" in {
    val leonardoDAO = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))

    Try(leonardoDAO.listApps(token, googleProjectId))

    Mockito
      .verify(leonardoDAO)
      .listApps(ArgumentMatchers.eq(token), ArgumentMatchers.eq(googleProjectId))

  }

  it should "call the listDisks API with google project" in {
    val leonardoDAO = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))

    Try(leonardoDAO.listDisks(token, googleProjectId))

    Mockito
      .verify(leonardoDAO)
      .listDisks(ArgumentMatchers.eq(token), ArgumentMatchers.eq(googleProjectId))

  }

  it should "call the listRuntimes API with google project" in {
    val leonardoDAO = Mockito.spy(new HttpLeonardoDAO(leonardoConfig))

    Try(leonardoDAO.listRuntimes(token, googleProjectId))

    Mockito
      .verify(leonardoDAO)
      .listRuntimes(ArgumentMatchers.eq(token), ArgumentMatchers.eq(googleProjectId))

  }

}
