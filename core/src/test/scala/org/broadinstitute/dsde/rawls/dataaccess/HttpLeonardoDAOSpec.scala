package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.UUID
import scala.language.postfixOps

class HttpLeonardoDAOSpec extends TestKit(ActorSystem("HttpLeonardoDAOSpec")) with AnyFlatSpecLike {

  val apiClient = new ApiClient()

  val workspaceId: UUID = UUID.randomUUID()

  val testConf: Config = ConfigFactory.load()
  val leonardoConfig: LeonardoConfig = LeonardoConfig.apply(testConf)

  "HttpLeonardoDAO" should "get an AppsV2Api object when calling getAppsV2leonardoApi during app creation" ignore {
    // TODO: does this need any tests?
  }

}
