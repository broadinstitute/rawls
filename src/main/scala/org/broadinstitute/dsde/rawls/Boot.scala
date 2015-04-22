package org.broadinstitute.dsde.rawls

import java.io.File

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.wordnik.swagger.model.ApiInfo
import org.broadinstitute.dsde.rawls.ws._
import spray.can.Http

import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import scala.sys.process.Process

object Boot extends App {

  private def setupSsl(conf: Config): Unit = {
    System.setProperty("javax.net.ssl.trustStore", conf.getString("ssl.truststore"))
    System.setProperty("javax.net.ssl.trustStorePassword", conf.getString("ssl.tsPasswd"))
    System.setProperty("javax.net.ssl.keyStore", conf.getString("ssl.keystore"))
    System.setProperty("javax.net.ssl.keyStorePassword", conf.getString("ssl.ksPasswd"))
    val toolOpts = List(
      "-Djavax.net.ssl.trustStore=/etc/truststore",
      "-Djavax.net.ssl.trustStorePassword=truststore",
      "-Djavax.net.ssl.keyStore=/etc/keystore",
      "-Djavax.net.ssl.keyStorePassword=keystore"
    )
    Process("env", None, "JAVA_TOOL_OPTIONS" -> toolOpts.mkString(";"))
  }

  private def startup(): Unit = {
    val conf = ConfigFactory.parseFile(new File("/etc/rawls.conf"))
    setupSsl(conf)

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("rawls")

    val swaggerConfig = conf.getConfig("swagger")
    val swaggerService = new SwaggerService(
      swaggerConfig.getString("apiVersion"),
      swaggerConfig.getString("baseUrl"),
      swaggerConfig.getString("apiDocs"),
      swaggerConfig.getString("swaggerVersion"),
      Seq(typeOf[RootRawlsApiService]),
      Option(new ApiInfo(
        swaggerConfig.getString("info"),
        swaggerConfig.getString("description"),
        swaggerConfig.getString("termsOfServiceUrl"),
        swaggerConfig.getString("contact"),
        swaggerConfig.getString("license"),
        swaggerConfig.getString("licenseUrl"))
      ))

    val service = system.actorOf(RawlsApiServiceActor.props(swaggerService), "rawls-service")

    implicit val timeout = Timeout(5.seconds)
    // start a new HTTP server on port 8080 with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 8080)
  }

  startup()
}
