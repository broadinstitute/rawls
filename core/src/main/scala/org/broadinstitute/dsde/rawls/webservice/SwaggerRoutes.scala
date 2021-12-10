package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.SwaggerConfig
import org.webjars.WebJarAssetLocator

/**
  * Created by dvoet on 7/18/17.
  */
trait SwaggerRoutes extends LazyLogging {
  private val swaggerUiPath = new WebJarAssetLocator().getFullPath("swagger-ui-dist", "index.html").replace("/index.html", "")

  val swaggerConfig: SwaggerConfig

  val swaggerContents: String = loadResource("/swagger/api-docs.yaml")

  val swaggerRoutes: server.Route = {
    path("") {
      get {
        serveIndex()
      }
    } ~
      path("api-docs.yaml") {
        get {
          complete(HttpEntity(ContentTypes.`application/octet-stream`, swaggerContents.getBytes))
        }
      } ~
      // We have to be explicit about the paths here since we're matching at the root URL and we don't
      // want to catch all paths lest we circumvent Spray's not-found and method-not-allowed error
      // messages.
      (pathPrefixTest("swagger-ui") | pathPrefixTest("oauth2") | pathSuffixTest("js")
        | pathSuffixTest("css") | pathPrefixTest("favicon")) {
        get {
          getFromResourceDirectory(swaggerUiPath)
        }
      }
  }

  private def serveIndex(): server.Route = {
    logger.info("Willy!!! You got a log message in swaggerRoutes.serveIndex")
    val swaggerOptions =
      """
        |        validatorUrl: null,
        |        apisSorter: "alpha",
        |        operationsSorter: "alpha",
        |        docExpansion: "none"
      """.stripMargin

    mapResponseEntity { entityFromJar =>
      entityFromJar.transformDataBytes(Flow.fromFunction[ByteString, ByteString] { original: ByteString =>
        ByteString(original.utf8String
          .replace("""url: "https://petstore.swagger.io/v2/swagger.json"""", "url: '/api-docs.yaml'")
          .replace("""layout: "StandaloneLayout"""", s"""layout: "StandaloneLayout", $swaggerOptions""")
          .replace("window.ui = ui", s"""ui.initOAuth({
                                        |        clientId: "${swaggerConfig.googleClientId}",
                                        |        clientSecret: "${swaggerConfig.realm}",
                                        |        realm: "${swaggerConfig.realm}",
                                        |        appName: "rawls",
                                        |        scopeSeparator: " ",
                                        |        additionalQueryStringParams: {}
                                        |      })
                                        |      window.ui = ui
                                        |      """.stripMargin)
        )})
    } {
      getFromResource(s"$swaggerUiPath/index.html")
    }
  }

  private def loadResource(filename: String) = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream(filename))
    try source.mkString finally source.close()
  }

}
