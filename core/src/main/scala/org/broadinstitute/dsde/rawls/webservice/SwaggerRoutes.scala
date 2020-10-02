package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.circe.yaml.syntax._
import io.circe.{yaml, _}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.config.SwaggerConfig
import org.webjars.WebJarAssetLocator

import scala.util.{Failure, Success, Try}

/**
  * Created by dvoet on 7/18/17.
  */
trait SwaggerRoutes extends LazyLogging {
  private val swaggerUiPath = new WebJarAssetLocator().getFullPath("swagger-ui-dist", "index.html").replace("/index.html", "")

  val swaggerConfig: SwaggerConfig

  // enable/disable snapshot routes based on a config flag
  val useDataRepoSwagger = Try(ConfigFactory.load().getBoolean("dataRepo.enabled")).toOption.getOrElse(false)

  val swaggerContents: String = if (useDataRepoSwagger) {
    Try(mergeYamls("/swagger/api-docs.yaml", "/swagger/data-repo-only.yaml")) match {
      case Success(merged) => merged
      case Failure(ex) =>
        logger.warn(s"Could not merge swagger yamls; defaulting to api-docs.yaml: ${ex.getMessage}", ex)
        loadResource("/swagger/api-docs.yaml")
    }
  } else {
    loadResource("/swagger/api-docs.yaml")
  }

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
    val swaggerOptions =
      """
        |        validatorUrl: null,
        |        apisSorter: "alpha",
        |        operationsSorter: "alpha"
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

  /**
   * read the contents of multiple yaml files and return the merged value as a String
   * @param filenames
   * @return
   */
  private def mergeYamls(filenames: String*): String = {

    // for each file, read it from disk, parse as yaml and return as a JsonObject
    val swaggerJsons = filenames.map { filename =>
      val contents: String = loadResource(filename)
      yaml.parser.parse(contents) match {
        case Right(json) => json.asObject.getOrElse(JsonObject.empty)
        case Left(parsingFailure) => throw new RawlsException(parsingFailure.message, parsingFailure.underlying)
      }
    }

    // merge all jsons, preferring rightmost
    val mergedJson: JsonObject = swaggerJsons.foldRight(JsonObject.empty) { (x, y) =>
      x.deepMerge(y)
    }

    // translate back to yaml and print as string
    Json.fromJsonObject(mergedJson).asYaml.spaces2
  }

}
