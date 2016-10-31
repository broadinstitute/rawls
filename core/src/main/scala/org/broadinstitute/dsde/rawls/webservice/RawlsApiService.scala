package org.broadinstitute.dsde.rawls.webservice

import akka.actor.{Actor, Props}
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, UserInfo}
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives
import org.broadinstitute.dsde.rawls.statistics.StatisticsService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.parboiled.common.FileUtils
import spray.http.MediaTypes._
import spray.http.{ContentType, HttpEntity, HttpResponse, StatusCodes}
import spray.routing.Directive.pimpApply
import spray.routing._
import spray.http.StatusCodes._
import spray.util.actorSystem

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object RawlsApiServiceActor {
  def props(workspaceServiceConstructor: UserInfo => WorkspaceService, userServiceConstructor: UserInfo => UserService, genomicsServiceConstructor: UserInfo => GenomicsService, statisticsServiceConstructor: UserInfo => StatisticsService, appVersion: ApplicationVersion, googleClientId: String, submissionTimeout: FiniteDuration)(implicit executionContext: ExecutionContext): Props = {
    Props(new RawlsApiServiceActor(workspaceServiceConstructor, userServiceConstructor, genomicsServiceConstructor, statisticsServiceConstructor, appVersion, googleClientId, submissionTimeout))
  }
}

class RawlsApiServiceActor(val workspaceServiceConstructor: UserInfo => WorkspaceService, val userServiceConstructor: UserInfo => UserService, val genomicsServiceConstructor: UserInfo => GenomicsService, val statisticsServiceConstructor: UserInfo => StatisticsService, val appVersion: ApplicationVersion, val googleClientId: String, val submissionTimeout: FiniteDuration)(implicit val executionContext: ExecutionContext) extends Actor
  with RootRawlsApiService with WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService
  with AdminApiService with UserApiService with StandardUserInfoDirectives with BillingApiService {

  def actorRefFactory = context
  def apiRoutes = options{ complete(OK) } ~ baseRoute ~ workspaceRoutes ~ entityRoutes ~ methodConfigRoutes ~ submissionRoutes ~ adminRoutes ~ userRoutes ~ billingRoutes
  def registerRoutes = options{ complete(OK) } ~ createUserRoute ~ getUserStatusRoute

  def receive = runRoute(
    swaggerRoute ~
    versionRoute ~
    pathPrefix("api") { apiRoutes } ~
    pathPrefix("register") { registerRoutes }
  )
}

trait RootRawlsApiService extends HttpService {
  val appVersion: ApplicationVersion
  val googleClientId: String

  val baseRoute = {
    path("headers") {
      get {
        requestContext => requestContext.complete(requestContext.request.headers.mkString(",\n"))
      }
    }
  }

  private val swaggerUiPath = "META-INF/resources/webjars/swagger-ui/2.1.1"

  val swaggerRoute = {
    get {
      pathPrefix("") {
        pathEnd {
          serveSwaggerIndex()
        } ~
          pathSingleSlash {
            complete {
              HttpResponse(StatusCodes.NotFound)
            }
          }
      } ~ getFromResourceDirectory("swagger/") ~ getFromResourceDirectory(swaggerUiPath)
    }
  }

  val versionRoute = {
    import spray.httpx.SprayJsonSupport._
    import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ApplicationVersionFormat
    path("version") {
      get {
        requestContext => requestContext.complete(appVersion)
      }
    }
  }

  private def serveSwaggerIndex(): Route = {
    print(swaggerUiPath)
    val indexHtml = getResourceFileContents(swaggerUiPath + "/index.html")
    complete {
      HttpEntity(ContentType(`text/html`),
        indexHtml
          .replace("your-client-id", googleClientId)
          .replace("scopeSeparator: \",\"", "scopeSeparator: \" \"")
          .replace("url = \"http://petstore.swagger.io/v2/swagger.json\";",
            "url = '/rawls.yaml';")
      )
    }
  }

  private def getResourceFileContents(path: String): String = {
    val classLoader = actorSystem(actorRefFactory).dynamicAccess.classLoader
    val inputStream = classLoader.getResource(path).openStream()
    try {
      FileUtils.readAllText(inputStream)
    } finally {
      inputStream.close()
    }
  }
}
