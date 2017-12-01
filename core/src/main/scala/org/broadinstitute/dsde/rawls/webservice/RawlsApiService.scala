package org.broadinstitute.dsde.rawls.webservice

import akka.actor.{Actor, Props}
import org.broadinstitute.dsde.rawls.dataaccess.{HttpSamDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.ExecutionServiceCluster
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.metrics.InstrumentationDirectives
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, UserInfo}
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives
import org.broadinstitute.dsde.rawls.statistics.StatisticsService
import org.broadinstitute.dsde.rawls.status.StatusService
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
  def props(workspaceServiceConstructor: UserInfo => WorkspaceService, userServiceConstructor: UserInfo => UserService, genomicsServiceConstructor: UserInfo => GenomicsService, statisticsServiceConstructor: UserInfo => StatisticsService, statusServiceConstructor: () => StatusService, executionServiceCluster: ExecutionServiceCluster, appVersion: ApplicationVersion, googleClientId: String, submissionTimeout: FiniteDuration, rawlsMetricBaseName: String, httpSamDAO: SamDAO)(implicit executionContext: ExecutionContext): Props = {
    Props(new RawlsApiServiceActor(workspaceServiceConstructor, userServiceConstructor, genomicsServiceConstructor, statisticsServiceConstructor, statusServiceConstructor, executionServiceCluster, appVersion, googleClientId, submissionTimeout, rawlsMetricBaseName, httpSamDAO))
  }
}

class RawlsApiServiceActor(val workspaceServiceConstructor: UserInfo => WorkspaceService, val userServiceConstructor: UserInfo => UserService, val genomicsServiceConstructor: UserInfo => GenomicsService, val statisticsServiceConstructor: UserInfo => StatisticsService, val statusServiceConstructor: () => StatusService, val executionServiceCluster: ExecutionServiceCluster, val appVersion: ApplicationVersion, val googleClientId: String, val submissionTimeout: FiniteDuration, override val workbenchMetricBaseName: String, val samDAO: SamDAO)(implicit val executionContext: ExecutionContext) extends Actor
  with RootRawlsApiService with WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService
  with AdminApiService with UserApiService with StandardUserInfoDirectives with BillingApiService with NotificationsApiService
  with StatusApiService with InstrumentationDirectives {

  def actorRefFactory = context
  def apiRoutes = options{ complete(OK) } ~ baseRoute ~ workspaceRoutes ~ entityRoutes ~ methodConfigRoutes ~ submissionRoutes ~ adminRoutes ~ userRoutes ~ billingRoutes ~ notificationsRoutes
  def registerRoutes = options{ complete(OK) } ~ createUserRoute

  def receive = runRoute(
    instrumentRequest {
      swaggerRoute ~
      versionRoutes ~
      statusRoute ~
      pathPrefix("api") { apiRoutes } ~
      pathPrefix("register") { registerRoutes }
    }
  )
}

trait RootRawlsApiService extends HttpService {
  val executionServiceCluster: ExecutionServiceCluster
  val appVersion: ApplicationVersion
  val googleClientId: String

  val baseRoute = {
    path("headers") {
      get {
        requestContext => requestContext.complete(requestContext.request.headers.mkString(",\n"))
      }
    }
  }

  private val swaggerUiPath = "META-INF/resources/webjars/swagger-ui/2.2.5"

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

  val versionRoutes = {
    import spray.httpx.SprayJsonSupport._
    import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ApplicationVersionFormat
    import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ExecutionServiceVersionFormat
    implicit val ec: ExecutionContext = ExecutionContext.global
    path("version") {
      get {
        requestContext => requestContext.complete(appVersion)
      }
    } ~
    path("version" / "executionEngine") {
      get {
        requestContext => executionServiceCluster.version map { requestContext.complete(_) }
      }
    }
  }

  private def serveSwaggerIndex(): Route = {
    val indexHtml = getResourceFileContents(swaggerUiPath + "/index.html")
    complete {
      HttpEntity(ContentType(`text/html`),
        indexHtml
          .replace("your-client-id", googleClientId)
          .replace("scopeSeparator: \",\"", "scopeSeparator: \" \"")
          .replace("url = \"http://petstore.swagger.io/v2/swagger.json\";",
            "url = '/rawls.yaml';")
          .replace("jsonEditor: false,", "jsonEditor: false, validatorUrl: null, apisSorter: 'alpha',")
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
