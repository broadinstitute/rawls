package org.broadinstitute.dsde.rawls.webservice

import akka.event.Logging.LogLevel
import akka.event.{Logging, LoggingAdapter}
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.ExecutionServiceCluster
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.metrics.InstrumentationDirectives
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, ErrorReport, UserInfo}
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, ExceptionHandler, RejectionHandler}
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import bio.terra.workspace.client.ApiException
import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.SwaggerConfig
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object RawlsApiService extends LazyLogging {
  val exceptionHandler = {
    import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

    ExceptionHandler {
      case withErrorReport: RawlsExceptionWithErrorReport =>
        complete(withErrorReport.errorReport.statusCode.getOrElse(StatusCodes.InternalServerError) -> withErrorReport.errorReport)
      case rollback:MySQLTransactionRollbackException =>
        logger.error(s"ROLLBACK EXCEPTION, PROBABLE DEADLOCK: ${rollback.getMessage} [${rollback.getErrorCode} ${rollback.getSQLState}] ${rollback.getNextException}", rollback)
        complete(StatusCodes.InternalServerError -> ErrorReport(rollback))
      case wsmApiException: ApiException =>
        complete(wsmApiException.getCode -> ErrorReport(wsmApiException).copy(stackTrace = Seq()))
      case e: Throwable =>
        // so we don't log the error twice when debug is enabled
        if (logger.underlying.isDebugEnabled) {
          logger.debug(e.getMessage, e)
        } else {
          logger.error(e.getMessage)
        }
        complete(StatusCodes.InternalServerError -> ErrorReport(e))
    }
  }

  def rejectionHandler = {
    import spray.json._
    import DefaultJsonProtocol._
    RejectionHandler.default.mapRejectionResponse {
      case res @ HttpResponse(status, _, ent: HttpEntity.Strict, _) =>
        res.withEntity(entity = HttpEntity(ContentTypes.`application/json`, Map(status.toString -> ent.data.utf8String).toJson.prettyPrint))
    }
  }
}

trait RawlsApiService //(val workspaceServiceConstructor: UserInfo => WorkspaceService, val userServiceConstructor: UserInfo => UserService, val genomicsServiceConstructor: UserInfo => GenomicsService, val statusServiceConstructor: () => StatusService, val executionServiceCluster: ExecutionServiceCluster, val appVersion: ApplicationVersion, val googleClientId: String, val submissionTimeout: FiniteDuration, override val workbenchMetricBaseName: String, val samDAO: SamDAO, val swaggerConfig: SwaggerConfig)(implicit val executionContext: ExecutionContext, val materializer: Materializer)
  extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService
  with AdminApiService with UserApiService with BillingApiService with BillingApiServiceV2 with NotificationsApiService with SnapshotApiService
  with StatusApiService with InstrumentationDirectives with SwaggerRoutes with VersionApiService with ServicePerimeterApiService {

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val entityServiceConstructor: UserInfo => EntityService
  val userServiceConstructor: UserInfo => UserService
  val genomicsServiceConstructor: UserInfo => GenomicsService
  val snapshotServiceConstructor: UserInfo => SnapshotService
  val statusServiceConstructor: () => StatusService
  val executionServiceCluster: ExecutionServiceCluster
  val appVersion: ApplicationVersion
  val googleClientId: String
  val submissionTimeout: FiniteDuration
  val workbenchMetricBaseName: String
  val samDAO: SamDAO
  val swaggerConfig: SwaggerConfig

  implicit val executionContext: ExecutionContext
  implicit val materializer: Materializer

  val baseApiRoutes = workspaceRoutes ~ entityRoutes ~ methodConfigRoutes ~ submissionRoutes ~ adminRoutes ~ userRoutes ~ billingRoutes ~ billingRoutesV2 ~ notificationsRoutes ~ servicePerimeterRoutes ~ snapshotRoutes

  def apiRoutes =
    options(complete(OK)) ~
    withExecutionContext(ExecutionContext.global) { //Serve real work off the global EC to free up the dispatcher to run more routes, including status
      baseApiRoutes
    }

  def route: server.Route = (logRequestResult & handleExceptions(RawlsApiService.exceptionHandler) & handleRejections(RawlsApiService.rejectionHandler)) {
    swaggerRoutes ~
    versionRoutes ~
    statusRoute ~
    pathPrefix("api")(apiRoutes)
  }

  // basis for logRequestResult lifted from http://stackoverflow.com/questions/32475471/how-does-one-log-akka-http-client-requests
  private def logRequestResult: Directive0 = {
    def entityAsString(entity: HttpEntity): Future[String] = {
      entity.dataBytes
        .map(_.decodeString(entity.contentType.charsetOption.getOrElse(HttpCharsets.`UTF-8`).value))
        .runWith(Sink.head)
    }

    def myLoggingFunction(logger: LoggingAdapter)(req: HttpRequest)(res: Any): Unit = {
      val entry = res match {
        case Complete(resp) =>
          val logLevel: LogLevel = resp.status.intValue / 100 match {
            case 5 => Logging.ErrorLevel
            case 4 => Logging.InfoLevel
            case _ => Logging.DebugLevel
          }
          entityAsString(resp.entity).map(data => LogEntry(s"${req.method} ${req.uri}: ${resp.status} entity: $data", logLevel))
        case other =>
          Future.successful(LogEntry(s"$other", Logging.DebugLevel)) // I don't really know when this case happens
      }
      entry.map(_.logTo(logger))
    }

    DebuggingDirectives.logRequestResult(LoggingMagnet(log => myLoggingFunction(log)))
  }
}

trait VersionApiService {
  val appVersion: ApplicationVersion
  val executionServiceCluster: ExecutionServiceCluster

  val versionRoutes = {
    import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ApplicationVersionFormat
    import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ExecutionServiceVersionFormat
    implicit val ec: ExecutionContext = ExecutionContext.global
    path("version") {
      get {
        complete(appVersion)
      }
    } ~
      path("version" / "executionEngine") {
        get {
          complete(executionServiceCluster.version)
        }
      }
  }
}

class RawlsApiServiceImpl(val workspaceServiceConstructor: UserInfo => WorkspaceService, val entityServiceConstructor: UserInfo => EntityService, val userServiceConstructor: UserInfo => UserService, val genomicsServiceConstructor: UserInfo => GenomicsService, val snapshotServiceConstructor: UserInfo => SnapshotService, val statusServiceConstructor: () => StatusService, val executionServiceCluster: ExecutionServiceCluster, val appVersion: ApplicationVersion, val googleClientId: String, val submissionTimeout: FiniteDuration, val batchUpsertMaxBytes: Long, override val workbenchMetricBaseName: String, val samDAO: SamDAO, val swaggerConfig: SwaggerConfig)(implicit val executionContext: ExecutionContext, val materializer: Materializer) extends RawlsApiService with StandardUserInfoDirectives
