package org.broadinstitute.dsde.rawls.webservice

import akka.event.Logging.LogLevel
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import akka.http.scaladsl.server.{Directive0, ExceptionHandler, RejectionHandler, Route}
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import bio.terra.workspace.client.ApiException
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.context.Context
import io.sentry.Sentry
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProjectOrchestrator
import org.broadinstitute.dsde.rawls.bucketMigration.{BucketMigrationService, BucketMigrationServiceImpl}
import org.broadinstitute.dsde.rawls.dataaccess.{ExecutionServiceCluster, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.metrics.InstrumentationDirectives
import org.broadinstitute.dsde.rawls.model.{ApplicationVersion, ErrorReport, RawlsRequestContext, UserInfo}
import org.broadinstitute.dsde.rawls.openam.StandardUserInfoDirectives
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceService}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration

import java.sql.SQLTransactionRollbackException
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object RawlsApiService extends LazyLogging {
  val exceptionHandler = {
    import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

    ExceptionHandler {
      case withErrorReport: RawlsExceptionWithErrorReport =>
        withErrorReport.errorReport.statusCode match {
          case Some(_: ServerError) => Sentry.captureException(withErrorReport)
          case _                    => // don't send 4xx or any other non-5xx errors to Sentry
        }
        complete(
          withErrorReport.errorReport.statusCode.getOrElse(
            StatusCodes.InternalServerError
          ) -> withErrorReport.errorReport
        )
      case rollback: SQLTransactionRollbackException =>
        logger.error(
          s"ROLLBACK EXCEPTION, PROBABLE DEADLOCK: ${rollback.getMessage} [${rollback.getErrorCode} ${rollback.getSQLState}] ${rollback.getNextException}",
          rollback
        )
        Sentry.captureException(rollback)
        complete(StatusCodes.InternalServerError -> ErrorReport(rollback))
      case wsmApiException: ApiException =>
        if (wsmApiException.getCode >= 500) {
          Sentry.captureException(wsmApiException)
        }
        complete(wsmApiException.getCode -> ErrorReport(wsmApiException).copy(stackTrace = Seq()))
      case dataEntityException: DataEntityException =>
        // propagate only the message; don't include the stack trace
        complete(dataEntityException.code -> ErrorReport(dataEntityException.getMessage))
      case e: Throwable =>
        // so we don't log the error twice when debug is enabled
        if (logger.underlying.isDebugEnabled) {
          logger.debug(e.getMessage, e)
        } else {
          logger.error(e.getMessage)
        }
        Sentry.captureException(e)
        complete(StatusCodes.InternalServerError -> ErrorReport(e))
    }
  }

  def rejectionHandler = {
    import spray.json._
    import DefaultJsonProtocol._
    RejectionHandler.default.mapRejectionResponse { case res @ HttpResponse(status, _, ent: HttpEntity.Strict, _) =>
      res.withEntity(entity =
        HttpEntity(ContentTypes.`application/json`, Map(status.toString -> ent.data.utf8String).toJson.prettyPrint)
      )
    }
  }
}

trait RawlsApiService
    extends WorkspaceApiService
    with WorkspaceApiServiceV2
    with EntityApiService
    with MethodConfigApiService
    with SubmissionApiService
    with AdminApiService
    with UserApiService
    with BillingApiService
    with BillingApiServiceV2
    with NotificationsApiService
    with SnapshotApiService
    with StatusApiService
    with InstrumentationDirectives
    with VersionApiService
    with ServicePerimeterApiService {

  val multiCloudWorkspaceServiceConstructor: RawlsRequestContext => MultiCloudWorkspaceService
  val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService
  val entityServiceConstructor: RawlsRequestContext => EntityService
  val userServiceConstructor: RawlsRequestContext => UserService
  val genomicsServiceConstructor: RawlsRequestContext => GenomicsService
  val snapshotServiceConstructor: RawlsRequestContext => SnapshotService
  val spendReportingConstructor: RawlsRequestContext => SpendReportingService
  val billingProjectOrchestratorConstructor: RawlsRequestContext => BillingProjectOrchestrator
  val statusServiceConstructor: () => StatusService
  val executionServiceCluster: ExecutionServiceCluster
  val appVersion: ApplicationVersion
  val submissionTimeout: FiniteDuration
  val workbenchMetricBaseName: String
  val samDAO: SamDAO
  val openIDConnectConfiguration: OpenIDConnectConfiguration

  implicit val executionContext: ExecutionContext
  implicit val materializer: Materializer

  val baseApiRoutes = (otelContext: Context) =>
    workspaceRoutesV2(otelContext) ~
      workspaceRoutes(otelContext) ~
      entityRoutes(otelContext) ~
      methodConfigRoutes(otelContext) ~
      submissionRoutes(otelContext) ~
      adminRoutes(otelContext) ~
      userRoutes(otelContext) ~
      billingRoutesV2(otelContext) ~
      billingRoutes(otelContext) ~
      notificationsRoutes ~
      servicePerimeterRoutes(otelContext) ~
      snapshotRoutes(otelContext)

  val instrumentedRoutes = instrumentRequest(baseApiRoutes)

  def apiRoutes =
    options(complete(OK)) ~
      withExecutionContext(ExecutionContext.global) { // Serve real work off the global EC to free up the dispatcher to run more routes, including status
        instrumentedRoutes
      }

  def route: server.Route = (logRequestResult & handleExceptions(RawlsApiService.exceptionHandler) & handleRejections(
    RawlsApiService.rejectionHandler
  )) {
    openIDConnectConfiguration.swaggerRoutes("swagger/api-docs.yaml") ~
      openIDConnectConfiguration.oauth2Routes(materializer.system) ~
      versionRoutes ~
      statusRoute ~
      pathPrefix("api")(apiRoutes)
  }

  // basis for logRequestResult lifted from http://stackoverflow.com/questions/32475471/how-does-one-log-akka-http-client-requests
  private def logRequestResult: Directive0 = {
    def entityAsString(entity: HttpEntity): Future[String] =
      entity.dataBytes
        .map(_.decodeString(entity.contentType.charsetOption.getOrElse(HttpCharsets.`UTF-8`).value))
        .runWith(Sink.head)

    // for these paths, don't log 5xx responses as ERROR; log them as WARN. This prevents spamming Sentry.
    val ignorableErrorPaths = Set("/status")

    def myLoggingFunction(logger: LoggingAdapter)(req: HttpRequest)(res: Any): Unit = {
      val entry = res match {
        case Complete(resp) =>
          val logLevel: LogLevel = resp.status.intValue / 100 match {
            case 5 if ignorableErrorPaths.contains(req.uri.path.toString()) => Logging.WarningLevel
            case 5 => Logging.ErrorLevel
            case 4 => Logging.InfoLevel
            case _ => Logging.DebugLevel
          }
          entityAsString(resp.entity).map(data =>
            LogEntry(s"${req.method} ${req.uri}: ${resp.status} entity: $data", logLevel)
          )
        case other =>
          Future.successful(LogEntry(s"$other", Logging.DebugLevel)) // I don't really know when this case happens
      }
      entry.map(_.logTo(logger))
    }

    DebuggingDirectives.logRequestResult(LoggingMagnet(myLoggingFunction))
  }
}

trait VersionApiService {
  val appVersion: ApplicationVersion
  val executionServiceCluster: ExecutionServiceCluster

  val versionRoutes = {
    import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ExecutionServiceVersionFormat
    import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ApplicationVersionFormat
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

class RawlsApiServiceImpl(val multiCloudWorkspaceServiceConstructor: RawlsRequestContext => MultiCloudWorkspaceService,
                          val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService,
                          val entityServiceConstructor: RawlsRequestContext => EntityService,
                          val userServiceConstructor: RawlsRequestContext => UserService,
                          val genomicsServiceConstructor: RawlsRequestContext => GenomicsService,
                          val snapshotServiceConstructor: RawlsRequestContext => SnapshotService,
                          val spendReportingConstructor: RawlsRequestContext => SpendReportingService,
                          val billingProjectOrchestratorConstructor: RawlsRequestContext => BillingProjectOrchestrator,
                          val bucketMigrationServiceConstructor: RawlsRequestContext => BucketMigrationService,
                          val statusServiceConstructor: () => StatusService,
                          val executionServiceCluster: ExecutionServiceCluster,
                          val appVersion: ApplicationVersion,
                          val submissionTimeout: FiniteDuration,
                          val batchUpsertMaxBytes: Long,
                          override val workbenchMetricBaseName: String,
                          val samDAO: SamDAO,
                          val openIDConnectConfiguration: OpenIDConnectConfiguration
)(implicit val executionContext: ExecutionContext, val materializer: Materializer)
    extends RawlsApiService
    with StandardUserInfoDirectives
