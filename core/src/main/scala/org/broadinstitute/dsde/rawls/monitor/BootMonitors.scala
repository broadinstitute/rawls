package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import cats.effect.IO
import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.{FastPassConfig, RawlsConfigManager}
import org.broadinstitute.dsde.rawls.coordination.{CoordinatedDataSourceAccess, CoordinatedDataSourceActor, DataSourceAccess, UncoordinatedDataSourceAccess}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.drs.DrsResolver
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.fastpass.FastPassMonitor
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{MethodConfigResolver, SubmissionMonitorConfig, SubmissionSupervisor, WorkflowSubmissionActor}
import org.broadinstitute.dsde.rawls.metrics.BardService
import org.broadinstitute.dsde.rawls.model.{CromwellBackend, RawlsRequestContext, WorkflowStatuses, WorkspaceCloudPlatform}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig
import org.broadinstitute.dsde.rawls.util
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceRepository, WorkspaceService, WorkspaceSettingRepository}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.jdk.DurationConverters.JavaDurationOps
import scala.language.postfixOps
import scala.util.Try

//noinspection ScalaUnnecessaryParentheses,ScalaUnusedSymbol,TypeAnnotation
// handles monitors which need to be started at boot time
object BootMonitors extends LazyLogging {

  def bootMonitors(system: ActorSystem,
                   appConfigManager: RawlsConfigManager,
                   slickDataSource: SlickDataSource,
                   gcsDAO: GoogleServicesDAO,
                   googleIamDAO: GoogleIamDAO,
                   googleStorageDAO: GoogleStorageDAO,
                   samDAO: SamDAO,
                   notificationDAO: NotificationDAO,
                   pubSubDAO: GooglePubSubDAO,
                   cwdsDAO: CwdsDAO,
                   leonardoDAO: LeonardoDAO,
                   workspaceRepository: WorkspaceRepository,
                   googleStorage: GoogleStorageService[IO],
                   methodRepoDAO: MethodRepoDAO,
                   drsResolver: DrsResolver,
                   entityService: RawlsRequestContext => EntityService,
                   workspaceService: RawlsRequestContext => WorkspaceService,
                   shardedExecutionServiceCluster: ExecutionServiceCluster,
                   maxActiveWorkflowsTotal: Int,
                   maxActiveWorkflowsPerUser: Int,
                   metricsPrefix: String,
                   requesterPaysRole: String,
                   useWorkflowCollectionField: Boolean,
                   useWorkflowCollectionLabel: Boolean,
                   gcpBatchBackend: CromwellBackend,
                   methodConfigResolver: MethodConfigResolver,
                   bardService: BardService,
                   workspaceSettingRepository: WorkspaceSettingRepository,
                   entityManager: EntityManager
  ): Unit =

    system.actorOf(QuicksilverMigrationMonitor.props(slickDataSource, entityManager, 10 seconds))

}
