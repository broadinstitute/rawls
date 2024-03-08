package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.Props
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor

object HealthMonitorFactory {
  def createHealthMonitorProps(appConfigManager: MultiCloudAppConfigManager,
                               slickDataSource: SlickDataSource,
                               gcsDAO: GoogleServicesDAO,
                               pubSubDAO: GooglePubSubDAO,
                               methodRepoDAO: MethodRepoDAO,
                               samDAO: SamDAO,
                               billingProfileManagerDAO: BillingProfileManagerDAO,
                               workspaceManagerDAO: WorkspaceManagerDAO,
                               executionServiceServers: Map[ExecutionServiceId, ExecutionServiceDAO]
  ): Props =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        HealthMonitor
          .propsInGoogleControlPlane(
            slickDataSource,
            gcsDAO,
            pubSubDAO,
            methodRepoDAO,
            samDAO,
            billingProfileManagerDAO,
            workspaceManagerDAO,
            executionServiceServers,
            Seq(gcsConfig.getString("notifications.topicName")),
            Seq.empty
          )
      case None =>
        HealthMonitor
          .propsInAzureControlPlane(slickDataSource, samDAO, billingProfileManagerDAO, workspaceManagerDAO)
    }
}
