package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.model.{GoogleProjectNumber, ServicePerimeterName}

import scala.collection.JavaConverters._

final case class WorkspaceServiceConfig(trackDetailedSubmissionMetrics: Boolean, workspaceBucketNamePrefix: String, staticProjectsInPerimeters: Map[ServicePerimeterName, Seq[GoogleProjectNumber]] = Map.empty)

case object WorkspaceServiceConfig {
  def apply[T <: WorkspaceServiceConfig](conf: Config): WorkspaceServiceConfig = {
    val gcsConfig = conf.getConfig("gcs")
    val staticProjectConfig = gcsConfig.getConfig("servicePerimeters.staticProjects")

    val mappings: Map[ServicePerimeterName, Seq[GoogleProjectNumber]] = staticProjectConfig.entrySet().asScala.map { entry =>
      val key = ServicePerimeterName(entry.getKey.replace("\"", ""))
      val value = staticProjectConfig.getStringList(entry.getKey).asScala.map(GoogleProjectNumber)
      key -> value
    }.toMap

    new WorkspaceServiceConfig(conf.getBoolean("submissionmonitor.trackDetailedSubmissionMetrics"), gcsConfig.getString("groupsPrefix"), mappings)
  }
}
