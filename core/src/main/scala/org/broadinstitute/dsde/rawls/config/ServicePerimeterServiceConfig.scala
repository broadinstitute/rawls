package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.model.{GoogleProjectNumber, ServicePerimeterName}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

final case class ServicePerimeterServiceConfig(
  staticProjectsInPerimeters: Map[ServicePerimeterName, Seq[GoogleProjectNumber]] = Map.empty,
  pollInterval: FiniteDuration,
  pollTimeout: FiniteDuration
)

case object ServicePerimeterServiceConfig {
  def apply[T <: ServicePerimeterServiceConfig](conf: Config): ServicePerimeterServiceConfig = {
    val gcsConfig = conf.getConfig("gcs")
    val staticProjectConfig = gcsConfig.getConfig("servicePerimeters.staticProjects")

    val mappings: Map[ServicePerimeterName, Seq[GoogleProjectNumber]] = staticProjectConfig
      .entrySet()
      .asScala
      .map { entry =>
        val key: ServicePerimeterName = ServicePerimeterName(entry.getKey.replace("\"", ""))
        val value: Seq[GoogleProjectNumber] =
          staticProjectConfig.getStringList(entry.getKey).asScala.map(GoogleProjectNumber).toSeq
        key -> value
      }
      .toMap

    val pollInterval = Duration.fromNanos(gcsConfig.getDuration("servicePerimeters.pollInterval").toNanos)
    val pollTimeout = Duration.fromNanos(gcsConfig.getDuration("servicePerimeters.pollTimeout").toNanos)

    new ServicePerimeterServiceConfig(mappings, pollInterval, pollTimeout)
  }
}
