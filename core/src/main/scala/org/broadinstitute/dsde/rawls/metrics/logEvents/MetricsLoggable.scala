package org.broadinstitute.dsde.rawls.metrics.logEvents

import net.logstash.logback.argument.{StructuredArgument, StructuredArguments}

import scala.jdk.CollectionConverters._

trait MetricsLoggable {

  protected def toLoggableMap: java.util.Map[String, Any]
  def event: String

  private val baseMap = Map[String, Any](
    "service" -> "rawls",
    "warehouse" -> true,
    "event" -> event
  )

  protected def transformMap(map: Map[String, Any]): java.util.Map[String, Any] =
    (baseMap ++ map.view.mapValues {
      case it: Iterable[_]     => it.asJava
      case option: Option[Any] => option.orNull
      case v: Any              => v
    }).asJava

  def toStructuredArguments: StructuredArgument = StructuredArguments.keyValue("eventProperties", toLoggableMap)

}
