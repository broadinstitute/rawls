package org.broadinstitute.dsde.rawls.metrics.logEvents

import scala.jdk.CollectionConverters._

trait BardEvent {

  def getProperties: java.util.Map[String, Any]
  def eventName: String

  private val baseMap = Map[String, Any]("event" -> eventName, "pushToMixpanel" -> false)

  protected def transformMap(map: Map[String, Any]): java.util.Map[String, Any] =
    (baseMap ++ map.view.mapValues {
      case it: Iterable[_]     => it.asJava
      case option: Option[Any] => option.orNull
      case v: Any              => v
    }).asJava

}
