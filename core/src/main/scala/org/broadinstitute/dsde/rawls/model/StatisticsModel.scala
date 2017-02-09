package org.broadinstitute.dsde.rawls.model

import spray.json._
import spray.json.DefaultJsonProtocol._

/**
 * Created by mbemis on 7/20/16.
 */

sealed trait Statistic
case class SingleStatistic(value: Double) extends Statistic
case class SummaryStatistics(min: Double, max: Double, mean: Double, stddev: Double) extends Statistic
case class StatisticsReport(startDate: String, endDate: String, statistics: Map[String, Statistic])

object StatisticsJsonSupport extends JsonSupport {
  implicit val SingleStatisticFormat = jsonFormat1(SingleStatistic)
  implicit val SummaryStatisticsFormat = jsonFormat4(SummaryStatistics)

  implicit object StatisticFormat extends RootJsonFormat[Statistic] {
    override def write(obj: Statistic): JsValue = obj match {
      case SingleStatistic(value) => JsObject(Map("value" -> JsNumber(value)))
      case SummaryStatistics(min, max, mean, stddev) => JsObject(Map("min" -> JsNumber(min), "max" -> JsNumber(max), "mean" -> JsNumber(mean), "stddev" -> JsNumber(stddev)))
    }
    override def read(json: JsValue): Statistic = ???
  }

  implicit val StatisticsReportFormat = jsonFormat3(StatisticsReport)
}
