package org.broadinstitute.dsde.rawls.spendreporting

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport

import scala.math.BigDecimal.RoundingMode
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import org.joda.time.DateTime

import java.util.Currency

object SpendReportUtils {

  def getCurrency(currencies: Seq[String]): Currency =
    currencies.distinct match {
      case head :: List() => Currency.getInstance(head)
      case head :: tail =>
        throw RawlsExceptionWithErrorReport(
          StatusCodes.InternalServerError,
          s"Inconsistent currencies found while aggregating spend data: $head and ${tail.head} cannot be combined"
        )
      case List() => throw RawlsExceptionWithErrorReport(StatusCodes.NotFound, "No currencies found for spend data")
    }

  def toBigDecimal(cost: Float, currencyCode: Currency): BigDecimal =
    BigDecimal(cost.toString).setScale(currencyCode.getDefaultFractionDigits, RoundingMode.HALF_EVEN)

  def convertJodaToJava(dateTimeOpt: Option[DateTime]): LocalDateTime =
    dateTimeOpt match {
      case Some(dateTime) =>
        LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime.getMillis), ZoneId.systemDefault())
      case None =>
        throw new IllegalArgumentException("DateTime value is missing")
    }

  def convertLocalDateTimeToJodaDateTime(localDateTime: LocalDateTime,
                                         zoneId: ZoneId = ZoneId.systemDefault()
  ): Option[DateTime] =
    Option(localDateTime).map { ldt =>
      // Convert LocalDateTime to ZonedDateTime
      val zonedDateTime: ZonedDateTime = ldt.atZone(zoneId)

      // Convert ZonedDateTime to Joda DateTime using epoch milli
      new DateTime(zonedDateTime.toInstant.toEpochMilli)
    }
}
