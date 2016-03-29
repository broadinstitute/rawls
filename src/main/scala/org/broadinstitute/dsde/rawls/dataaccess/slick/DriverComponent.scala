package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp

import org.broadinstitute.dsde.rawls.model.{Attributable, ErrorReport}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import slick.driver.JdbcProfile
import spray.http.StatusCodes

import scala.concurrent.ExecutionContext

trait DriverComponent {
  val driver: JdbcProfile
  implicit val executionContext: ExecutionContext

  // needed by MySQL but not actually used; we will always overwrite
  val defaultTimeStamp = Timestamp.valueOf("2001-01-01 01:01:01.0")

  import driver.api._

  def uniqueResult[V](readAction: driver.api.Query[_, _, Seq]): ReadAction[Option[V]] = {
    readAction.result map {
      case Seq() => None
      case Seq(one) => Option(one.asInstanceOf[V])
      case tooMany => throw new RawlsException(s"Expected 0 or 1 result but found all of these: $tooMany")
    }
  }

  def uniqueResult[V](results: ReadAction[Seq[V]]): ReadAction[Option[V]] = {
    results map {
      case Seq() => None
      case Seq(one) => Option(one)
      case tooMany => throw new RawlsException(s"Expected 0 or 1 result but found all of these: $tooMany")
    }
  }

  //in general, we only support alphanumeric, spaces, _, and - for user-input
  def validateUserDefinedString(s: String) = {
    if(!s.matches("[A-z0-9_-]+")) throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = s"""Invalid input: "$s". Input may only contain alphanumeric characters, underscores, and dashes.""", statusCode = StatusCodes.BadRequest))
  }

  def validateAttributeName(name: String) = {
    if (Attributable.reservedAttributeNames.exists(_.equalsIgnoreCase(name))) {
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(message = s"Attribute name $name is reserved", statusCode = StatusCodes.BadRequest))
    }
  }

  def createBatches[T](items: Set[T], batchSize: Int = 1000): Iterable[Set[T]] = {
    items.zipWithIndex.groupBy(_._2 % batchSize).values.map(_.map(_._1))
  }
}
