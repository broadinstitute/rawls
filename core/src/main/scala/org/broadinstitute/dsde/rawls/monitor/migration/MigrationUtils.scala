package org.broadinstitute.dsde.rawls.monitor.migration

import cats.implicits.{catsSyntaxOptionId, toTraverseOps}
import org.broadinstitute.dsde.rawls.RawlsException
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.language.higherKinds

object MigrationUtils {
  sealed trait Outcome

  object Outcome {
    case object Success extends Outcome

    final case class Failure(message: String) extends Outcome

    final def fromFields(outcome: Option[String], message: Option[String]): Either[String, Option[Outcome]] = {
      type EitherStringT[T] = Either[String, T]
      outcome.traverse[EitherStringT, Outcome] {
        case "Success" => Right(Success)
        case "Failure" => Right(Failure(message.getOrElse("")))
        case other => Left(s"""Failed to read outcome: unknown value -- "$other"""")
      }
    }

    final def toTuple(outcome: Outcome): (Option[String], Option[String]) = outcome match {
      case Success => ("Success".some, None)
      case Failure(msg) => ("Failure".some, msg.some)
    }
  }

  def unsafeFromEither[A, B](f: A => Either[String, B], a: A): B = f(a) match {
    case Right(r) => r
    case Left(msg) => throw new RawlsException(msg)
  }

  implicit class IgnoreResultExtensionMethod[+R, +S <: NoStream, -E <: Effect](action: DBIOAction[R, S, E]) {
    /** Ignore the result of the DBIOAction and return unit */
    def ignore: DBIOAction[Unit, NoStream, E with Effect] = action >> DBIOAction.successful()
  }

  import slick.jdbc.MySQLProfile.api._
  implicit class InsertExtensionMethod[E, T, C[_]](query: Query[E, T, C]) {
    /** alias for `+=` supporting dot syntax */
    def insert(value: T) = query += value
  }

}
