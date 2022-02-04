package org.broadinstitute.dsde.rawls.monitor.migration

import cats.effect.IO
import cats.implicits._
import cats.kernel.Semigroup
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success}
import slick.dbio.{DBIOAction, Effect, NoStream}
import slick.lifted.Query
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.Future
import scala.language.higherKinds

object MigrationUtils {
  sealed trait Outcome

  object Outcome {
    case object Success extends Outcome

    final case class Failure(message: String) extends Outcome

    final def fromFields(outcome: Option[String], message: Option[String]): Either[String, Option[Outcome]] =
      outcome.traverse[Either[String, *], Outcome] {
        case "Success" => Right(Success)
        case "Failure" => Right(Failure(message.getOrElse("")))
        case other => Left(s"""Failed to read outcome: unknown value -- "$other"""")
      }

    final def toTuple(outcome: Outcome): (Option[String], Option[String]) = outcome match {
      case Success => ("Success".some, None)
      case Failure(msg) => ("Failure".some, msg.some)
    }
  }

  object Implicits {
    implicit val semigroupOutcome: Semigroup[Outcome] = (a, b) => a match {
        case Success => b
        case Failure(msgA) => b match {
          case Success => a
          case Failure(msgB) => Failure (msgA ++ "\n" ++ msgB)
        }
      }

    implicit class IgnoreResultExtensionMethod[+R, +S <: NoStream, -E <: Effect](action: DBIOAction[R, S, E]) {
      /** Ignore the result of the DBIOAction and return unit */
      def ignore: DBIOAction[Unit, NoStream, E with Effect] = action >> DBIOAction.successful()
    }


    implicit class InsertExtensionMethod[E, A, C[_]](query: Query[E, A, C]) {
      import slick.jdbc.MySQLProfile.api._
      /** alias for `+=` supporting dot syntax */
      def insert(value: A) = query += value
    }


    implicit class FutureToIO[+A](future: => Future[A]) {
      def io: IO[A] = IO.fromFuture(IO(future))
    }


    implicit class ToJsonOps(data: Map[String, Any]) {
      def toJson: JsValue = JsObject(data.mapValues(s => new JsString(s.toString)))
    }
  }


  def unsafeFromEither[A, B](f: A => Either[String, B], a: A): B = f(a) match {
    case Right(r) => r
    case Left(msg) => throw new RawlsException(msg)
  }

  final case class WorkspaceMigrationException(message: String,
                                               data: Map[String, Any] = Map.empty,
                                               cause: Throwable = null)
    extends RawlsException(
      message = (Map("message" -> message) ++ data).toJson.toString,
      cause = cause
    ) {}

}
