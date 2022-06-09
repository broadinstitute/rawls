package org.broadinstitute.dsde.rawls.monitor.migration

import cats.arrow.Arrow
import cats.effect.IO
import cats.implicits._
import cats.kernel.Semigroup
import cats.{CoflatMap, MonadThrow, StackSafeMonad}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success}
import slick.dbio.{DBIOAction, Effect, NoStream}
import slick.lifted.Query
import spray.json.{DeserializationException, JsObject, JsString, JsValue, RootJsonFormat}

import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object MigrationUtils {
  sealed trait Outcome {
    def isSuccess: Boolean
    def isFailure: Boolean
  }

  object Outcome {
    case object Success extends Outcome {
      override def isSuccess: Boolean = true
      override def isFailure: Boolean = false
    }

    final case class Failure(message: String) extends Outcome {
      override def isSuccess: Boolean = false
      override def isFailure: Boolean = true
    }

    final def fromFields(outcome: Option[String], message: Option[String]): Either[String, Option[Outcome]] =
      outcome.traverse[Either[String, *], Outcome] {
        case "Success" => Right(Success)
        case "Failure" => Right(Failure(message.getOrElse("")))
        case other => Left(s"""Failed to read outcome: unknown value -- "$other"""")
      }

    final def fromEither(either: Either[Throwable, Unit]): Outcome = either match {
      case Right(_) => Success
      case Left(throwable) => Failure(throwable.getMessage)
    }

    final def toTuple(outcome: Outcome): (String, Option[String]) = outcome match {
      case Success => ("Success", None)
      case Failure(msg) => ("Failure", msg.some)
    }

    final def toFields(outcome: Option[Outcome]): (Option[String], Option[String]) =
      outcome
        .map(Arrow[Function].first((_ : String).some) compose toTuple)
        .getOrElse((None, None))
  }

  object Implicits {
    implicit val outcomeJsonFormat = new RootJsonFormat[Outcome] {
      val JsSuccess = JsObject("type" -> JsString("success"))

      object JsFailure {
        def unapply(json: JsValue): Option[String] = json match {
          case JsObject(fields) =>
            for {
              jsType <- fields.get("type")
              jsMessage <- fields.get("message")
              message <- jsMessage match {
                case JsString(message) if jsType == JsString("failure") => Some(message)
                case _ => None
              }
            } yield message
          case _ => None
        }
      }

      override def read(json: JsValue): Outcome = json match {
        case JsSuccess => Success
        case JsFailure(message) => Failure(message)
        case _ => throw DeserializationException(s"""Malformed json outcome: "$json".""")
      }

      override def write(outcome: Outcome): JsValue = outcome match {
        case Success => JsSuccess
        case Failure(message) => JsObject(
          "type" -> JsString("failure"),
          "message" -> JsString(message)
        )
      }
    }


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


    implicit class ToJsonOps(data: Iterable[(String, Any)]) {
      def toJson: JsValue = JsObject(
        data.map(Arrow[Function].second((s: Any) => new JsString(s.toString))).toMap
      )
    }

    implicit def monadErrorDBIOAction[E <: Effect]
    : MonadThrow[DBIOAction[*, NoStream, E]] with CoflatMap[DBIOAction[*, NoStream, E]] =
      new MonadThrow[DBIOAction[*, NoStream, E]]
        with StackSafeMonad[DBIOAction[*, NoStream, E]]
        with CoflatMap[DBIOAction[*, NoStream, E]] {

        override def pure[A](x: A): DBIOAction[A, NoStream, E] =
          DBIOAction.successful(x)

        override def map[A, B](fa: DBIOAction[A, NoStream, E])
                              (f: A => B)
        : DBIOAction[B, NoStream, E] = fa.map(f)

        override def flatMap[A, B](fa: DBIOAction[A, NoStream, E])
                                  (f: A => DBIOAction[B, NoStream, E])
        : DBIOAction[B, NoStream, E] = fa.flatMap(f)

        override def coflatMap[A, B](fa: DBIOAction[A, NoStream, E])
                                    (f: DBIOAction[A, NoStream, E] => B)
        : DBIOAction[B, NoStream, E] = pure(f(fa))

        override def raiseError[A](t: Throwable): DBIOAction[A, NoStream, E] =
          DBIOAction.failed(t)

        override def handleErrorWith[A](fa: DBIOAction[A, NoStream, E])
                                       (f: Throwable => DBIOAction[A, NoStream, E])
        : DBIOAction[A, NoStream, E] = fa.asTry.flatMap {
            case scala.util.Success(a) => pure(a)
            case scala.util.Failure(t) => f(t)
          }
      }
  }


  def unsafeFromEither[A](fa: => Either[String, A]): A = fa match {
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
