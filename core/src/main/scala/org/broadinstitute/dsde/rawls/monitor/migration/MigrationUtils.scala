package org.broadinstitute.dsde.rawls.monitor.migration

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import cats.kernel.Semigroup
import com.google.cloud.{Identity, Policy, Role}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success}
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates.standardGoogleRetryConfig
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import slick.dbio.{DBIOAction, Effect, NoStream}
import spray.json.{JsObject, JsString}

import scala.collection.JavaConversions._
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


    import slick.jdbc.MySQLProfile.api._
    implicit class InsertExtensionMethod[E, A, C[_]](query: Query[E, A, C]) {
      /** alias for `+=` supporting dot syntax */
      def insert(value: A) = query += value
    }


    implicit class FutureToIO[+A](future: => Future[A]) {
      def io: IO[A] = IO.fromFuture(IO(future))
    }

    // TODO: move into workbench-libs
    implicit class StorageServiceExtensions[F[_]](storageService: GoogleStorageService[F]) {
      final def policyToStorageRoles(policy: Policy): Map[StorageRole, NonEmptyList[Identity]] =
        policy
          .getBindings
          .foldLeft(Map.newBuilder[StorageRole, NonEmptyList[Identity]]) { (builder, binding) =>
            NonEmptyList.fromList(binding._2.toList).map { identities =>
              builder += (StorageRole.CustomStorageRole(binding._1.getValue).asInstanceOf[StorageRole] -> identities)
            }.getOrElse(builder)
          }
          .result


      def removeIamPolicy(bucketName: GcsBucketName,
                          roles: Map[StorageRole, NonEmptyList[Identity]],
                          traceId: Option[TraceId] = None,
                          retryConfig: RetryConfig = standardGoogleRetryConfig)
      : fs2.Stream[F, Unit] =
        for {
          policy <- storageService.getIamPolicy(bucketName, traceId, retryConfig)
          newStorageRoles = policyToStorageRoles {
            roles
              .foldLeft(policy.toBuilder) { (builder, role) =>
                builder.removeIdentity(Role.of(role._1.name), role._2.head, role._2.tail: _*)
              }
              .build
          }
          _ <- storageService.overrideIamPolicy(bucketName, newStorageRoles, traceId, retryConfig)
        } yield ()
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
      message = JsObject((Map("message" -> message) ++ data).mapValues(s => new JsString(s.toString))).toString,
      cause = cause
    ) {}

}
