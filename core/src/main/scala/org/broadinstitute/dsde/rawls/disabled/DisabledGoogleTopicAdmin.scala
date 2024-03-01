package org.broadinstitute.dsde.rawls.disabled

import cats.effect.{Async, Resource}
import cats.mtl.Ask
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.Identity
import com.google.pubsub.v1.{Topic, TopicName}
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.GoogleTopicAdmin
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.typelevel.log4cats.StructuredLogger

class DisabledGoogleTopicAdmin[F[_]: Async] extends GoogleTopicAdmin[F] {

  /**
   * @param traceId uuid for tracing a unique call flow in logging
   */
  def create(projectTopicName: TopicName, traceId: Option[TraceId] = None): F[Unit] =
    throw new NotImplementedError("create is not implemented for Azure.")

  def delete(projectTopicName: TopicName, traceId: Option[TraceId] = None): F[Unit] =
    throw new NotImplementedError("delete is not implemented for Azure.")

  def list(project: GoogleProject)(implicit ev: Ask[F, TraceId]): Stream[F, Topic] =
    throw new NotImplementedError("list is not implemented for Azure.")


  def createWithPublisherMembers(projectTopicName: TopicName,
                                 members: List[Identity],
                                 traceId: Option[TraceId] = None
                                ): F[Unit] =
    throw new NotImplementedError("createWithPublisherMembers is not implemented for Azure.")
}

object DisabledGoogleTopicAdmin {
  def fromCredentialPath[F[_]: StructuredLogger: Async](pathToCredential: String
                                                       ): Resource[F, GoogleTopicAdmin[F]] = {
    Resource.pure[F, GoogleTopicAdmin[F]](new DisabledGoogleTopicAdmin[F])
  }

  def fromServiceAccountCrendential[F[_]: StructuredLogger: Async](
                                                                    serviceAccountCredentials: ServiceAccountCredentials
                                                                  ): Resource[F, GoogleTopicAdmin[F]] =
    Resource.pure[F, GoogleTopicAdmin[F]](new DisabledGoogleTopicAdmin[F])
}

