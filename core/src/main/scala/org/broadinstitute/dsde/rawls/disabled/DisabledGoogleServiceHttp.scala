package org.broadinstitute.dsde.rawls.disabled

import cats.effect.{Async, Resource, Temporal}
import com.google.cloud.Identity
import com.google.pubsub.v1.TopicName
import org.broadinstitute.dsde.workbench.google2.{Filters, GoogleServiceHttp}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.typelevel.log4cats.Logger

class DisabledGoogleServiceHttp[F[_]: Async] extends GoogleServiceHttp[F] {
  override def createNotification(topic: TopicName,
                         bucketName: GcsBucketName,
                         filters: Filters,
                         traceId: Option[TraceId]
                        ): F[Unit] =
    throw new NotImplementedError("createNotification is not implemented for Azure.")
  override def getProjectServiceAccount(project: GoogleProject, traceId: Option[TraceId]): F[Identity] =
    throw new NotImplementedError("getProjectServiceAccount is not implemented for Azure.")
}


