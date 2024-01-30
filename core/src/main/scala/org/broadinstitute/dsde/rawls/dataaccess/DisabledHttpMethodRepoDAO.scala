package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.PathMatchers.Segment
import akka.http.scaladsl.server.directives.PathDirectives._
import akka.stream.Materializer
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.MethodRepoConfig
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.metrics.{Expansion, InstrumentedRetry, RawlsExpansion, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.HttpClientUtilsGzipInstrumented

import scala.concurrent.{ExecutionContext, Future}

class DisabledHttpMethodRepoDAO(agoraConfig: MethodRepoConfig[Agora.type],
                        dockstoreConfig: MethodRepoConfig[Dockstore.type],
                        override val workbenchMetricBaseName: String
                       )(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext)
  extends MethodRepoDAO
    with DsdeHttpDAO
    with InstrumentedRetry
    with LazyLogging
    with RawlsInstrumented
    with ServiceDAOWithStatus {

  override val http = Http(system)
  override val httpClientUtils = HttpClientUtilsGzipInstrumented()

  private val agoraServiceURL = agoraConfig.serviceUrl
  private val dockstoreServiceURL = dockstoreConfig.serviceUrl
  protected val statusUrl = s"$agoraServiceURL/status"

  implicit private lazy val baseMetricBuilder: ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(SubsystemMetricKey, Subsystems.Agora)

  // Strip out unique IDs from metrics by providing a redactedUriExpansion
  override protected val UriExpansion: Expansion[Uri] = RawlsExpansion.redactedUriExpansion(
    Seq((Slash ~ "api" ~ Slash ~ "v1").? / ("configurations" | "methods") / Segment / Segment / Segment)
  )

  private def postAgoraEntity(url: String, agoraEntity: AgoraEntity, userInfo: UserInfo): Future[AgoraEntity] =
    throw new NotImplementedError("This method is not implemented for Azure.")

  override def getMethodConfig(namespace: String,
                               name: String,
                               version: Int,
                               userInfo: UserInfo
                              ): Future[Option[AgoraEntity]] =
    throw new NotImplementedError("getMethodConfig is not implemented for Azure.")

  private def noneIfNotFound[T]: PartialFunction[Throwable, Option[T]] =
    throw new NotImplementedError("noneIfNotFound is not implemented for Azure.")

  override def getMethod(method: MethodRepoMethod, userInfo: UserInfo): Future[Option[WDL]] =
    throw new NotImplementedError("getMethod is not implemented for Azure.")

  private def getAgoraEntity(url: String, userInfo: UserInfo): Future[Option[AgoraEntity]] =
    throw new NotImplementedError("getAgoraEntity is not implemented for Azure.")

  private def getDockstoreMethod(method: DockstoreMethod): Future[Option[GA4GHTool]] =
    throw new NotImplementedError("getDockstoreMethod is not implemented for Azure.")

  override def postMethodConfig(namespace: String,
                                name: String,
                                methodConfiguration: MethodConfiguration,
                                userInfo: UserInfo
                               ): Future[AgoraEntity] =
    throw new NotImplementedError("postMethodConfig is not implemented for Azure.")
  override def getStatus(implicit executionContext: ExecutionContext): Future[SubsystemStatus] =
    throw new NotImplementedError("getStatus is not implemented for Azure.")

}

