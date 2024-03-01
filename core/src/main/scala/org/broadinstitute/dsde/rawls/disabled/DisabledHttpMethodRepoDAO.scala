package org.broadinstitute.dsde.rawls.disabled

import akka.actor.ActorSystem
import akka.http.scaladsl.client.RequestBuilding
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.MethodRepoDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.HttpClientUtilsGzipInstrumented

import scala.concurrent.{ExecutionContext, Future}

class DisabledHttpMethodRepoDAO(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends MethodRepoDAO {
  override def getMethodConfig(namespace: String,
                               name: String,
                               version: Int,
                               userInfo: UserInfo
  ): Future[Option[AgoraEntity]] =
    throw new NotImplementedError("getMethodConfig is not implemented for Azure.")
  override def getMethod(method: MethodRepoMethod, userInfo: UserInfo): Future[Option[WDL]] =
    throw new NotImplementedError("getMethod is not implemented for Azure.")
  override def postMethodConfig(namespace: String,
                                name: String,
                                methodConfiguration: MethodConfiguration,
                                userInfo: UserInfo
  ): Future[AgoraEntity] =
    throw new NotImplementedError("postMethodConfig is not implemented for Azure.")
  override def getStatus(implicit executionContext: ExecutionContext): Future[SubsystemStatus] =
    Future.successful(SubsystemStatus(ok = true, None))
}
