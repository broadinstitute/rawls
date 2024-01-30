package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.accesscontextmanager.v1.model.Operation
import com.google.api.services.accesscontextmanager.v1.{AccessContextManager, AccessContextManagerScopes}
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, GoogleUtilities}
import org.broadinstitute.dsde.rawls.model.ServicePerimeterName
import org.broadinstitute.dsde.rawls.util.FutureSupport

import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.config.Config

class DisabledHttpGoogleAccessContextManagerDAO(storageConfig: Config,
                                                override val workbenchMetricBaseName: String
                                               )(implicit val system: ActorSystem, implicit val executionContext: ExecutionContext)
  extends FutureSupport
    with GoogleUtilities
    with AccessContextManagerDAO
{

  val httpTransport: NetHttpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory: GsonFactory = GsonFactory.getDefaultInstance
  val accessContextScopes: Seq[String] = Seq(AccessContextManagerScopes.CLOUD_PLATFORM)

  val clientEmail: String = storageConfig.getString("serviceClientEmail")
  val serviceProject: String = storageConfig.getString("serviceProject")
  val appName: String = storageConfig.getString("appName")
  val pemFile: String = storageConfig.getString("pathToPem")

  def getAccessContextManagerCredential: Credential =
    throw new NotImplementedError("getAccessContextManagerCredential method is not implemented for Azure.")

  def getAccessContextManager(credential: Credential): AccessContextManager =
    throw new NotImplementedError("getAccessContextManager method is not implemented for Azure.")

  def overwriteProjectsInServicePerimeter(servicePerimeterName: ServicePerimeterName,
                                          billingProjectNumbers: Set[String]
                                         ): Future[Operation] =
    throw new NotImplementedError("overwriteProjectsInServicePerimeter method is not implemented for Azure.")

  override def pollOperation(operationId: String): Future[Operation] =
    throw new NotImplementedError("pollOperation method is not implemented for Azure.")
}

