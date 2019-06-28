package org.broadinstitute.dsde.rawls.google

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.accesscontextmanager.v1beta.model.{Operation, ServicePerimeter, ServicePerimeterConfig}
import com.google.api.services.accesscontextmanager.v1beta.{AccessContextManager, AccessContextManagerScopes}
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.rawls.model.ServicePerimeterName
import org.broadinstitute.dsde.rawls.util.FutureSupport

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class HttpGoogleAccessContextManagerDAO(clientEmail: String, pemFile: String, appName: String, serviceProject: String, override val workbenchMetricBaseName: String)
                                       (implicit val system: ActorSystem, implicit val executionContext: ExecutionContext)
  extends FutureSupport with GoogleUtilities with AccessContextManagerDAO {


  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance

  val accessContextScopes = Seq(AccessContextManagerScopes.CLOUD_PLATFORM)


  def getAccessContextManagerCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(accessContextScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

  def getAccessContextManager(credential: Credential): AccessContextManager = {
    new AccessContextManager.Builder(httpTransport, jsonFactory, credential).setApplicationName(appName).build()
  }

  def overwriteProjectsInServicePerimeter(servicePerimeterName: ServicePerimeterName, billingProjectNumbers: Seq[String]): Future[Operation] = {
    implicit val service = GoogleInstrumentedService.AccessContextManager

    retryWhen500orGoogleError(() =>
    {
      val creds = getAccessContextManagerCredential
      val accessContextManager = getAccessContextManager(creds)

      val fullyQualifiedProjects = billingProjectNumbers.map("projects/" + _)
      val servicePerimeter = new ServicePerimeter().setStatus(new ServicePerimeterConfig().setResources(fullyQualifiedProjects.asJava))

      val patchRequest = accessContextManager.accessPolicies().servicePerimeters().patch(servicePerimeterName.value, servicePerimeter).setUpdateMask("status.resources")
      executeGoogleRequest(patchRequest)
    })
  }

  override def pollOperation(operationId: String): Future[Operation] = {
    implicit val service = GoogleInstrumentedService.AccessContextManager

    val creds = getAccessContextManagerCredential
    val accessContextManager = getAccessContextManager(creds)

    retryWhen500orGoogleError(() => {
      executeGoogleRequest(accessContextManager.operations().get(operationId))
    })
  }
}
