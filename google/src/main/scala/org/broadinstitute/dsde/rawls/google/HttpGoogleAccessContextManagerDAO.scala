package org.broadinstitute.dsde.rawls.google

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.accesscontextmanager.v1beta.model.AccessPolicy
import com.google.api.services.accesscontextmanager.v1beta.{AccessContextManager, AccessContextManagerScopes}
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, ServicePerimeterName}
import org.broadinstitute.dsde.rawls.util.FutureSupport

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class HttpGoogleAccessContextManagerDAO(clientEmail: String, pemFile: String, appName: String, serviceProject: String, override val workbenchMetricBaseName: String)
                                       (implicit val system: ActorSystem, implicit val executionContext: ExecutionContext)
  extends FutureSupport with GoogleUtilities {


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



  def addProjectToServicePerimeter(accessPolicy: AccessPolicy, servicePerimeterName: ServicePerimeterName, billingProjectNumber: String) = {
    implicit val service = GoogleInstrumentedService.AccessContextManager

    retryWhen500orGoogleError(() =>
    {
      val creds = getAccessContextManagerCredential
      val servicePerimeterFullyQualifiedName = s"${accessPolicy.getName}/servicePerimeters/${servicePerimeterName.value}"
      val accessContextManager = getAccessContextManager(creds)

      //`accessPolicies/{policy_id}/servicePerimeters/{service_perimeters_id}`
      val getRequest = accessContextManager.accessPolicies().servicePerimeters().get(servicePerimeterFullyQualifiedName)
      val servicePerimeter = executeGoogleRequest(getRequest)

      val existingProjects = Option(servicePerimeter.getStatus.getResources).getOrElse(new java.util.ArrayList[String]())
      existingProjects.add("projects/" + billingProjectNumber)
      servicePerimeter.getStatus.setResources(existingProjects)
      val patchRequest = accessContextManager.accessPolicies().servicePerimeters().patch(servicePerimeterFullyQualifiedName, servicePerimeter).setUpdateMask("status.resources")
      executeGoogleRequest(patchRequest)
    })

  }

  /**
    * At the time of writing this method, google's documentation says we will only ever have one access policy per organization.
    * However, the list api returns a list, so that's what we're returning too.
    *
    * @param organizationId must be formatted as `organizations/{numeric_org_id}`
    * @return
    */
  def listAccessPolicies(organizationId: String) = {
    implicit val service = GoogleInstrumentedService.AccessContextManager

    retryWhen500orGoogleError(() =>
    {
      val creds = getAccessContextManagerCredential
      val getRequest = getAccessContextManager(creds).accessPolicies().list().setParent(organizationId)
      val response = executeGoogleRequest(getRequest)
      Option(response.getAccessPolicies).map(_.asScala).getOrElse(Seq.empty)

    })
  }
}
