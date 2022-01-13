package org.broadinstitute.dsde.rawls.entities.opensearch

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.client.{ApiException => DatarepoApiException}
import bio.terra.workspace.client.{ApiException => WorkspaceApiException}
import bio.terra.workspace.model.{DataRepoSnapshotResource, ReferenceTypeEnum, ResourceType}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.DataReferenceName

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.HttpHost
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.opensearch.client.RestClient
import org.opensearch.client.RestClientBuilder
import org.opensearch.client.RestHighLevelClient;

class OpenSearchEntityProviderBuilder()
                                     (implicit protected val executionContext: ExecutionContext)
  extends EntityProviderBuilder[OpenSearchEntityProvider] with LazyLogging {

  override def builds: TypeTag[OpenSearchEntityProvider] = typeTag[OpenSearchEntityProvider]

  // Establish credentials to use basic authentication.
  // Only for demo purposes. Don't specify your credentials in code.
  val credentialsProvider = new BasicCredentialsProvider
  credentialsProvider.setCredentials(AuthScope.ANY,
    new UsernamePasswordCredentials("admin", "admin"))

  class ClientCallback extends RestClientBuilder.HttpClientConfigCallback {
    override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder) = {
      httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
    }
  }

  override def build(requestArguments: EntityRequestArguments): Try[OpenSearchEntityProvider] = {
    // build high-level client, pass it in to the provider

    val builder = RestClient.builder(
      new HttpHost("host.docker.internal", 9200, "http"))
        .setHttpClientConfigCallback(new ClientCallback)

    val client = new RestHighLevelClient(builder)

    Success(new OpenSearchEntityProvider(requestArguments, client))
  }




}
