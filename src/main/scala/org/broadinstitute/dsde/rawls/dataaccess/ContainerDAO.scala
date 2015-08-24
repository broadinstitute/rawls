package org.broadinstitute.dsde.rawls.dataaccess

import java.io.File

import akka.actor.ActorSystem
import com.google.api.client.util.store.FileDataStoreFactory
import com.typesafe.config.{ConfigFactory, Config}

/**
 * Created by mbemis on 8/19/15.
 */
class ContainerDAO(methodRepoServer: String, executionServiceServer: String) {

  implicit val system = ActorSystem("rawls")
  val conf = ConfigFactory.parseFile(new File("/etc/rawls.conf"))

  val workflowDAO = new GraphWorkflowDAO
  val workspaceDAO = new GraphWorkspaceDAO
  val entityDAO = new GraphEntityDAO
  val methodConfigDAO = new GraphMethodConfigurationDAO
  val methodRepoDAO = new HttpMethodRepoDAO(methodRepoServer)
  val executionServiceDAO = new HttpExecutionServiceDAO(executionServiceServer)
  val submissionDAO = new GraphSubmissionDAO(new GraphWorkflowDAO)
  val gcsDAO: GoogleCloudStorageDAO = new HttpGoogleCloudStorageDAO(
    conf.getConfig("gcs").getString("secrets"),
    new FileDataStoreFactory(new File(conf.getConfig("gcs").getString("dataStoreRoot"))),
    conf.getConfig("gcs").getString("redirectBaseURL")
  )

}
