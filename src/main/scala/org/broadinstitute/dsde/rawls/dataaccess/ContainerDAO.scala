package org.broadinstitute.dsde.rawls.dataaccess

import java.io.File

import akka.actor.ActorSystem
import com.google.api.client.util.store.FileDataStoreFactory
import com.typesafe.config.{ConfigFactory, Config}

/**
 * Created by mbemis on 8/19/15.
 */

class ContainerDAO(conf: Config) extends AbstractContainerDAO {

  override val workflowDAO = new GraphWorkflowDAO
  override val workspaceDAO = new GraphWorkspaceDAO
  override val entityDAO = new GraphEntityDAO
  override val methodConfigDAO = new GraphMethodConfigurationDAO
  override val methodRepoDAO = new HttpMethodRepoDAO(conf.getConfig("methodrepo").getString("server"))
  override val executionServiceDAO = new HttpExecutionServiceDAO(conf.getConfig("executionservice").getString("server"))
  override val submissionDAO = new GraphSubmissionDAO(new GraphWorkflowDAO)
  override val gcsDAO: GoogleCloudStorageDAO = new HttpGoogleCloudStorageDAO(
    conf.getConfig("gcs").getString("secrets"),
    new FileDataStoreFactory(new File(conf.getConfig("gcs").getString("dataStoreRoot"))),
    conf.getConfig("gcs").getString("redirectBaseURL")
  )

}
