package org.broadinstitute.dsde.rawls.deltalayer

import cats.effect.{ContextShift, IO}
import com.google.cloud.bigquery.Acl.Entity
import com.google.cloud.bigquery.{Acl, BigQueryException, DatasetId}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, SamPolicyWithNameAndEmail, SamResourceTypeNames, SamWorkspacePolicyNames, UserInfo, Workspace}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object DeltaLayer {
  /**
    * Google's doc on naming datasets:
    * the dataset name must be unique for each project. The dataset name can contain the following:
    * Up to 1,024 characters.
    * Letters (uppercase or lowercase), numbers, and underscores.
    * Note: In the Cloud Console, datasets that begin with an underscore are hidden from the navigation pane. You can
    * query tables and views in these datasets even though these datasets aren't visible.
    * Dataset names are case-sensitive: mydataset and MyDataset can coexist in the same project.
    * Dataset names cannot contain spaces or special characters such as -, &, @, or %.
    *
    * @param workspace Workspace for which to create a Delta Layer dataset name
    * @return name of the Delta Layer dataset for this workspace
    */
  def generateDatasetNameForWorkspace(workspace: Workspace): String = {
    "deltalayer_forworkspace_" + dashesToUnderscores(workspace.workspaceIdAsUUID)
  }

  /**
    * replace dashes in a uuid with underscores
    * @param uuid uuid in which to replace characters
    * @return string representation of the uuid after replacing characters
    */
  private def dashesToUnderscores(uuid: UUID): String = {
    uuid.toString.replace('-', '_')
  }

  /**
    * defines the labels attached to a Delta Layer dataset
    * @param workspace workspace in which the Delta Layer dataset will be created
    * @return correct labels for the dataset
    */
  private def calculateDatasetLabels(workspace: Workspace): Map[String, String] =
    Map("workspace_id" -> workspace.workspaceId)

}

class DeltaLayer(bqServiceFactory: GoogleBigQueryServiceFactory, deltaLayerWriter: DeltaLayerWriter, samDAO: SamDAO, clientEmail: WorkbenchEmail, deltaLayerStreamerEmail: WorkbenchEmail)
                (implicit protected val executionContext: ExecutionContext, implicit val contextShift: ContextShift[IO])
                extends LazyLogging {

  /**
    * Create a BigQuery dataset using the supplied parameters. Callers should use createDataset or createDatasetIfNotExist instead of
    * this method; this method exists as a standalone to ease with testing.
    *
    * @param googleProjectId the project in which to create the dataset
    * @param datasetName name for the created dataset
    * @param datasetLabels labels for the created dataset
    * @param aclBindings acls for the created dataset
    * @return DatasetId if created
    */
  def bqCreate(googleProjectId: GoogleProjectId, datasetName: String,
                       datasetLabels: Map[String, String], aclBindings: Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]]): IO[DatasetId] = {
    val bqServiceResource = bqServiceFactory.getServiceForProject(googleProjectId)
    bqServiceResource.use(_.createDataset(datasetName, datasetLabels, aclBindings))
    // the GoogleBigQueryService.createDataset method, inside workbench-libs, always returns null for the DatasetId's project.
    // the dataset is created in the correct project, but the return value is misleading; it results in "null" where
    // we expect something more informative, such as the log message "successfully created Delta Layer companion
    // BigQuery dataset my-dataset-name in project null" in createDatasetIfNotExist below.
  }

  /**
    * Creates the Delta Layer companion dataset for a given workspace, assigning it proper ACLs and labels
    * based on the workspace. Throws an error if the dataset already exists.
    * @param workspace workspace in which to create the companion dataset
    * @param userInfo user credentials for retrieving workspace ACLs
    * @return the created dataset's project and name
    */
  def createDataset(workspace: Workspace, userInfo: UserInfo): Future[DatasetId] = {
    val datasetName = DeltaLayer.generateDatasetNameForWorkspace(workspace)
    val datasetLabels = DeltaLayer.calculateDatasetLabels(workspace)

    samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, userInfo) flatMap { samPolicies =>
      val aclBindings = calculateDatasetAcl(samPolicies)
      bqCreate(workspace.googleProject, datasetName, datasetLabels, aclBindings).unsafeToFuture()
    }
  }

  /**
    * Creates the Delta Layer companion dataset for a given workspace, assigning it proper ACLs and labels
    * based on the workspace. Ignores errors if the dataset already exists.
    * @param workspace workspace in which to create the companion dataset
    * @param userInfo user credentials for retrieving workspace ACLs
    * @return a boolean indicating whether or not this call created the dataset, plus dataset's project and name
    */
  def createDatasetIfNotExist(workspace: Workspace, userInfo: UserInfo): Future[(Boolean, DatasetId)] = {
    createDataset(workspace, userInfo).map { datasetId =>
      // dataset was created
      logger.info(s"successfully created Delta Layer companion BigQuery dataset ${datasetId.getDataset} in project ${datasetId.getProject}")
      (true, datasetId)
    }.recover {
      case bqe: BigQueryException if bqe.getCode == 409 =>
        // dataset already exists
        val ds = DatasetId.of(workspace.googleProject.value, DeltaLayer.generateDatasetNameForWorkspace(workspace))
        logger.info(s"Delta Layer companion dataset already exists; createDatasetIfNotExist ignoring BigQuery 409 for ${ds.getProject}/${ds.getDataset}")
        (false, ds)
      case err  =>
        // some other error occurred; rethrow that error
        throw err
    }
  }

  /**
    * Deletes the Delta Layer companion dataset from a given workspace.
    * @param workspace the workspace from which to delete the companion dataset
    * @return true if dataset was deleted, false if it was not found (and throws on other errors)
    */
  def deleteDataset(workspace: Workspace): Future[Boolean] = {
    val bqService = bqServiceFactory.getServiceForProject(workspace.googleProject)
    val datasetName = DeltaLayer.generateDatasetNameForWorkspace(workspace)
    // the GoogleBigQueryService.deleteDataset call below is already 404-safe; no need to trap
    // for the case where the dataset doesn't exist
    bqService.use(_.deleteDataset(datasetName)).unsafeToFuture()
  }

  /**
    * Defines the ACLs to be placed on a Delta Layer dataset
    * @param samPolicies the pre-existing policies for the workspace in which the dataset will be created
    * @return the BigQuery ACLs to use for the dataset
    */
  private def calculateDatasetAcl(samPolicies: Set[SamPolicyWithNameAndEmail]): Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]] = {
    val accessPolicies = Seq(SamWorkspacePolicyNames.owner, SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.reader)
    val defaultIamRoles = Map(Acl.Role.OWNER -> Seq((clientEmail, Acl.Entity.Type.USER)), Acl.Role.WRITER -> Seq((deltaLayerStreamerEmail, Acl.Entity.Type.USER)))
    val projectOwnerPolicy = samPolicies.filter(_.policyName == SamWorkspacePolicyNames.projectOwner).head.policy.memberEmails
    val filteredSamPolicies = samPolicies.filter(samPolicy => accessPolicies.contains(samPolicy.policyName)).map(_.email) ++ projectOwnerPolicy
    val samAclBindings = Acl.Role.READER -> filteredSamPolicies.map{ filteredSamPolicyEmail =>(filteredSamPolicyEmail, Acl.Entity.Type.GROUP) }.toSeq
    defaultIamRoles + samAclBindings
  }

}
