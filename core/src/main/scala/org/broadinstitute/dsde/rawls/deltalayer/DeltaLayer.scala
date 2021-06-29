package org.broadinstitute.dsde.rawls.deltalayer

import cats.effect.{ContextShift, IO, Resource}
import com.google.cloud.bigquery.{Acl, BigQueryException, DatasetId}
import com.google.cloud.bigquery.Acl.Entity
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, SamPolicyWithNameAndEmail, SamResourceTypeNames, SamWorkspacePolicyNames, UserInfo, Workspace}
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DeltaLayer {
  @deprecated(message = "Use generateDatasetNameForWorkspace instead; one delta layer companion per workspace", since = "2021-06-22")
  def generateDatasetNameForReference(datasetReferenceId: UUID) = {
    "deltalayer_" + dashesToUnderscores(datasetReferenceId)
  }

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

}

class DeltaLayer(bqServiceFactory: GoogleBigQueryServiceFactory, deltaLayerWriter: DeltaLayerWriter, samDAO: SamDAO, clientEmail: WorkbenchEmail, deltaLayerStreamerEmail: WorkbenchEmail)
                (implicit protected val executionContext: ExecutionContext, implicit val contextShift: ContextShift[IO])
                extends LazyLogging {

  def bqCreate(googleProjectId: GoogleProjectId, datasetName: String,
                       datasetLabels: Map[String, String], aclBindings: Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]]): IO[DatasetId] = {
    val bqServiceResource = bqServiceFactory.getServiceForProject(googleProjectId)
    bqServiceResource.use(_.createDataset(datasetName, datasetLabels, aclBindings))
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
    val datasetLabels = calculateDatasetLabels(workspace)

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
    * @return status of dataset deletion
    */
  def deleteDataset(workspace: Workspace): IO[Boolean] = {
    val bqService = bqServiceFactory.getServiceForProject(workspace.googleProject)
    val datasetName = DeltaLayer.generateDatasetNameForWorkspace(workspace)
    bqService.use(_.deleteDataset(datasetName))
  }

  private def calculateDatasetAcl(samPolicies: Set[SamPolicyWithNameAndEmail]): Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]] = {
    val accessPolicies = Seq(SamWorkspacePolicyNames.owner, SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.reader)
    val defaultIamRoles = Map(Acl.Role.OWNER -> Seq((clientEmail, Acl.Entity.Type.USER)), Acl.Role.WRITER -> Seq((deltaLayerStreamerEmail, Acl.Entity.Type.USER)))
    val projectOwnerPolicy = samPolicies.filter(_.policyName == SamWorkspacePolicyNames.projectOwner).head.policy.memberEmails
    val filteredSamPolicies = samPolicies.filter(samPolicy => accessPolicies.contains(samPolicy.policyName)).map(_.email) ++ projectOwnerPolicy
    val samAclBindings = Acl.Role.READER -> filteredSamPolicies.map{ filteredSamPolicyEmail =>(filteredSamPolicyEmail, Acl.Entity.Type.GROUP) }.toSeq
    defaultIamRoles + samAclBindings
  }

  private def calculateDatasetLabels(workspace: Workspace): Map[String, String] =
    Map("workspace_id" -> workspace.workspaceId)

}
