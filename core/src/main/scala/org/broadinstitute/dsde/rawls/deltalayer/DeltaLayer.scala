package org.broadinstitute.dsde.rawls.deltalayer

import cats.effect.{ContextShift, IO}
import com.google.cloud.bigquery.{Acl, DatasetId}
import com.google.cloud.bigquery.Acl.Entity
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.model.{SamPolicyWithNameAndEmail, SamResourceTypeNames, SamWorkspacePolicyNames, UserInfo, Workspace}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.concurrent.ExecutionContext

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
                (implicit protected val executionContext: ExecutionContext, implicit val contextShift: ContextShift[IO]) {

  /**
    * Creates the Delta Layer companion dataset for a given workspace, assigning it proper ACLs and labels
    * based on the workspace.
    * @param workspace workspace in which to create the companion dataset
    * @param userInfo user credentials for retrieving workspace ACLs
    * @return the created dataset's project and name
    */
  def createDataset(workspace: Workspace, userInfo: UserInfo): IO[DatasetId] = {
    for {
      samPolicies <- IO.fromFuture(IO(samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, workspace.workspaceId, userInfo)))
      bqService = bqServiceFactory.getServiceForProject(workspace.googleProject)
      datasetName = DeltaLayer.generateDatasetNameForWorkspace(workspace)
      datasetLabels = Map("workspace_id" -> workspace.workspaceId)
      aclBindings = calculateDatasetAcl(samPolicies)
      datasetId <- bqService.use(_.createDataset(datasetName, datasetLabels, aclBindings))
    } yield {
      // the DatasetId object contains project name and dataset name
      datasetId
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

}
