package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import slick.dbio.Effect.{Read, Write}
import slick.profile.FixedSqlAction
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext

/**
 * Created by dvoet on 2/4/16.
 */
case class WorkspaceRecord(
  namespace: String,
  name: String,
  id: UUID,
  bucketName: String,
  createdDate: Timestamp,
  lastModified: Timestamp,
  createdBy: String,
  isLocked: Boolean
)
case class WorkspaceAttributeRecord(workspaceId: UUID, attributeId: Long)
case class WorkspaceAccessRecord(workspaceId: UUID, groupName: String, accessLevel: String)

trait WorkspaceComponent {
  this: DriverComponent
    with AttributeComponent
    with RawlsGroupComponent
    with RawlsUserComponent
    with EntityComponent
    with SubmissionComponent
    with WorkflowComponent
    with MethodConfigurationComponent =>

  import driver.api._

  class WorkspaceTable(tag: Tag) extends Table[WorkspaceRecord](tag, "WORKSPACE") {
    def id = column[UUID]("id", O.PrimaryKey)
    def namespace = column[String]("namespace", O.Length(254))
    def name = column[String]("name", O.Length(254))
    def bucketName = column[String]("bucket_name")
    def createdDate = column[Timestamp]("created_date", O.Default(defaultTimeStamp))
    def lastModified = column[Timestamp]("last_modified", O.Default(defaultTimeStamp))
    def createdBy = column[String]("created_by")
    def isLocked = column[Boolean]("is_locked")

    def uniqueNamespaceName = index("IDX_WS_UNIQUE_NAMESPACE_NAME", (namespace, name), unique = true)

    def * = (namespace, name, id, bucketName, createdDate, lastModified, createdBy, isLocked) <> (WorkspaceRecord.tupled, WorkspaceRecord.unapply)
  }

  class WorkspaceAttributeTable(tag: Tag) extends Table[WorkspaceAttributeRecord](tag, "WORKSPACE_ATTRIBUTE") {
    def attributeId = column[Long]("attribute_id", O.PrimaryKey)
    def workspaceId = column[UUID]("workspace_id")

    def workspace = foreignKey("FK_WS_ATTR_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def attribute = foreignKey("FK_WS_ATTR_ATTRIBUTE", attributeId, attributeQuery)(_.id)

    def * = (workspaceId, attributeId) <> (WorkspaceAttributeRecord.tupled, WorkspaceAttributeRecord.unapply)
  }

  class WorkspaceAccessTable(tag: Tag) extends Table[WorkspaceAccessRecord](tag, "WORKSPACE_ACCESS") {
    def groupName = column[String]("group_name", O.Length(254))
    def workspaceId = column[UUID]("workspace_id")
    def accessLevel = column[String]("access_level", O.Length(254))

    def workspace = foreignKey("FK_WS_ACCESS_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def group = foreignKey("FK_WS_ACCESS_GROUP", groupName, rawlsGroupQuery)(_.groupName)

    def accessPrimaryKey = primaryKey("PK_WORKSPACE_ACCESS", (workspaceId, accessLevel))

    def * = (workspaceId, groupName, accessLevel) <> (WorkspaceAccessRecord.tupled, WorkspaceAccessRecord.unapply)
  }

  protected val workspaceAttributeQuery = TableQuery[WorkspaceAttributeTable]
  protected val workspaceAccessQuery = TableQuery[WorkspaceAccessTable]

  object workspaceQuery extends TableQuery(new WorkspaceTable(_)) {
    private type WorkspaceQueryType = driver.api.Query[WorkspaceTable, WorkspaceRecord, Seq]

    def save(workspace: Workspace): ReadWriteAction[Workspace] = {
      validateUserDefinedString(workspace.namespace)
      validateUserDefinedString(workspace.name)
      workspace.attributes.keys.foreach { value =>
        validateUserDefinedString(value)
        validateAttributeName(value)
      }

      val workspaceRecord = marshalWorkspace(workspace)

      workspaceQuery insertOrUpdate workspaceRecord andThen {
        DBIO.seq(workspace.accessLevels.map { case (accessLevel, group) =>
          workspaceAccessQuery insertOrUpdate WorkspaceAccessRecord(workspaceRecord.id, group.groupName.value, accessLevel.toString)
        }.toSeq: _*)

      } andThen {
        workspaceAttributes(workspaceRecord.id).result.flatMap { attributeRecords =>
          // clear existing attributes, any concurrency locking should be handled at the workspace level
          val deleteActions = deleteWorkspaceAttributes(attributeRecords)

          // insert attributes
          val insertActions = workspace.attributes.flatMap { case (name, attribute) =>
            attributeQuery.insertAttributeRecords(name, attribute, workspaceRecord.id).map(_.flatMap{ attrId =>
              insertWorkspaceAttributeMapping(workspaceRecord, attrId)
            })
          }

          DBIO.seq(deleteActions ++ insertActions:_*)
        }
      } map(_ => workspace)
    }

    def findByName(workspaceName: WorkspaceName): ReadAction[Option[Workspace]] = {
      loadWorkspace(findByNameQuery(workspaceName))
    }

    def findById(workspaceId: String): ReadAction[Option[Workspace]] = {
      loadWorkspace(findByIdQuery(UUID.fromString(workspaceId)))
    }

    def delete(workspaceName: WorkspaceName): ReadWriteAction[Boolean] = {
      uniqueResult[WorkspaceRecord](findByNameQuery(workspaceName)).flatMap {
        case None => DBIO.successful(false)
        case Some(workspaceRecord) =>
          workspaceAttributes(workspaceRecord.id).result.flatMap(recs => DBIO.seq(deleteWorkspaceAttributes(recs):_*)) flatMap { _ =>
            //should we be deleting ALL workspace-related things inside of this method?
            workspaceAccessQuery.filter(_.workspaceId === workspaceRecord.id).delete
          } flatMap { _ =>
            findByIdQuery(workspaceRecord.id).delete
          } map { count =>
            count > 0
          }
      }
    }

    def lock(workspaceName: WorkspaceName): ReadWriteAction[Int] = {
      findByNameQuery(workspaceName).map(_.isLocked).update(true)
    }

    def unlock(workspaceName: WorkspaceName): ReadWriteAction[Int] = {
      findByNameQuery(workspaceName).map(_.isLocked).update(false)
    }
    
    def listEmailsAndAccessLevel(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[(String, WorkspaceAccessLevel)]] = {
      val accessAndUserEmail = (for {
        access <- workspaceAccessQuery if (access.workspaceId === workspaceContext.workspaceId)
        group <- rawlsGroupQuery if (access.groupName === group.groupName)
        userGroup <- groupUsersQuery if (group.groupName === userGroup.groupName)
        user <- rawlsUserQuery if (user.userSubjectId === userGroup.userSubjectId)
      } yield (access, user)).map { case (access, user) => (access.accessLevel, user.userEmail) }

      val accessAndSubGroupEmail = (for {
        access <- workspaceAccessQuery if (access.workspaceId === workspaceContext.workspaceId)
        group <- rawlsGroupQuery if (access.groupName === group.groupName)
        subGroupGroup <- groupSubgroupsQuery if (group.groupName === subGroupGroup.parentGroupName)
        subGroup <- rawlsGroupQuery if (subGroup.groupName === subGroupGroup.childGroupName)
      } yield (access, subGroup)).map { case (access, subGroup) => (access.accessLevel, subGroup.groupEmail) }

      (accessAndUserEmail union accessAndSubGroupEmail).result.map(_.map { case (access, email) => 
        (email, WorkspaceAccessLevels.withName(access))
      })
    }

    def deleteWorkspaceAccessReferences(workspaceId: UUID) = {
      workspaceAccessQuery.filter(_.workspaceId === workspaceId).delete
    }

    def deleteWorkspaceEntityAttributes(workspaceId: UUID) = {
      entityQuery.filter(_.workspaceId === workspaceId).result flatMap { recs =>
        DBIO.sequence(recs.map(e => entityQuery.entityAttributes(e.id).result.flatMap { recs =>
          val attributeDeletes = entityQuery.deleteEntityAttributes(recs)
          DBIO.seq(attributeDeletes:_*)
        }))
      }
    }

    def deleteWorkspaceEntities(workspaceId: UUID) = {
      entityQuery.filter(_.workspaceId === workspaceId).delete
    }

    def deleteWorkspaceSubmissions(workspaceId: UUID) = {
      submissionQuery.filter(_.workspaceId === workspaceId).result flatMap { result =>
        DBIO.seq(result.map(sub => submissionQuery.deleteSubmissionAction(sub.id)).toSeq:_*)
      }
    }

    def deleteWorkspaceMethodConfigs(workspaceId: UUID) = {
      methodConfigurationQuery.filter(_.workspaceId === workspaceId).result flatMap { result =>
        DBIO.seq(result.map(mc => methodConfigurationQuery.deleteMethodConfigurationAction(mc.id)).toSeq:_*)
      }
    }

    private def workspaceAttributes(workspaceId: UUID) = for {
      workspaceAttrRec <- workspaceAttributeQuery if workspaceAttrRec.workspaceId === workspaceId
      attributeRec <- attributeQuery if workspaceAttrRec.attributeId === attributeRec.id
    } yield (attributeRec)

    private def workspaceAttributesWithReferences(workspaceId: UUID) = {
      workspaceAttributes(workspaceId) joinLeft entityQuery on (_.valueEntityRef === _.id)
    }

    private def deleteWorkspaceAttributes(attributeRecords: Seq[AttributeRecord]) = {
      Seq(deleteWorkspaceAttributeMappings(attributeRecords), attributeQuery.deleteAttributeRecords(attributeRecords))
    }

    private def insertWorkspaceAttributeMapping(workspaceRecord: WorkspaceRecord, attrId: Long): FixedSqlAction[Int, driver.api.NoStream, Write] = {
      workspaceAttributeQuery += WorkspaceAttributeRecord(workspaceRecord.id, attrId)
    }

    private def deleteWorkspaceAttributeMappings(attributeRecords: Seq[AttributeRecord]): FixedSqlAction[Int, driver.api.NoStream, Write] = {
      workspaceAttributeQuery.filter(_.attributeId inSetBind (attributeRecords.map(_.id))).delete
    }

    private def findByNameQuery(workspaceName: WorkspaceName): WorkspaceQueryType = {
      filter(rec => rec.namespace === workspaceName.namespace && rec.name === workspaceName.name)
    }

    def findByIdQuery(workspaceId: UUID): WorkspaceQueryType = {
      filter(_.id === workspaceId)
    }
    
    def listPermissionPairsForGroups(groups: Set[RawlsGroupRef]): ReadAction[Seq[WorkspacePermissionsPair]] = {
      val query = for {
        accessLevel <- workspaceAccessQuery if (accessLevel.groupName.inSetBind(groups.map(_.groupName.value)))
        workspace <- workspaceQuery if (workspace.id === accessLevel.workspaceId)
      } yield (workspace, accessLevel)
      query.result.map(_.map { case (workspace, accessLevel) => WorkspacePermissionsPair(workspace.id.toString(), WorkspaceAccessLevels.withName(accessLevel.accessLevel)) })
    }

    private def loadWorkspace(lookup: WorkspaceQueryType): DBIOAction[Option[Workspace], NoStream, Read] = {
      uniqueResult[WorkspaceRecord](lookup).flatMap {
        case None => DBIO.successful(None)
        case Some(workspaceRec) =>
          for (
            attributes <- loadAttributes(workspaceRec.id);
            accessGroups <- loadAccessGroupRefs(workspaceRec.id)
          ) yield Option(unmarshalWorkspace(workspaceRec, attributes, accessGroups))
      }
    }

    private def marshalWorkspace(workspace: Workspace) = {
      WorkspaceRecord(workspace.namespace, workspace.name, UUID.fromString(workspace.workspaceId), workspace.bucketName, new Timestamp(workspace.createdDate.getMillis), new Timestamp(workspace.lastModified.getMillis), workspace.createdBy, workspace.isLocked)
    }

    private def unmarshalWorkspace(workspaceRec: WorkspaceRecord, attributes: Map[String, Attribute], accessGroups: Map[WorkspaceAccessLevel, RawlsGroupRef]): Workspace = {
      Workspace(workspaceRec.namespace, workspaceRec.name, workspaceRec.id.toString, workspaceRec.bucketName, new DateTime(workspaceRec.createdDate), new DateTime(workspaceRec.lastModified), workspaceRec.createdBy, attributes, accessGroups, workspaceRec.isLocked)
    }

    private def loadAttributes(workspaceId: UUID) = {
      workspaceAttributesWithReferences(workspaceId).result.map(attributeQuery.unmarshalAttributes)
    }

    private def loadAccessGroupRefs(workspaceId: UUID) = {
      (workspaceAccessQuery filter (_.workspaceId === workspaceId)).result.map(unmarshalRawlsGroupRefs)
    }

    private def unmarshalRawlsGroupRefs(workspaceAccessRecords: Seq[WorkspaceAccessRecord]): Map[WorkspaceAccessLevel, RawlsGroupRef] = {
      workspaceAccessRecords.map(rec => WorkspaceAccessLevels.withName(rec.accessLevel) -> RawlsGroupRef(RawlsGroupName(rec.groupName))).toMap
    }
  }
}
