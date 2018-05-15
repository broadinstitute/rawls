package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.{Date, UUID}

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import slick.jdbc.JdbcProfile

case class MethodConfigurationRecord(id: Long,
                                     namespace: String,
                                     name: String,
                                     workspaceId: UUID,
                                     rootEntityType: String,
                                     methodUri: String,
                                     methodConfigVersion: Int,
                                     deleted: Boolean,
                                     deletedDate: Option[Timestamp])

case class MethodConfigurationInputRecord(methodConfigId: Long, id: Long, key: String, value: String)

case class MethodConfigurationOutputRecord(methodConfigId: Long, id: Long, key: String, value: String)

case class MethodConfigurationPrereqRecord(methodConfigId: Long, id: Long, key: String, value: String)

trait MethodConfigurationComponent {
  this: DriverComponent with WorkspaceComponent =>

  import driver.api._

  class MethodConfigurationTable(tag: Tag) extends Table[MethodConfigurationRecord](tag, "METHOD_CONFIG") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def namespace = column[String]("NAMESPACE", O.Length(254))
    def name = column[String]("NAME", O.Length(254))
    def workspaceId = column[UUID]("WORKSPACE_ID")
    def rootEntityType = column[String]("ROOT_ENTITY_TYPE", O.Length(254))
    def methodUri = column[String]("METHOD_URI")
    def methodConfigVersion = column[Int]("METHOD_CONFIG_VERSION")
    def deleted = column[Boolean]("DELETED")
    def deletedDate = column[Option[Timestamp]]("deleted_date")

    def * = (id, namespace, name, workspaceId, rootEntityType, methodUri, methodConfigVersion, deleted, deletedDate) <> (MethodConfigurationRecord.tupled, MethodConfigurationRecord.unapply)

    def workspace = foreignKey("FK_MC_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def namespaceNameIdx = index("IDX_CONFIG", (workspaceId, namespace, name, methodConfigVersion), unique = true)
  }

  class MethodConfigurationInputTable(tag: Tag) extends Table[MethodConfigurationInputRecord](tag, "METHOD_CONFIG_INPUT") {
    def methodConfigId = column[Long]("METHOD_CONFIG_ID")
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def key = column[String]("KEY", O.Length(254))
    def value = column[String]("VALUE")

    def * = (methodConfigId, id, key, value) <> (MethodConfigurationInputRecord.tupled, MethodConfigurationInputRecord.unapply)

    def methodConfig = foreignKey("FK_MC_INPUT", methodConfigId, methodConfigurationQuery)(_.id)
    def configKeyIdx = index("IDX_MC_INPUT", (methodConfigId, key), unique = true)
  }

  class MethodConfigurationOutputTable(tag: Tag) extends Table[MethodConfigurationOutputRecord](tag, "METHOD_CONFIG_OUTPUT") {
    def methodConfigId = column[Long]("METHOD_CONFIG_ID")
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def key = column[String]("KEY", O.Length(254))
    def value = column[String]("VALUE")

    def * = (methodConfigId, id, key, value) <> (MethodConfigurationOutputRecord.tupled, MethodConfigurationOutputRecord.unapply)

    def methodConfig = foreignKey("FK_MC_OUTPUT", methodConfigId, methodConfigurationQuery)(_.id)
    def configKeyIdx = index("IDX_MC_OUTPUT", (methodConfigId, key), unique = true)
  }

  class MethodConfigurationPrereqTable(tag: Tag) extends Table[MethodConfigurationPrereqRecord](tag, "METHOD_CONFIG_PREREQ") {
    def methodConfigId = column[Long]("METHOD_CONFIG_ID")
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def key = column[String]("KEY", O.Length(254))
    def value = column[String]("VALUE")

    def * = (methodConfigId, id, key, value) <> (MethodConfigurationPrereqRecord.tupled, MethodConfigurationPrereqRecord.unapply)

    def methodConfig = foreignKey("FK_MC_PREREQ", methodConfigId, methodConfigurationQuery)(_.id)
    def configKeyIdx = index("IDX_MC_PREREQ", (methodConfigId, key), unique = true)
  }

  protected val methodConfigurationInputQuery = TableQuery[MethodConfigurationInputTable]
  protected val methodConfigurationOutputQuery = TableQuery[MethodConfigurationOutputTable]
  protected val methodConfigurationPrereqQuery = TableQuery[MethodConfigurationPrereqTable]

  object methodConfigurationQuery extends TableQuery(new MethodConfigurationTable(_)) {

    private type MethodConfigurationQueryType = driver.api.Query[MethodConfigurationTable, MethodConfigurationRecord, Seq]
    private type MethodConfigurationInputQueryType = driver.api.Query[MethodConfigurationInputTable, MethodConfigurationInputRecord, Seq]
    private type MethodConfigurationOutputQueryType = driver.api.Query[MethodConfigurationOutputTable, MethodConfigurationOutputRecord, Seq]
    private type MethodConfigurationPrereqQueryType = driver.api.Query[MethodConfigurationPrereqTable, MethodConfigurationPrereqRecord, Seq]

    /*
      the core methods
     */

    //For readability in tests and whatnot.
    def create(workspaceContext: SlickWorkspaceContext, newMethodConfig: MethodConfiguration): ReadWriteAction[MethodConfiguration] = upsert(workspaceContext, newMethodConfig)

    //Looks for an existing method config with the same namespace and name.
    //If it exists, archives it.
    //In either case, saves the new method configuration.
    def upsert(workspaceContext: SlickWorkspaceContext, newMethodConfig: MethodConfiguration): ReadWriteAction[MethodConfiguration] = {
      uniqueResult[MethodConfigurationRecord](findActiveByName(workspaceContext.workspaceId, newMethodConfig.namespace, newMethodConfig.name)) flatMap {
        //note that we ignore the version in newMethodConfig, as the version is defined by how many MCs have ever lived at the target location
        case None =>
          saveWithoutArchive(workspaceContext, newMethodConfig)
        case Some(currentMethodConfigRec) =>
          archive(workspaceContext, currentMethodConfigRec) andThen
            saveWithoutArchive(workspaceContext, newMethodConfig, currentMethodConfigRec.methodConfigVersion + 1)
      }
    } map { _ => newMethodConfig }

    //"Update" the method config at oldMethodConfig[name|namespace] to be newMethodConfig, changing the name and namespace if necessary
    //It's like a rename and upsert all in one.
    //The MC at oldMethodConfig[name|namespace] MUST exist. It will be archived.
    //If there's a method config at the location specified in newMethodConfig that will be archived too.
    def update(workspaceContext: SlickWorkspaceContext, oldMethodConfigNamespace: String, oldMethodConfigName: String, newMethodConfig: MethodConfiguration): ReadWriteAction[MethodConfiguration] = {
      //Look up the MC we're moving.
      uniqueResult[MethodConfigurationRecord](findActiveByName(workspaceContext.workspaceId, oldMethodConfigNamespace, oldMethodConfigName)) flatMap {
        case None => DBIO.failed(new RawlsException(s"Can't find method config $oldMethodConfigNamespace/$oldMethodConfigName."))
        case Some(currentMethodConfigRec) =>
          //if we're moving the MC to a new location, archive the one at the old location.
          val maybeArchive = if( currentMethodConfigRec.namespace != newMethodConfig.namespace || currentMethodConfigRec.name != newMethodConfig.name ) {
            archive(workspaceContext, currentMethodConfigRec)
          } else {
            DBIO.successful(())
          }
          //Once we've archived the old location we just upsert on top of the new location.
          maybeArchive andThen upsert(workspaceContext, newMethodConfig)
      }
    } map { _ => newMethodConfig }

    def archive(workspaceContext: SlickWorkspaceContext, methodConfigRec: MethodConfigurationRecord): ReadWriteAction[Int] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        hideMethodConfigurationAction(methodConfigRec.id, methodConfigRec.name)
    }

    //Adds the new method configuration with the given version.
    //It's up to you to call archive on the previous one!
    private def saveWithoutArchive(workspaceContext: SlickWorkspaceContext, methodConfig: MethodConfiguration, version: Int = 1) = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        (methodConfigurationQuery returning methodConfigurationQuery.map(_.id) +=
          marshalMethodConfig(workspaceContext.workspaceId, methodConfig.copy(methodConfigVersion=version))) flatMap { configId =>
            saveMaps(methodConfig, configId)
          }
    }


    private def saveMaps(methodConfig: MethodConfiguration, configId: Long) = {
      val prerequisites = methodConfig.prerequisites.map { case (key, value) => marshalConfigPrereq(configId, key, value) }
      val inputs = methodConfig.inputs.map { case (key, value) => marshalConfigInput(configId, key, value) }
      val outputs = methodConfig.outputs.map{ case (key, value) => marshalConfigOutput(configId, key, value) }

      (methodConfigurationPrereqQuery ++= prerequisites) andThen
        (methodConfigurationInputQuery ++= inputs) andThen
        (methodConfigurationOutputQuery ++= outputs)
    }

    def get(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String): ReadAction[Option[MethodConfiguration]] = {
      loadMethodConfigurationByName(workspaceContext.workspaceId, methodConfigurationNamespace, methodConfigurationName)
    }

    def get(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationId: Long): ReadAction[Option[MethodConfiguration]] = {
      loadMethodConfigurationById(methodConfigurationId)
    }

    // Delete a method - actually just "hides" the method - used when deleting a method from a workspace
    def delete(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String): ReadWriteAction[Boolean] = {
      workspaceQuery.updateLastModified(workspaceContext.workspaceId) andThen
        uniqueResult[MethodConfigurationRecord](findActiveByName(workspaceContext.workspaceId, methodConfigurationNamespace, methodConfigurationName)) flatMap {
          case None => DBIO.successful(false)
          case Some(methodConfigRec) => {
            hideMethodConfigurationAction(methodConfigRec.id, methodConfigurationName)
          } map { count =>
            count > 0
          }
        }
    }

    def hideMethodConfigurationAction(id: Long, methodConfigName: String): ReadWriteAction[Int] = {
      val currentTime = new Timestamp(new Date().getTime)
      findById(id).map(rec => (rec.deleted, rec.name, rec.deletedDate))
        .update(true, renameForHiding(id, methodConfigName), Some(currentTime))
    }

    // performs actual deletion (not hiding) of everything that depends on a method configuration
    object MethodConfigurationDependenciesDeletionQuery extends RawSqlQuery {
      val driver: JdbcProfile = MethodConfigurationComponent.this.driver

      def deleteAction(workspaceId: UUID): WriteAction[Seq[Int]] = {
        val tables: Seq[String] = Seq("METHOD_CONFIG_INPUT", "METHOD_CONFIG_OUTPUT", "METHOD_CONFIG_PREREQ")

        DBIO.sequence(tables map { table =>
          sqlu"""delete t from #$table as t
                inner join METHOD_CONFIG as mc on mc.id=t.method_config_id
                where mc.workspace_id=$workspaceId"""
        })
      }
    }

    // performs actual deletion (not hiding) of method configuration
    def deleteFromDb(workspaceId: UUID): WriteAction[Int] = {
      MethodConfigurationDependenciesDeletionQuery.deleteAction(workspaceId) andThen
        filter(_.workspaceId === workspaceId).delete
    }

    // standard listing: does not include "deleted" MCs
    def listActive(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[MethodConfigurationShort]] = {
      findActiveByWorkspace(workspaceContext.workspaceId).result.map(recs => recs.map(rec => unmarshalMethodConfigToShort(rec)))
    }

    /*
      find helper methods
     */

    def findById(id: Long): MethodConfigurationQueryType = {
      filter(rec => rec.id === id)
    }

    private def findByWorkspace(workspaceId: UUID): MethodConfigurationQueryType = {
      filter(rec => rec.workspaceId === workspaceId)
    }

    private def findActiveByWorkspace(workspaceId: UUID): MethodConfigurationQueryType = {
      findByWorkspace(workspaceId).filterNot(_.deleted)
    }

    def findActiveByName(workspaceId: UUID, methodNamespace: String, methodName: String): MethodConfigurationQueryType = {
      filter(rec => rec.namespace === methodNamespace && rec.name === methodName && rec.workspaceId === workspaceId && !rec.deleted)
    }

    /*
      load helper methods
     */

    def loadMethodConfiguration(methodConfigRec: MethodConfigurationRecord): ReadAction[MethodConfiguration] = {
      for {
        inputs <- loadInputs(methodConfigRec.id)
        outputs <- loadOutputs(methodConfigRec.id)
        prereqs <- loadPrereqs(methodConfigRec.id)
      } yield unmarshalMethodConfig(methodConfigRec, inputs, outputs, prereqs)
    }

    def loadMethodConfigurationByName(workspaceId: UUID, methodConfigNamespace: String, methodConfigName: String): ReadAction[Option[MethodConfiguration]] = {
      uniqueResult[MethodConfigurationRecord](findActiveByName(workspaceId, methodConfigNamespace, methodConfigName)) flatMap {
        case None => DBIO.successful(None)
        case Some(methodConfigRec) =>
          loadMethodConfiguration(methodConfigRec) map (Some(_))
      }
    }

    def loadMethodConfigurationById(id: Long): ReadAction[Option[MethodConfiguration]] = {
      uniqueResult[MethodConfigurationRecord](findById(id)) flatMap {
        case None => DBIO.successful(None)
        case Some(methodConfigRec) =>
          loadMethodConfiguration(methodConfigRec) map (Some(_))
      }
    }


    private def loadInputs(methodConfigId: Long) = {
      (methodConfigurationInputQuery filter (_.methodConfigId === methodConfigId)).result.map(unmarshalConfigInputs)
    }

    private def loadOutputs(methodConfigId: Long) = {
      (methodConfigurationOutputQuery filter (_.methodConfigId === methodConfigId)).result.map(unmarshalConfigOutputs)
    }

    private def loadPrereqs(methodConfigId: Long) = {
      (methodConfigurationPrereqQuery filter (_.methodConfigId === methodConfigId)).result.map(unmarshalConfigPrereqs)
    }

    /*
      the marshal/unmarshal helper methods
     */

    private def marshalMethodConfig(workspaceId: UUID, methodConfig: MethodConfiguration) = {
      MethodConfigurationRecord(0, methodConfig.namespace, methodConfig.name, workspaceId, methodConfig.rootEntityType, methodConfig.methodRepoMethod.methodUri, methodConfig.methodConfigVersion, methodConfig.deleted, methodConfig.deletedDate.map( d => new Timestamp(d.getMillis)))
    }

    def unmarshalMethodConfig(methodConfigRec: MethodConfigurationRecord, inputs: Map[String, AttributeString], outputs: Map[String, AttributeString], prereqs: Map[String, AttributeString]): MethodConfiguration = {
      MethodConfiguration(methodConfigRec.namespace, methodConfigRec.name, methodConfigRec.rootEntityType, prereqs, inputs, outputs, MethodRepoMethod.fromUri(methodConfigRec.methodUri), methodConfigRec.methodConfigVersion, methodConfigRec.deleted, methodConfigRec.deletedDate.map(ts => new DateTime(ts)))
    }

    private def unmarshalMethodConfigToShort(methodConfigRec: MethodConfigurationRecord): MethodConfigurationShort = {
      MethodConfigurationShort(methodConfigRec.name, methodConfigRec.rootEntityType, MethodRepoMethod.fromUri(methodConfigRec.methodUri), methodConfigRec.namespace)
    }

    private def marshalConfigInput(configId: Long, key: String, value: AttributeString) = {
      MethodConfigurationInputRecord(configId, 0, key, value.value)
    }

    private def unmarshalConfigInputs(inputRecords: Seq[MethodConfigurationInputRecord]): Map[String, AttributeString] = {
      inputRecords.map(rec => rec.key -> AttributeString(rec.value)).toMap
    }

    private def marshalConfigOutput(configId: Long, key: String, value: AttributeString) = {
      MethodConfigurationOutputRecord(configId, 0, key, value.value)
    }

    private def unmarshalConfigOutputs(outputRecords: Seq[MethodConfigurationOutputRecord]): Map[String, AttributeString] = {
      outputRecords.map(rec => rec.key -> AttributeString(rec.value)).toMap
    }

    private def marshalConfigPrereq(configId: Long, key: String, value: AttributeString) = {
      MethodConfigurationPrereqRecord(configId, 0, key, value.value)
    }

    private def unmarshalConfigPrereqs(prereqRecords: Seq[MethodConfigurationPrereqRecord]): Map[String, AttributeString] = {
      prereqRecords.map(rec => rec.key -> AttributeString(rec.value)).toMap
    }
  }
}
