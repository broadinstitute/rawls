package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import slick.driver.JdbcDriver
import org.joda.time.DateTime

case class MethodConfigurationRecord(id: Long,
                                     namespace: String,
                                     name: String,
                                     workspaceId: UUID,
                                     rootEntityType: String,
                                     methodNamespace: String,
                                     methodName: String,
                                     methodVersion: Int,
                                     deleted: Boolean)

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
    def methodNamespace = column[String]("METHOD_NAMESPACE")
    def methodName = column[String]("METHOD_NAME")
    def methodVersion = column[Int]("METHOD_VERSION")
    def deleted = column[Boolean]("DELETED")

    def * = (id, namespace, name, workspaceId, rootEntityType, methodNamespace, methodName, methodVersion, deleted) <> (MethodConfigurationRecord.tupled, MethodConfigurationRecord.unapply)

    def workspace = foreignKey("FK_MC_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def namespaceNameIdx = index("IDX_CONFIG", (workspaceId, namespace, name), unique = true)
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
    def save(workspaceContext: SlickWorkspaceContext, methodConfig: MethodConfiguration): ReadWriteAction[MethodConfiguration] = {

      def saveMaps(configId: Long) = {
        val prerequisites = methodConfig.prerequisites.map { case (key, value) => marshalConfigPrereq(configId, key, value) }
        val inputs = methodConfig.inputs.map { case (key, value) => marshalConfigInput(configId, key, value) }
        val outputs = methodConfig.outputs.map{ case (key, value) => marshalConfigOutput(configId, key, value) }

        (methodConfigurationPrereqQuery ++= prerequisites) andThen
          (methodConfigurationInputQuery ++= inputs) andThen
            (methodConfigurationOutputQuery ++= outputs)
      }

      uniqueResult[MethodConfigurationRecord](findByName(workspaceContext.workspaceId, methodConfig.namespace, methodConfig.name)) flatMap {
        case None =>
          val configInsert = (methodConfigurationQuery returning methodConfigurationQuery.map(_.id) +=  marshalMethodConfig(workspaceContext.workspaceId, methodConfig))
          configInsert flatMap { configId =>
            saveMaps(configId)
          }
        case Some(methodConfigRec) =>
          findInputsByConfigId(methodConfigRec.id).delete andThen
            findOutputsByConfigId(methodConfigRec.id).delete andThen
            findPrereqsByConfigId(methodConfigRec.id).delete andThen
            findById(methodConfigRec.id).map(rec => (rec.methodNamespace, rec.methodName, rec.methodVersion)).update(methodConfig.methodRepoMethod.methodNamespace, methodConfig.methodRepoMethod.methodName, methodConfig.methodRepoMethod.methodVersion) andThen
            saveMaps(methodConfigRec.id) andThen
            findByName(workspaceContext.workspaceId, methodConfig.namespace, methodConfig.name).map(_.rootEntityType).update(methodConfig.rootEntityType)
      }
    } map { _ => methodConfig }

    def get(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String): ReadAction[Option[MethodConfiguration]] = {
      loadMethodConfigurationByName(workspaceContext.workspaceId, methodConfigurationNamespace, methodConfigurationName)
    }

    def rename(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String): ReadWriteAction[Int] = {
      findByName(workspaceContext.workspaceId, methodConfigurationNamespace, methodConfigurationName).map(_.name).update(newName)
    }

    def delete(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String): ReadWriteAction[Boolean] = {
      uniqueResult[MethodConfigurationRecord](findByName(workspaceContext.workspaceId, methodConfigurationNamespace, methodConfigurationName)) flatMap {
        case None => DBIO.successful(false)
        case Some(methodConfigRec) => {
          DeleteMethodConfigurationQuery.hideAction(methodConfigRec.id)
        } map { count =>
          count > 0
        }
      }
    }


    object DeleteMethodConfigurationQuery extends RawSqlQuery {
      val driver: JdbcDriver = MethodConfigurationComponent.this.driver

      def deleteAction(workspaceId: UUID) = {
        val tables: Seq[String] = Seq("METHOD_CONFIG_INPUT", "METHOD_CONFIG_OUTPUT", "METHOD_CONFIG_PREREQ")

        DBIO.sequence(tables map { table =>
          sqlu"""delete t from #$table as t
                inner join METHOD_CONFIG as mc on mc.id=t.method_config_id
                where mc.workspace_id=$workspaceId"""
        })
      }

      def hideAction(methodId: Long) = {
        val now = DateTime.now.toString("yyyy-MM-dd_HH:mm:ss")
        sqlu"""UPDATE METHOD_CONFIG mc
            SET mc.DELETED = 1, mc.NAMESPACE = mcNAMESPACE + "-DELETED-" + $now
            WHERE mc.ID = $methodId AND mc.DELETE = 0;"""

      }
    }


    def list(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[MethodConfigurationShort]] = {
      findByWorkspace(workspaceContext.workspaceId).result.map(recs => recs.map(rec => unmarshalMethodConfigToShort(rec)))
    }

    def deleteMethodConfigurationAction(id: Long): ReadWriteAction[Int] = {
      findInputsByConfigId(id).delete andThen
        findOutputsByConfigId(id).delete andThen
        findPrereqsByConfigId(id).delete andThen
        findById(id).delete
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

    private def findInputsByConfigId(configId: Long): MethodConfigurationInputQueryType = {
      methodConfigurationInputQuery.filter(rec => rec.methodConfigId === configId)
    }

    private def findOutputsByConfigId(configId: Long): MethodConfigurationOutputQueryType = {
      methodConfigurationOutputQuery.filter(rec => rec.methodConfigId === configId)
    }

    private def findPrereqsByConfigId(configId: Long): MethodConfigurationPrereqQueryType = {
      methodConfigurationPrereqQuery.filter(rec => rec.methodConfigId === configId)
    }

    def findByName(workspaceId: UUID, methodNamespace: String, methodName: String): MethodConfigurationQueryType = {
      filter(rec => rec.namespace === methodNamespace && rec.name === methodName && rec.workspaceId === workspaceId)
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
      uniqueResult[MethodConfigurationRecord](findByName(workspaceId, methodConfigNamespace, methodConfigName)) flatMap {
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
      MethodConfigurationRecord(0, methodConfig.namespace, methodConfig.name, workspaceId, methodConfig.rootEntityType, methodConfig.methodRepoMethod.methodNamespace, methodConfig.methodRepoMethod.methodName, methodConfig.methodRepoMethod.methodVersion, false)
    }

    def unmarshalMethodConfig(methodConfigRec: MethodConfigurationRecord, inputs: Map[String, AttributeString], outputs: Map[String, AttributeString], prereqs: Map[String, AttributeString]): MethodConfiguration = {
      MethodConfiguration(methodConfigRec.namespace, methodConfigRec.name, methodConfigRec.rootEntityType, prereqs, inputs, outputs, MethodRepoMethod(methodConfigRec.methodNamespace, methodConfigRec.methodName, methodConfigRec.methodVersion))
    }

    private def unmarshalMethodConfigToShort(methodConfigRec: MethodConfigurationRecord): MethodConfigurationShort = {
      MethodConfigurationShort(methodConfigRec.name, methodConfigRec.rootEntityType, MethodRepoMethod(methodConfigRec.methodNamespace, methodConfigRec.methodName, methodConfigRec.methodVersion), methodConfigRec.namespace)
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
