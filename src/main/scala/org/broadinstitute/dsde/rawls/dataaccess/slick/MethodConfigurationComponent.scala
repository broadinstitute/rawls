package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

case class MethodConfigurationRecord(id: Long,
                                     namespace: String,
                                     name: String,
                                     workspaceId: UUID,
                                     rootEntityType: String,
                                     methodNamespace: String,
                                     methodName: String,
                                     methodVersion: Int)

case class MethodConfigurationInputRecord(methodConfigId: Long, id: Long, key: String, value: String)

case class MethodConfigurationOutputRecord(methodConfigId: Long, id: Long, key: String, value: String)

case class MethodConfigurationPrereqRecord(methodConfigId: Long, id: Long, key: String, value: String)

trait MethodConfigurationComponent {
  this: DriverComponent with WorkspaceComponent =>

  import driver.api._

  class MethodConfigurationTable(tag: Tag) extends Table[MethodConfigurationRecord](tag, "METHOD_CONFIG") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def namespace = column[String]("NAMESPACE")
    def name = column[String]("NAME")
    def workspaceId = column[UUID]("WORKSPACE_ID")
    def rootEntityType = column[String]("ROOT_ENTITY_TYPE")
    def methodNamespace = column[String]("METHOD_NAMESPACE")
    def methodName = column[String]("METHOD_NAME")
    def methodVersion = column[Int]("METHOD_VERSION")

    def * = (id, namespace, name, workspaceId, rootEntityType, methodNamespace, methodName, methodVersion) <> (MethodConfigurationRecord.tupled, MethodConfigurationRecord.unapply)

    def workspace = foreignKey("FK_MC_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def namespaceNameIdx = index("IDX_CONFIG", (namespace, name), unique = true)
  }

  class MethodConfigurationInputTable(tag: Tag) extends Table[MethodConfigurationInputRecord](tag, "METHOD_CONFIG_INPUT") {
    def methodConfigId = column[Long]("METHOD_CONFIG_ID")
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def key = column[String]("KEY")
    def value = column[String]("VALUE")

    def * = (methodConfigId, id, key, value) <> (MethodConfigurationInputRecord.tupled, MethodConfigurationInputRecord.unapply)

    def methodConfig = foreignKey("FK_MC_INPUT", methodConfigId, methodConfigurationQuery)(_.id)
    def configKeyIdx = index("IDX_MC_INPUT", (methodConfigId, key), unique = true)
  }

  class MethodConfigurationOutputTable(tag: Tag) extends Table[MethodConfigurationOutputRecord](tag, "METHOD_CONFIG_OUTPUT") {
    def methodConfigId = column[Long]("METHOD_CONFIG_ID")
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def key = column[String]("KEY")
    def value = column[String]("VALUE")

    def * = (methodConfigId, id, key, value) <> (MethodConfigurationOutputRecord.tupled, MethodConfigurationOutputRecord.unapply)

    def methodConfig = foreignKey("FK_MC_OUTPUT", methodConfigId, methodConfigurationQuery)(_.id)
    def configKeyIdx = index("IDX_MC_OUTPUT", (methodConfigId, key), unique = true)
  }

  class MethodConfigurationPrereqTable(tag: Tag) extends Table[MethodConfigurationPrereqRecord](tag, "METHOD_CONFIG_PREREQ") {
    def methodConfigId = column[Long]("METHOD_CONFIG_ID")
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def key = column[String]("KEY")
    def value = column[String]("VALUE")

    def * = (methodConfigId, id, key, value) <> (MethodConfigurationPrereqRecord.tupled, MethodConfigurationPrereqRecord.unapply)

    def methodConfig = foreignKey("FK_MC_PREREQ", methodConfigId, methodConfigurationQuery)(_.id)
    def configKeyIdx = index("IDX_MC_PREREQ", (methodConfigId, key), unique = true)
  }

  protected val methodConfigurationQuery = TableQuery[MethodConfigurationTable]
  protected val methodConfigurationInputQuery = TableQuery[MethodConfigurationInputTable]
  protected val methodConfigurationOutputQuery = TableQuery[MethodConfigurationOutputTable]
  protected val methodConfigurationPrereqQuery = TableQuery[MethodConfigurationPrereqTable]
}
