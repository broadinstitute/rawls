package org.broadinstitute.dsde.rawls.datamigration

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.DomainObject
import org.joda.time.DateTime

import scala.collection.JavaConversions._

/**
 * Infrastructure for migrating data in the rawls database. When rawls starts, before starting any services
 * it Boot calls DataMigration.migrateData. This will call migrate for each member of dataMigrations in order.
 * Once run, each DataMigration is recorded. If it is detected that a DataMigration has already run, it is skipped.
 *
 * To add a new DataMigration, implement the DataMigration trait and add it to the appropriate place (usually at
 * the end) of the dataMigrations seq.
 */
object DataMigration extends LazyLogging {

  def migrateData(config: Config, containerDAO: GraphContainerDAO, dataSource: DataSource): Unit = {
    val duplicateMigrationIds = dataMigrations.groupBy(_.migrationId).collect { case (id, migrations) if migrations.size > 1 => (id, migrations) }
    if (!duplicateMigrationIds.isEmpty) {
      throw new RawlsException(s"duplicate migration keys exist for $duplicateMigrationIds")
    }
    val migrationEntryDAO = new MigrationEntryDAO

    val migrationEntries = dataSource.inTransaction() { migrationEntryDAO.list }

    logger.info(s"DataMigration: detected prior migrations: ${migrationEntries.toSeq.sortBy(_.runDate.getMillis)}")

    dataMigrations.foreach { dataMigration =>
      if (migrationEntries.map(_.migrationId).contains(dataMigration.migrationId)) {
        logger.info(s"DataMigration: skipping ${dataMigration.migrationId}")
      } else {
        logger.info(s"DataMigration: running ${dataMigration.migrationId}")
        dataSource.inTransaction() { txn =>
          dataMigration.migrate(config, containerDAO, txn)
          migrationEntryDAO.save(MigrationEntry(
            dataMigration.migrationId,
            DateTime.now(),
            config.getString("version.build.number"),
            config.getString("version.git.hash"),
            config.getString("version.version")),
          txn)
        }
        logger.info(s"DataMigration: completed ${dataMigration.migrationId}")
      }
    }
  }

  val dataMigrations: Seq[DataMigration] = Seq(
    AddWorkspaceGroupEmailMigration
  )
}

trait DataMigration {
  val migrationId: String
  def migrate(config: Config, containerDAO: GraphContainerDAO, txn: RawlsTransaction): Unit
}

case class MigrationEntry(migrationId: String, runDate: DateTime, buildNumber: String, gitHash: String, rawlsVersion: String) extends DomainObject {
  override def idFields: Seq[String] = Seq("migrationId")
}

class MigrationEntryDAO extends GraphDAO {
  import CachedTypes.migrationEntryType

  def list(txn: RawlsTransaction): Seq[MigrationEntry] = txn withGraph { graph =>
    graph.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.MigrationEntry).map(loadObject[MigrationEntry]).toSeq
  }

  def save(migrationEntry: MigrationEntry, txn: RawlsTransaction): Unit = txn withGraph { graph =>
    saveObject[MigrationEntry](migrationEntry, addVertex(graph, VertexSchema.MigrationEntry), None, graph)
  }
}