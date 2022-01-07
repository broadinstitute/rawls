package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import slick.jdbc.TransactionIsolation

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.collection.JavaConverters._

class ParallelShardingMigration(slickDataSource: SlickDataSource) extends LazyLogging {

  import slickDataSource.dataAccess.driver.api._

  // prod db has 24 CPUs, that's the ideal for number of threads
  // NB increase slick.db.connectionTimeout setting to avoid "Connection is not available" errors
  val nThreads = 12
  val threadPool = Executors.newFixedThreadPool(nThreads)
  implicit val fixedThreadPool: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(threadPool)

  val migrationsRunning: AtomicInteger = new AtomicInteger(0)
  val shardsStarted: AtomicInteger = new AtomicInteger(0)
  val shardsFinished: AtomicInteger = new AtomicInteger(0)
  val shardsWithWarnings: ConcurrentHashMap[String, Int] = new ConcurrentHashMap[String, Int](0)

  // migrate all shards - except for "_archived". This will include the 4 shards we already migrated, but
  // those will be quick no-ops because they'll copy zero rows.
  val shardsToMigrate = (slickDataSource.dataAccess.allShards - "archived").toSeq.sorted
  val nShards = shardsToMigrate.size

  def migrate() = {

    val tsMigration = System.currentTimeMillis()

    // orphaned-row deletion:
    // when we migrated the initial 4 shards, we did not delete the migrated rows from
    // ENTITY_ATTRIBUTE_archived. This saved time (since the deletion is slow), but
    // it causes problems. When deleting a workspace or deleting individual entities,
    // these orphaned rows throw foreign-key errors because they still have FKs to ENTITY.
    //
    // we do not need to concern ourselves with these orphans if we're doing a full migration
    // that drops and recreates the _archived table upon completion. This code is here - commented
    // out - in case we need it at any point.
    /*
    logger.warn(s"START deletion of orphan rows.")
    val tsDeleteStart = System.currentTimeMillis()
    val deleted = deleteOrphanRows()
    logger.warn(s"deleted $deleted orphan rows in ${fmt(System.currentTimeMillis()-tsDeleteStart)}ms.")
     */

    logger.info(s"START migration of $nShards shards.")

    logger.info(s"calculating expected row counts to migrate for each shard (this will take some time) ... ")
    val expectedCounts = Await.result(Future.traverse(shardsToMigrate) { shardId =>
      expectedMigrationRowCount(shardId).map(count => (shardId -> count))
    }.map(_.toMap), Duration.Inf)

    // output rows-to-migrate, sorted by shardid
    logger.info(s"        SHARD : ROWS TO MIGRATE")
    expectedCounts.toList.sortBy(_._1).foreach {
      case (shardId, rowsToMigrate) =>
        logger.info(s"        $shardId : ${fmt(rowsToMigrate)}")
    }

    // now, order from least-to-most rows to migrate. In other words,
    // we migrate the smallest shards first to get them over with.
    // in the event that anything goes wrong mid-migration, we'll have
    // gotten at least some of the shards migrated
    val orderedShards = expectedCounts.toList.sortBy(_._2)
    val migrationResults = Future.traverse(orderedShards) {
      case (shardId, rowsToMigrate) => migrateShard(shardId, rowsToMigrate)
    }

    Await.result(migrationResults, Duration.Inf)

    if (shardsWithWarnings.isEmpty) {
      logger.info(s"all $nShards shards migrated as expected.")
    } else {
      logger.error("===============================================")
      logger.error(s"ALERT! ALERT! ${shardsWithWarnings.size()} shards did not migrate as expected. Details follow:")
      val warns = shardsWithWarnings.asScala
      val sortedKeys = warns.keys.toList.sorted
      sortedKeys.foreach { shardId =>
        logger.error(s"        shard $shardId was off by ${fmt(warns(shardId))} rows")
      }
      logger.error("===============================================")
    }

    logger.info(s"overall duration for migration: ${timefmt(System.currentTimeMillis() - tsMigration)}")

    // now that all shards have migrated - and ONLY if they all succeeded - drop and recreate the _archived table
    // That should be a manual step to ensure a human is looking at the results and we don't
    // have any risk of losing data
    logger.warn(s"IF AND ONLY IF you are satisfied with the migration results, you must manually delete " +
      s"and re-create the ENTITY_ATTRIBUTE_archived table. This is necessary 1) to delete the now-orphaned rows, and " +
      s"2) so future liquibase migrations still find the expected table")

    val exampleDropCreateSQL =
      """
        |
        |        DROP TABLE ENTITY_ATTRIBUTE_archived;
        |
        |        CREATE TABLE `ENTITY_ATTRIBUTE_archived` (
        |          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
        |          `name` varchar(200) NOT NULL,
        |          `value_string` text,
        |          `value_number` double DEFAULT NULL,
        |          `value_boolean` bit(1) DEFAULT NULL,
        |          `value_entity_ref` bigint(20) unsigned DEFAULT NULL,
        |          `list_index` int(11) DEFAULT NULL,
        |          `owner_id` bigint(20) unsigned NOT NULL,
        |          `list_length` int(11) DEFAULT NULL,
        |          `namespace` varchar(32) NOT NULL DEFAULT 'default',
        |          `VALUE_JSON` longtext,
        |          `deleted` bit(1) DEFAULT b'0',
        |          `deleted_date` timestamp(6) NULL DEFAULT NULL,
        |          PRIMARY KEY (`id`),
        |          KEY `FK_ENT_ATTRIBUTE_ENTITY_REF` (`value_entity_ref`),
        |          KEY `UNQ_ENTITY_ATTRIBUTE` (`owner_id`,`namespace`,`name`,`list_index`),
        |          CONSTRAINT `FK_ATTRIBUTE_PARENT_ENTITY` FOREIGN KEY (`owner_id`) REFERENCES `ENTITY` (`id`),
        |          CONSTRAINT `FK_ENT_ATTRIBUTE_ENTITY_REF` FOREIGN KEY (`value_entity_ref`) REFERENCES `ENTITY` (`id`)
        |        ) ENGINE=InnoDB AUTO_INCREMENT=280224374 DEFAULT CHARSET=utf8;
        |""".stripMargin

    logger.warn(s"The SQL to drop and re-create the ENTITY_ATTRIBUTE_archived table is: $exampleDropCreateSQL")

    logger.info("Rawls startup will now continue.")

  }

  private def deleteOrphanRows() = {
    val deleteSql =
      sqlu"""DELETE ea from ENTITY_ATTRIBUTE_archived ea, ENTITY e, WORKSPACE w
                 WHERE e.workspace_id = w.id
                 AND ea.owner_id = e.id
                 AND w.shard_state = 'Sharded';"""

    Await.result(runSql(deleteSql), Duration.Inf)
  }

  // this shenanigans around futures and blocking makes it easy to use the Future.traverse above
  private def migrateShard(shardId: String, rowsToMigrate: Int): Future[Int] = {
    Future(migrateShardImpl(shardId, rowsToMigrate))
  }

  private def migrateShardImpl(shardId: String, rowsToMigrate: Int): Int = {
    val tick = System.currentTimeMillis()

    shardsStarted.incrementAndGet()
    migrationsRunning.incrementAndGet()

    logger.info(s"[$shardId] migration for shard $shardId starting " +
      s"($shardsStarted/$nShards started, $shardsFinished/$nShards finished, $migrationsRunning/$nThreads threads in use). ")

    val shardCountSql =
      sql"""select count(1) from ENTITY_ATTRIBUTE_#$shardId;""".as[Int]

    // count rows in shard, before migration
    val shardCountBefore = Await.result(runSql(shardCountSql), Duration.Inf).head

    logger.info(s"[$shardId] shard $shardId expects to migrate ${fmt(rowsToMigrate)} rows")

    val sql = sql"""call populateShardAndMarkAsSharded($shardId);""".as[Int]

    // block for the result, helps with concurrency
    val procResult = Await.result(runSql(sql), Duration.Inf).head

    // count rows in shard, after migration
    val shardCountAfter = Await.result(runSql(shardCountSql), Duration.Inf).head

    // compare row count after migration to expected row count
    if (shardCountAfter == shardCountBefore + rowsToMigrate) {
      logger.info(s"[$shardId] SUCCESS: shard $shardId finished with ${fmt(shardCountAfter)} rows, as expected")
    } else {
      shardsWithWarnings.put(shardId, shardCountBefore + rowsToMigrate - shardCountAfter)
      logger.error(s"[$shardId] DANGER: shard $shardId finished with " +
        s"${fmt(shardCountAfter)} rows, " +
        s"expected ${fmt(shardCountBefore + rowsToMigrate)} (${fmt(shardCountBefore)} + ${fmt(rowsToMigrate)})")
    }

    val elapsed = System.currentTimeMillis() - tick

    shardsFinished.incrementAndGet()
    migrationsRunning.decrementAndGet()

    logger.info(s"[$shardId] migration for shard $shardId done     " +
      s"($shardsStarted/$nShards started, $shardsFinished/$nShards finished, $migrationsRunning/$nThreads threads in use) " +
      s"- ${fmt(rowsToMigrate)} rows in ${timefmt(elapsed)}")
    s"$shardId: $procResult"

    procResult

  }

  private def expectedMigrationRowCount(shardId: String): Future[Int] = {
    // count rows in archived table for this shard
    val rowsToMigrateSql =
      sql"""select count(1) from ENTITY_ATTRIBUTE_archived ea, ENTITY e, WORKSPACE w
              WHERE e.workspace_id = w.id
              AND ea.owner_id = e.id
              AND shardIdentifier(hex(w.id)) = $shardId
              AND w.shard_state = 'unsharded';""".as[Int]
    runSql(rowsToMigrateSql.head)
  }

  private def runSql[T](sql: ReadWriteAction[T]) = {
    // Serializable here ensures the before/after row counts are accurate; these really help
    // with verifying success of shard copies.
    // Since we intend this to run while Rawls as a whole is down, and no other db activity,
    // it really shouldn't matter
    val transaction = sql
      .transactionally
      .withTransactionIsolation(TransactionIsolation.ReadCommitted)

    slickDataSource.database.run(transaction)
  }

  // for nice logging output
  val formatter = java.text.NumberFormat.getIntegerInstance
  private def fmt(in: Int) = formatter.format(in)
  private def fmt(in: Long) = formatter.format(in)

  private def timefmt(millis: Long) = {
    val minutes = TimeUnit.MILLISECONDS.toMinutes(millis)
    val seconds = TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(minutes)
    s"$minutes min, $seconds sec"
  }

}

