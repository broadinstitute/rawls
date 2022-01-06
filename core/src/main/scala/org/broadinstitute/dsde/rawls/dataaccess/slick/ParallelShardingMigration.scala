package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import slick.jdbc.TransactionIsolation

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}


class ParallelShardingMigration(slickDataSource: SlickDataSource) extends LazyLogging {

  import slickDataSource.dataAccess.driver.api._

  // prod db has 24 CPUs, that's the ideal for number of threads
  // NB increase slick.db.connectionTimeout setting to avoid "Connection is not available" errors
  val nThreads = 24
  val threadPool = Executors.newFixedThreadPool(nThreads)
  implicit val fixedThreadPool: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(threadPool)

  // note: var!
  var migrationsRunning: AtomicInteger = new AtomicInteger(0)
  var shardsStarted: AtomicInteger = new AtomicInteger(0)
  var shardsFinished: AtomicInteger = new AtomicInteger(0)

  val shardsToMigrate = (slickDataSource.dataAccess.allShards - "archived").toSeq.sorted
  val nShards = shardsToMigrate.size

  def migrate() = {
    // migrate all shards - except for "_archived". This will include the 4 shards we already migrated, but
    // those will be quick no-ops.
    logger.warn(s"migration of $nShards shards starting.")

    val migrationResults = Future.traverse(shardsToMigrate) { shardId => migrateShard(shardId) }

    val res = Await.result(migrationResults, Duration.Inf)
    logger.warn(s"******* res: $res")

    // now that all shards have migrated - and ONLY if they all succeeded - drop and recreate the _archived table
    // That should be a manual step to ensure a human is looking at the results and we don't
    // have any risk of losing data
  }

  // this shenanigans around futures and blocking makes it easy to use the Future.traverse above
  private def migrateShard(shardId: String): Future[String] = {
    Future(migrateShardImpl(shardId))
  }

  private def migrateShardImpl(shardId: String): String = {
    val tick = System.currentTimeMillis()

    shardsStarted.incrementAndGet()
    migrationsRunning.incrementAndGet()

    logger.info(s"[$shardId] migration for shard $shardId starting " +
      s"($shardsStarted/$nShards started, $shardsFinished/$nShards finished, $migrationsRunning/$nThreads threads in use). ")

    // count rows in archived table for this shard
    val rowsToMigrateSql =
      sql"""select count(1) from ENTITY_ATTRIBUTE_archived ea, ENTITY e, WORKSPACE w
              WHERE e.workspace_id = w.id
              AND ea.owner_id = e.id
              AND shardIdentifier(hex(w.id)) = $shardId;""".as[Int]
    val rowsToMigrate = Await.result(runSql(rowsToMigrateSql), Duration.Inf).head

    val shardCountSql =
      sql"""select count(1) ENTITY_ATTRIBUTE_#$shardId;""".as[Int]

    // count rows in shard, before migration
    val shardCountBefore = Await.result(runSql(shardCountSql), Duration.Inf).head

    logger.info(s"[$shardId] shard $shardId expects to migrate ${fmt(rowsToMigrate)} rows")

    // TODO: swap from the 'select count' sql to the 'call stored proc' sql ONLY when ready to actually migrate
    // TODO: verify that the call to populateShardAndMarkAsSharded($shardId); works as intended here in Scala (we
    //        know it works in the db)
    val sql = sql"""select count(1) from ENTITY_ATTRIBUTE_#$shardId;""".as[String]
    // val sql = sql"""call populateShardAndMarkAsSharded($shardId);""".as[String]

    // block for the result, helps with concurrency
    val procResult = Await.result(runSql(sql), Duration.Inf).head

    // count rows in shard, after migration
    val shardCountAfter = Await.result(runSql(shardCountSql), Duration.Inf).head

    // TODO: compare row count after migration to expected row count
    if (shardCountAfter != shardCountBefore + rowsToMigrate) {
      logger.error(s"[$shardId] DANGER: shard $shardId finished with " +
        s"${fmt(shardCountAfter)} rows, " +
        s"expected ${fmt(shardCountBefore + rowsToMigrate)} (${fmt(shardCountBefore)} + ${fmt(rowsToMigrate)})")
    }

    val elapsed = System.currentTimeMillis() - tick

    shardsFinished.incrementAndGet()
    migrationsRunning.decrementAndGet()

    logger.info(s"[$shardId] migration for shard $shardId done     " +
      s"($shardsStarted/$nShards started, $shardsFinished/$nShards finished, $migrationsRunning/$nThreads threads in use) " +
      s"in ${fmt(elapsed)} ms: $procResult")
    s"$shardId: $procResult"

  }

  private def runSql[T](sql: ReadWriteAction[T]) = {
    // ReadCommitted here? Since we intend this to run while Rawls as a whole is down, and no other db activity,
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
}

