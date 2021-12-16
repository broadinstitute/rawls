package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import slick.jdbc.TransactionIsolation

import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}


class ParallelShardingMigration(slickDataSource: SlickDataSource) extends LazyLogging {

  import slickDataSource.dataAccess.driver.api._

  // thread pool with 24 threads to match the 24 CPUs in the prod db
  val threadPool = Executors.newFixedThreadPool(24)
  implicit val fixedThreadPool: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(threadPool)

  def migrate() = {
    // migrate all shards - except for "_archived". This will include the 4 shards we already migrated, but
    // those will be quick no-ops.
    val shardsToMigrate = (slickDataSource.dataAccess.allShards - "archived").toSeq.sorted

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
    logger.warn(s"migration for shard $shardId starting ...")

    // TODO: swap from the 'select count' sql to the 'call stored proc' sql ONLY when ready to actually migrate
    // TODO: verify that the call to populateShardAndMarkAsSharded($shardId); works as intended here in Scala (we
    //        know it works in the db)
    val sql = sql"""select count(1) from ENTITY_ATTRIBUTE_#$shardId;""".as[String]
    // val sql = sql"""call populateShardAndMarkAsSharded($shardId);""".as[String]

    // ReadCommitted here? Since we intend this to run while Rawls as a whole is down, and no other db activity,
    // it really shouldn't matter
    val transaction = sql
      .transactionally
      .withTransactionIsolation(TransactionIsolation.ReadCommitted)

    val transactionRun = slickDataSource.database.run(transaction)

    // block for the result, helps with concurrency
    val procResult = Await.result(transactionRun, Duration.Inf).head

    val elapsed = System.currentTimeMillis() - tick

    logger.warn(s"migration for shard $shardId done in $elapsed ms: $procResult")
    s"$shardId: $procResult"
  }

}

