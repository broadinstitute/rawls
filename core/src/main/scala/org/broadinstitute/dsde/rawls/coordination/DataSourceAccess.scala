package org.broadinstitute.dsde.rawls.coordination

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SlickDataSource}
import slick.jdbc.TransactionIsolation

import scala.concurrent.Future
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.reflect.ClassTag

/**
 * Queries data access, possibly running one future at a time to avoid deadlocks. WA-177
 */
trait DataSourceAccess {
  val slickDataSource: SlickDataSource

  def inTransaction[A: ClassTag](dataAccessFunction: DataAccess => ReadWriteAction[A],
                                 isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): Future[A]

  def inTransactionWithAttrTempTable[A: ClassTag](dataAccessFunction: DataAccess => ReadWriteAction[A],
                                                  isolationLevel: TransactionIsolation =
                                                    TransactionIsolation.RepeatableRead
  ): Future[A]
}

/**
 * Send all data access requests as concurrently as possible, even if deadlocks may occur.
 */
class UncoordinatedDataSourceAccess(override val slickDataSource: SlickDataSource) extends DataSourceAccess {

  override def inTransaction[A: ClassTag](dataAccessFunction: DataAccess => ReadWriteAction[A],
                                          isolationLevel: TransactionIsolation = TransactionIsolation.RepeatableRead
  ): Future[A] =
    slickDataSource.inTransaction(dataAccessFunction, isolationLevel)

  override def inTransactionWithAttrTempTable[A: ClassTag](dataAccessFunction: DataAccess => ReadWriteAction[A],
                                                           isolationLevel: TransactionIsolation =
                                                             TransactionIsolation.RepeatableRead
  ): Future[A] =
    slickDataSource.inTransactionWithAttrTempTable(
      Set(AttributeTempTableType.Entity, AttributeTempTableType.Workspace)
    )(dataAccessFunction, isolationLevel)
}

/**
 * Queue up the request in an underlying actor's mailbox, that will run only one request at a time, primarily to avoid
 * deadlocks. NOTE: Ideally the call to inTransaction should only contain IO requests, and should not spend significant
 * CPU processing time, as other threads/actors will be waiting for the exclusive use of the shared actor.
 */
class CoordinatedDataSourceAccess(override val slickDataSource: SlickDataSource,
                                  dataSourceActor: ActorRef,
                                  starTimeout: FiniteDuration,
                                  waitTimeout: FiniteDuration,
                                  askTimeout: FiniteDuration
) extends DataSourceAccess {
  override def inTransaction[A: ClassTag](dataAccessFunction: DataAccess => ReadWriteAction[A],
                                          isolationLevel: TransactionIsolation
  ): Future[A] = {
    // How long to wait for the actor to get to and finish running the function
    implicit val implicitTimeout: Timeout = Timeout(askTimeout)
    // Throws a DeadlineTimeoutException the function is not started within startTimeout
    // Throws a TimeoutException if the function does not run within the waitTimeout
    // Throws an AskTimeoutException if we do not hear back either way before askTimeout
    dataSourceActor
      .ask(
        CoordinatedDataSourceActor.Run(
          slickDataSource = slickDataSource,
          dataAccessFunction = dataAccessFunction,
          isolationLevel = isolationLevel,
          startDeadline = Deadline.now + starTimeout,
          waitTimeout = waitTimeout
        )
      )
      .mapTo[A]
  }

  override def inTransactionWithAttrTempTable[A: ClassTag](dataAccessFunction: DataAccess => ReadWriteAction[A],
                                                           isolationLevel: TransactionIsolation =
                                                             TransactionIsolation.RepeatableRead
  ): Future[A] = {
    // How long to wait for the actor to get to and finish running the function
    implicit val implicitTimeout: Timeout = Timeout(askTimeout)
    // Throws a DeadlineTimeoutException the function is not started within startTimeout
    // Throws a TimeoutException if the function does not run within the waitTimeout
    // Throws an AskTimeoutException if we do not hear back either way before askTimeout
    dataSourceActor
      .ask(
        CoordinatedDataSourceActor.RunWithTempTables(
          slickDataSource = slickDataSource,
          dataAccessFunction = dataAccessFunction,
          isolationLevel = isolationLevel,
          startDeadline = Deadline.now + starTimeout,
          waitTimeout = waitTimeout
        )
      )
      .mapTo[A]
  }
}
