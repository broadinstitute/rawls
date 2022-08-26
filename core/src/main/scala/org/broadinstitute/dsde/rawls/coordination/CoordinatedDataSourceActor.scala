package org.broadinstitute.dsde.rawls.coordination

import akka.actor.{Actor, Props, Status}
import org.broadinstitute.dsde.rawls.coordination.CoordinatedDataSourceActor.StartDeadlineException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SlickDataSource}
import slick.jdbc.TransactionIsolation

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
 * Runs each data source transaction one at a time.
 */
class CoordinatedDataSourceActor() extends Actor {
  def run[A](startDeadline: Deadline, waitTimeout: FiniteDuration)(future: => Future[A]): Unit = {
    val result = if (startDeadline.hasTimeLeft()) {
      Try(Await.result(future, waitTimeout)) match {
        case Success(value)     => value
        case Failure(throwable) => Status.Failure(throwable)
      }
    } else {
      Status.Failure(new StartDeadlineException())
    }
    sender() ! result
  }

  override def receive: Receive = {
    case CoordinatedDataSourceActor.Run(
          slickDataSource,
          dataAccessFunction,
          isolationLevel,
          startDeadline,
          waitTimeout
        ) =>
      run(startDeadline, waitTimeout) {
        // NOTE: We could feed the JDBC calls yet-another timeout-per-statement, but warning: the
        // java.sql.Statement.setQueryTimeout() implementation is different for each JDBC driver!
        slickDataSource.inTransaction(dataAccessFunction, isolationLevel)
      }
    case CoordinatedDataSourceActor.RunWithTempTables(
          slickDataSource,
          dataAccessFunction,
          isolationLevel,
          startDeadline,
          waitTimeout
        ) =>
      run(startDeadline, waitTimeout) {
        // NOTE: We could feed the JDBC calls yet-another timeout-per-statement, but warning: the
        // java.sql.Statement.setQueryTimeout() implementation is different for each JDBC driver!
        slickDataSource.inTransactionWithAttrTempTable(
          Set(AttributeTempTableType.Entity, AttributeTempTableType.Workspace)
        )(dataAccessFunction, isolationLevel)
      }
  }
}

object CoordinatedDataSourceActor {

  final case class Run[A: ClassTag](slickDataSource: SlickDataSource,
                                    dataAccessFunction: DataAccess => ReadWriteAction[A],
                                    isolationLevel: TransactionIsolation,
                                    startDeadline: Deadline,
                                    waitTimeout: FiniteDuration
  )
  final case class RunWithTempTables[A: ClassTag](slickDataSource: SlickDataSource,
                                                  dataAccessFunction: DataAccess => ReadWriteAction[A],
                                                  isolationLevel: TransactionIsolation,
                                                  startDeadline: Deadline,
                                                  waitTimeout: FiniteDuration
  )

  class StartDeadlineException() extends Exception("too busy, unable to start running in time")

  def props(): Props = Props(new CoordinatedDataSourceActor())
}
