package org.broadinstitute.dsde.rawls.dataaccess

//import cats.effect.{Blocker, ContextShift, IO, Resource, Sync, Timer}
//import cats.effect.{Blocker, ContextShift, IO, Resource, Sync, Timer}
import cats.effect._
// import cats.implicits._
import com.google.cloud.bigquery.{BigQuery, JobId, QueryJobConfiguration, TableResult}
import io.chrisdavenport.log4cats.StructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService

import scala.language.higherKinds

object MockBigQueryServiceFactory {

  def ioFactory: MockBigQueryServiceFactory[IO] = {
    implicit val logger: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
    implicit val contextShift: ContextShift[IO] = cats.effect.IO.contextShift(TestExecutionContext.testExecutionContext)
    implicit val timer: Timer[IO] = cats.effect.IO.timer(TestExecutionContext.testExecutionContext)
    val blocker = Blocker.liftExecutionContext(TestExecutionContext.testExecutionContext)

    new MockBigQueryServiceFactory[IO](blocker)
  }

}


class MockBigQueryServiceFactory[F[_]: Sync: ContextShift: Timer: StructuredLogger](blocker: Blocker)
  extends GoogleBigQueryServiceFactory[F](blocker: Blocker) {

  override def getServiceForPet(petKey: String): Resource[F, GoogleBigQueryService[F]] = {
    Resource.pure(new MockGoogleBigQueryService[F])
  }

}

class MockGoogleBigQueryService[F[_]] extends GoogleBigQueryService[F] {
  override def query(queryJobConfiguration: QueryJobConfiguration, options: BigQuery.JobOption*): F[TableResult] = ???

  override def query(queryJobConfiguration: QueryJobConfiguration, jobId: JobId, options: BigQuery.JobOption*): F[TableResult] = ???
}