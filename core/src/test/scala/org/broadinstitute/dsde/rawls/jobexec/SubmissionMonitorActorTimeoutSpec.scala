package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential.Builder
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess.AttributeTempTableType.Workspace
import org.broadinstitute.dsde.rawls.dataaccess.{
  MockGoogleServicesDAO,
  MockShardedExecutionServiceCluster,
  SlickDataSource
}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeName,
  AttributeString,
  Entity,
  RawlsTracingContext,
  WorkflowStatuses
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.sql.BatchUpdateException
import slick.jdbc.TransactionIsolation

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class SubmissionMonitorActorTimeoutSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with MockitoTestUtils
    with TestDriverComponent {

  import driver.api._

  def this() = this(ActorSystem("WorkflowMonitorSpec"))

  val mockGoogleServicesDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
  val mockNotificationDAO: NotificationDAO = mock[NotificationDAO]
  val mockSamDAO = new MockSamDAO(slickDataSource)

  // copied from LocalEntityProviderTimeoutSpec
  /** Locks all rows of the ENTITY table for ${lockSeconds} seconds. Use this to simulate database contention. */
  private def lockAllEntities(dataSource: SlickDataSource, lockSeconds: Int): Future[Unit] = {
    val locker = for {
      _ <- sql"""select * from ENTITY for update;""".as[Unit]
      _ <- sql"""select sleep($lockSeconds);""".as[Unit]
    } yield ()
    dataSource.database.run(locker.transactionally.withTransactionIsolation(TransactionIsolation.Serializable))

    // .withTransactionIsolation(TransactionIsolation.Serializable)
  }

  behavior of "SubmissionMonitorActor query timeouts"

  it should "should enforce on saveEntities()" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    withWorkspaceContext(testData.workspace) { workspaceContext =>
      // create a SubmissionMonitorActor with a long poll interval
      val config = SubmissionMonitorConfig(1 day,
                                           30 days,
                                           trackDetailedSubmissionMetrics = false,
                                           10,
                                           enableEmailNotifications = false
      )
      val submissionMonitorActorRef = TestActorRef[SubmissionMonitorActor](
        SubmissionMonitorActor.props(
          testData.wsName,
          UUID.fromString(testData.submission1.submissionId),
          new UncoordinatedDataSourceAccess(dataSource),
          mockSamDAO,
          mockGoogleServicesDAO,
          mockNotificationDAO,
          MockShardedExecutionServiceCluster
            .fromDAO(new SubmissionTestExecutionServiceDAO(WorkflowStatuses.Submitted.toString), dataSource),
          config,
          Duration.create(1, SECONDS),
          "test"
        )
      )

      // generate a workflow entity update
      val entityRef = Entity(testData.sample1.entityType, testData.sample1.name, Map())
      val entityUpdate =
        WorkflowEntityUpdate(
          entityRef,
          Map(AttributeName.withDefaultNS("SubmissionMonitorActorTimeoutSpec") -> AttributeString("updated value"))
        )
      val updatedEntitiesAndWorkspace = Seq(Left(Option(entityUpdate), Option(workspaceContext)))

      // lock the entity table
      val lockTime = 3 // seconds
      val lockFuture: Future[Unit] = lockAllEntities(dataSource, lockTime)

      val actual = intercept[BatchUpdateException] {
        runAndWait(
          submissionMonitorActorRef.underlyingActor.saveEntities(dataSource.dataAccess,
                                                                 workspaceContext,
                                                                 updatedEntitiesAndWorkspace,
                                                                 RawlsTracingContext(None)
          ),
          Duration.create(lockTime, SECONDS)
        )
      }

      actual.getMessage shouldBe "Statement cancelled due to timeout or client request"

      // finally, ensure we wait for the previous lock to commit - and therefore unlock - before
      // ending the test, so we don't leave locks on the db for any other tests
      val _ = Await.result(lockFuture, Duration.create(lockTime, SECONDS))
    }
  }
}
