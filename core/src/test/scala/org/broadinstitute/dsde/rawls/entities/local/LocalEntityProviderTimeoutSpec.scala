package org.broadinstitute.dsde.rawls.entities.local

import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeName, AttributeString, Entity}
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.dsl.ResultOfATypeInvocation
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike
import slick.jdbc.TransactionIsolation

import scala.concurrent.Future

class LocalEntityProviderTimeoutSpec extends AnyWordSpecLike with Matchers with ScalaFutures with TestDriverComponent {

  import driver.api._

  /** Locks all rows of the ENTITY table for ${lockSeconds} seconds. Use this to simulate database contention. */
  private def lockAllEntities(dataSource: SlickDataSource, lockSeconds: Int): Future[Unit] = {
    val locker = for {
      _ <- sql"""select * from ENTITY for update;""".as[Unit]
      _ <- sql"""select sleep($lockSeconds);""".as[Unit]
    } yield ()
    dataSource.database.run(locker.transactionally.withTransactionIsolation(TransactionIsolation.Serializable))
  }

  /** Implementation for all of the tests in this class. They share a lot of setup and assertion code. */
  private def lockedEntitiesTest(dataSource: SlickDataSource, expectedClass: ResultOfATypeInvocation[_])(
    futureToTest: LocalEntityProvider => Future[_]
  ): Unit =
    withWorkspaceContext(localEntityProviderTestData.workspace) { workspaceContext =>
      // create a single entity
      val testEntity = Entity("deleteTimeoutTest", "unitTestType", Map.empty)
      runAndWait(entityQuery.save(localEntityProviderTestData.workspace, testEntity))

      // create entity provider, configured with a 1-second timeout
      val localEntityProvider = new LocalEntityProvider(
        EntityRequestArguments(workspaceContext, testContext),
        slickDataSource,
        false, // <-- statistics cache disabled, but probably irrelevant for this test
        java.time.Duration.ofSeconds(1), // <----- one-second timeout
        "testMetricsBaseName"
      )

      val lockTime = 3 // seconds

      // ensure the wait-time for the futureValue is longer than the timeout set on the query
      val timeout: Timeout = Timeout(scaled(Span(lockTime, Seconds)))
      val interval: Interval = Interval(scaled(Span(250, Milliseconds)))

      // lock the entity table. Note this is a Future and we don't wait on it here.
      val lockFuture: Future[Unit] = lockAllEntities(dataSource, lockTime)

      // now attempt to execute the "futureToTest" code under test, using the provider configured
      // with a timeout of 1 second. Because the table is locked, the underlying query should wait
      // trying to get the lock, then time out after 1 second.
      val actual: Throwable = futureToTest(localEntityProvider).failed
        .futureValue(timeout, interval)

      actual shouldBe expectedClass

      // finally, ensure we wait for the previous lock to commit - and therefore unlock - before
      // ending the test, so we don't leave locks on the db for any other tests
      val _ = lockFuture.futureValue(timeout, interval)
    }

  "LocalEntityProvider query timeouts" should {

    "enforce on deleteEntities" in withLocalEntityProviderTestDatabase { dataSource =>
      lockedEntitiesTest(dataSource, a[MySQLTimeoutException]) { localEntityProvider =>
        localEntityProvider.deleteEntities(
          Seq(AttributeEntityReference(entityType = "unitTestType", entityName = "deleteTimeoutTest"))
        )
      }
    }

    "enforce on deleteEntitiesOfType" in withLocalEntityProviderTestDatabase { dataSource =>
      lockedEntitiesTest(dataSource, a[MySQLTimeoutException]) { localEntityProvider =>
        localEntityProvider.deleteEntitiesOfType("unitTestType")
      }
    }

    "enforce on batchUpsert" in withLocalEntityProviderTestDatabase { dataSource =>
      lockedEntitiesTest(dataSource, a[RawlsExceptionWithErrorReport]) { localEntityProvider =>
        val attrUpdate = AddUpdateAttribute(AttributeName.withDefaultNS("newAttr"), AttributeString("whatever"))
        val entityUpdate =
          EntityUpdateDefinition(name = "deleteTimeoutTest", entityType = "unitTestType", Seq(attrUpdate))
        localEntityProvider.batchUpsertEntities(Seq(entityUpdate))
      }
    }

    "enforce on batchUpdate" in withLocalEntityProviderTestDatabase { dataSource =>
      lockedEntitiesTest(dataSource, a[RawlsExceptionWithErrorReport]) { localEntityProvider =>
        val attrUpdate = AddUpdateAttribute(AttributeName.withDefaultNS("newAttr"), AttributeString("whatever"))
        val entityUpdate =
          EntityUpdateDefinition(name = "deleteTimeoutTest", entityType = "unitTestType", Seq(attrUpdate))
        localEntityProvider.batchUpdateEntities(Seq(entityUpdate))
      }
    }
//    "enforce on batchUpdateEntitiesImpl" ignore fail()

  }

}
