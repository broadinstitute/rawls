package org.broadinstitute.dsde.rawls.entities.local

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import breeze.linalg._
import breeze.stats._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{
  GoogleBigQueryServiceFactoryImpl,
  MockBigQueryServiceFactory,
  SlickDataSource
}
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.{
  MockDataRepoDAO,
  MockSamDAO,
  MockWorkspaceManagerDAO,
  RemoteServicesMockServer
}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{
  AddUpdateAttribute,
  AttributeUpdateOperation,
  EntityUpdateDefinition
}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeString,
  RawlsRequestContext,
  RawlsUser,
  UserInfo
}
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice.EntityApiService
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.io.Source

/**
  * This spec contains one test and that test is ignored by default; it does not run as part of the main unit test suite.
  *
  * The "test" is a pseudo perf test for batchUpsert-entity and delete-entity (which is actually hide-entity). The goal
  * of this test is to allow you, the developer, to quickly and easily test before/after changes to the
  * batchUpsert-entity and delete-entity code paths. Furthermore, the test always passes unless something throws an
  * exception; it is up to you to look at the console/log output and find the performance timings it outputs.
  *
  * Because this test runs on your laptop, or potentially in GHA, the numbers it produces should not be
  * considered to be objective results, and they do not translate to performance on production. They should only be used
  * to compare before/after your code changes.
  */
class BatchUpsertScalingSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with Matchers
    with TestDriverComponent
    with RawlsTestUtils
    with Eventually
    with MockitoTestUtils
    with RawlsStatsDTestUtils
    with BeforeAndAfterAll
    with LazyLogging {

  // =========== START SERVICES AND MOCKS SETUP ===========
  // copied from EntityServiceSpec
  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }

  // noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  class TestApiService(dataSource: SlickDataSource, val user: RawlsUser)(implicit
    val executionContext: ExecutionContext
  ) extends EntityApiService
      with MockUserInfoDirectivesWithUser {
    private val ctx1 = RawlsRequestContext(UserInfo(user.userEmail, OAuth2BearerToken("foo"), 0, user.userSubjectId))
    lazy val entityService: EntityService = entityServiceConstructor(ctx1)

    def actorRefFactory = system
    val samDAO = new MockSamDAO(dataSource)

    val bigQueryServiceFactory: GoogleBigQueryServiceFactoryImpl = MockBigQueryServiceFactory.ioFactory()

    val testConf = ConfigFactory.load()

    override val batchUpsertMaxBytes = testConf.getLong("entityUpsert.maxContentSizeBytes")

    val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName,
      EntityManager.defaultEntityManager(
        dataSource,
        new MockWorkspaceManagerDAO(),
        new MockDataRepoDAO(mockServer.mockServerBaseUrl),
        samDAO,
        bigQueryServiceFactory,
        DataRepoEntityProviderConfig(100, 10, 0),
        testConf.getBoolean("entityStatisticsCache.enabled"),
        testConf.getDuration("entities.queryTimeout"),
        workbenchMetricBaseName
      ),
      1000
    ) _
  }

  def withTestDataServices[T](testCode: TestApiService => T): T =
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      withServices(dataSource, testData.userOwner)(testCode)
    }

  private def withServices[T](dataSource: SlickDataSource, user: RawlsUser)(testCode: (TestApiService) => T) = {
    val apiService = new TestApiService(dataSource, user)
    testCode(apiService)
  }
  // =========== END SERVICES AND MOCKS SETUP ===========

  behavior of "Batch Upsert scaling"

  // test is currently ignored; manually re-enable it in order to run tests locally
  it should "calculate perf stats for a medium-sized data table, updating one cell and ignoring unchanged cells" ignore withTestDataServices {
    testApiService =>
      // =========== START DATA SETUP ===========
      // read batch upsert file from src/test/resources
      val batchUpsertFile: String =
        Source.fromResource("fixtures/batchUpsert/mediumtext-initial.json").getLines().mkString

      // parse into EntityUpdateDefinition. This is our initial load file, as if the user had an empty workspace
      // and uploaded a TSV to create a new data table.
      val initialUpsert: Seq[EntityUpdateDefinition] = batchUpsertFile.parseJson.convertTo[Seq[EntityUpdateDefinition]]

      // copy the initial upsert, but change one value. This is as if the user downloaded a data table to TSV,
      // modified one cell, and re-uploaded the entire TSV.
      val firstUpdateDef: EntityUpdateDefinition = initialUpsert.head
      val firstAddUpdate: AddUpdateAttribute = firstUpdateDef.operations
        .collectFirst { case aua: AddUpdateAttribute =>
          aua
        }
        .getOrElse(
          fail("test or fixture invalid: expected first operation in first update to be an AddUpdateAttribute")
        )
      val modifiedAddUpdate: AttributeUpdateOperation = firstAddUpdate.copy(addUpdateAttribute =
        AttributeString(s"${firstAddUpdate.addUpdateAttribute.toString}-changed")
      )
      val modifiedUpdateDef = firstUpdateDef.copy(operations = modifiedAddUpdate +: firstUpdateDef.operations.tail)
      val modifiedUpsert: Seq[EntityUpdateDefinition] = modifiedUpdateDef +: initialUpsert.tail

      // find all entity refs in the initial file; this represents all the entities we need to delete to remove the entire table
      val refsToDelete: Seq[AttributeEntityReference] = initialUpsert.map { updateDef =>
        AttributeEntityReference(updateDef.entityType, updateDef.name)
      }
      // =========== END DATA SETUP ===========

      /**
      * timer function
      */
      def profile[T](op: => T): (Long, T) = {
        val tick = System.nanoTime()
        val result = op
        val duration =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime - tick) // could just stick with nanos for more precision
        (duration, result)
      }

      // container class for the timings we measure
      case class Timing(load: Long, change: Long, delete: Long)

      val NUM_ITERATIONS = 20 // how many times should we loop over the calls-to-be-profiled?
      val waitDuration: Duration = 10.minutes // how long should we wait for any call?

      // recursive function to execute the service calls NUM_ITERATIONS times
      def repeatCalls(iteration: Int, previousTimings: Seq[Timing]): Seq[Timing] =
        if (iteration + 1 > NUM_ITERATIONS) {
          previousTimings
        } else {
          val (loadDuration, _) = profile {
            Await.result(
              testApiService.entityService.batchUpsertEntities(minimalTestData.wsName, initialUpsert, None, None),
              waitDuration
            )
          }
          val (changeDuration, _) = profile {
            Await.result(
              testApiService.entityService.batchUpsertEntities(minimalTestData.wsName, modifiedUpsert, None, None),
              waitDuration
            )
          }
          val (deleteDuration, _) = profile {
            Await.result(testApiService.entityService.deleteEntities(minimalTestData.wsName, refsToDelete, None, None),
                         waitDuration
            )
          }
          val thisTiming = Timing(loadDuration, changeDuration, deleteDuration)

          logger.error(
            s"iteration ${iteration + 1} of $NUM_ITERATIONS: load ${loadDuration}ms, change ${changeDuration}ms, delete ${deleteDuration}ms"
          )

          repeatCalls(iteration + 1, previousTimings :+ thisTiming)
        }

      // launch the loop
      val initialTimings = Seq.empty[Timing]
      val finalTimings = repeatCalls(0, initialTimings)

      // write the timings to the log
      def outputStats(label: String, timings: Seq[Double]): Unit =
        logger.error(
          s"$label: mean ${mean(timings).toInt}ms, min ${min(timings).toInt}ms, max ${max(timings).toInt}ms, stddev ${stddev(timings).toInt}ms, over $NUM_ITERATIONS iterations"
        )
      outputStats("batchUpsert all entities from empty", finalTimings.map(_.load.toDouble))
      outputStats("batchUpsert single value on top of existing", finalTimings.map(_.change.toDouble))
      outputStats("Delete (hide) all entities", finalTimings.map(_.delete.toDouble))

      // beyond outputting timings to the log, this test doesn't assert anything (well, an exception will fail the test)

  }

}
