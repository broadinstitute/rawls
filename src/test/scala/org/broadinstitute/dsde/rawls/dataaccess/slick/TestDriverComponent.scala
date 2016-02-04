package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.scalatest.{FlatSpec, BeforeAndAfterAll, Matchers}
import slick.backend.DatabaseConfig
import slick.driver.JdbcProfile
import slick.driver.H2Driver.api._

import scala.concurrent.duration._
import scala.concurrent.Await

/**
 * Created by dvoet on 2/3/16.
 */
trait TestDriverComponent extends FlatSpec with DriverComponent with Matchers with BeforeAndAfterAll with AllComponents {

  override implicit val executionContext = TestExecutionContext.testExecutionContext

  val databaseConfig: DatabaseConfig[JdbcProfile] = DatabaseConfig.forConfig[JdbcProfile]("h2mem1")
  override val driver: JdbcProfile = databaseConfig.driver
  val database = databaseConfig.db

  protected def runAndWait[R](action: DBIOAction[R, _ <: NoStream, _ <: Effect], duration: Duration = 1 minutes): R = {
    Await.result(database.run(action.transactionally), duration)
  }

  import driver.api._

  override def beforeAll: Unit = {
    runAndWait(allSchemas.create)
  }

  override def afterAll: Unit = {
    runAndWait(allSchemas.drop)
  }

}
