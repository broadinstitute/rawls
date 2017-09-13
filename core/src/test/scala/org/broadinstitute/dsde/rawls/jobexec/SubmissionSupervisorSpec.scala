package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.concurrent.Eventually

class SubmissionSupervisorSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually with RawlsTestUtils with MockitoTestUtils with RawlsStatsDTestUtils {

  import driver.api._

  def this() = this(ActorSystem("SubmissionSupervisorSpec"))

  val testDbName = "SubmissionSupervisorSpec"

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "SubmissionSupervisor" should "maintain correct submission metrics for multiple active submissions" in withConstantTestDatabase {
    /*
    TODO:
    * send SaveCurrentWorkflowStatusCounts once, check the global and active and sub workflow counts are correct
    * send another one for a different ws/subid, check again
    * NOTE: SaveCurrentWorkflowStatusCounts doesn't update the global gauges, you need UpdateGlobalJobExecGauges for that
     */
  }

  it should "unregister a submission's workflow gauge when the submission completes" in withConstantTestDatabase {
    //todo: test that the workflow gauge gets unregistered and also that the global submission stats are fine
  }

  it should "unregister a workspace's submission gauge when the last submission in a workspace completes" in withConstantTestDatabase {
    //todo: see above
  }
}
