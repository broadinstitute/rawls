package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, LeonardoDAO, SamDAO}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.WorkspaceDeletionRunner
import org.broadinstitute.dsde.rawls.util.Retry
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.{AnyFlatSpec, AnyFlatSpecLike}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext
import scala.language.postfixOps
import org.mockito.ArgumentMatchers

class WorkspaceDeletionRunnerSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures{
implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

 behavior of "something"


  it should "do something" in {
//    val deletionRunner = new WorkspaceDeletionRunner(
//      mock[SamDAO], mock[WorkspaceManagerDAO], mock[WorkspaceRepository], mock[LeonardoDAO], mock[GoogleServicesDAO]
//    )
  }
}
