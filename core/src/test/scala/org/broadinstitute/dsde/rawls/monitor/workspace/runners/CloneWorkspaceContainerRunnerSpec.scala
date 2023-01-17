package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.RawlsUserEmail
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.CloneWorkspaceContainerRunnerSpec.monitorRecord
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.ExecutionContext

object CloneWorkspaceContainerRunnerSpec {
  val userEmail: String = "user@email.com"
  val workspaceId: UUID = UUID.randomUUID()

  val monitorRecord: WorkspaceManagerResourceMonitorRecord =
    WorkspaceManagerResourceMonitorRecord.forCloneWorkspaceContainer(
      UUID.randomUUID(),
      workspaceId,
      RawlsUserEmail(userEmail)
    )
}

class CloneWorkspaceContainerRunnerSpec extends AnyFlatSpecLike with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "initial setup and basic requirements updating workspace cloning status"

  it should "return a completed status if the workspace id is None" in {
    val runner = new CloneWorkspaceContainerRunner(
      mock[SamDAO],
      mock[WorkspaceManagerDAO],
      mock[SlickDataSource],
      mock[GoogleServicesDAO]
    )
    whenReady(runner(monitorRecord.copy(workspaceId = None)))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )
  }

}
