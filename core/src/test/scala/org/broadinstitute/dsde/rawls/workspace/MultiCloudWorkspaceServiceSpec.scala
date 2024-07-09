package org.broadinstitute.dsde.rawls.workspace

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.model._
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.config.{AzureConfig, MultiCloudWorkspaceConfig, MultiCloudWorkspaceManagerConfig}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.{LeonardoDAO, MockLeonardoDAO, SamDAO}
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, MockWorkspaceManagerDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceType.McWorkspace
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeString,
  WorkspaceCloudPlatform,
  WorkspaceName,
  WorkspacePolicy,
  WorkspaceRequest
}
import org.broadinstitute.dsde.rawls.workspace.MultiCloudWorkspaceService.getStorageContainerName
import org.broadinstitute.dsde.workbench.client.leonardo
import org.mockito.ArgumentMatchers.{any, anyString, eq => equalTo}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues}
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class MultiCloudWorkspaceServiceSpec
    extends AnyFlatSpecLike
    with MockitoSugar
    with ScalaFutures
    with Matchers
    with OptionValues
    with TestDriverComponent {
  implicit val actorSystem: ActorSystem = ActorSystem("MultiCloudWorkspaceServiceSpec")
  implicit val workbenchMetricBaseName: ShardId = "test"

}
