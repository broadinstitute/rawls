package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectOperationRecord, TestDriverComponent}
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.ExecutionContext

class CreatingBillingProjectMonitorSpec extends MockitoSugar with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually {

  val defaultDataSource: SlickDataSource = slickDataSource
  val defaultExecutionContext: ExecutionContext = executionContext

  val defaultServicePerimeterName: ServicePerimeterName = ServicePerimeterName("accessPolicies/policyName/servicePerimeters/servicePerimeterName")
  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultCromwellBucketUrl = "bucket-url"
  val defaultBillingProjectName = RawlsBillingProjectName("test-bp")

  def getCreatingBillingProjectMonitor(datasource: SlickDataSource = defaultDataSource,
                                       gcsDAO: GoogleServicesDAO = new MockGoogleServicesDAO("test"),
                                       projectTemplate: ProjectTemplate = ProjectTemplate(Map.empty),
                                       samDAO: SamDAO = new MockSamDAO(defaultDataSource)(defaultExecutionContext),
                                       requesterPaysRole: String = "requesterPaysRole")(implicit executionContext: ExecutionContext = defaultExecutionContext): CreatingBillingProjectMonitor = {
    new CreatingBillingProjectMonitor {
      override implicit val executionContext: ExecutionContext = executionContext
      override val datasource: SlickDataSource = datasource
      override val gcsDAO: GoogleServicesDAO = gcsDAO
      override val projectTemplate: ProjectTemplate = projectTemplate
      override val samDAO: SamDAO = samDAO
      override val requesterPaysRole: String = requesterPaysRole
    }
  }

  "CreatingBillingProjectMonitor" should "set project status to 'AddingToPerimeter' when it's been successfully created and it has a service perimeter" in {
    // billing project finished create step, has service perimeter -- should go to adding perimeter
    withEmptyTestDatabase { dataSource =>
      val creatingBillingProjectMonitor = getCreatingBillingProjectMonitor(dataSource)


      val billingProject = RawlsBillingProject(defaultBillingProjectName, defaultCromwellBucketUrl, CreationStatuses.Creating, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(defaultGoogleProjectNumber))
      val creatingOperation = RawlsBillingProjectOperationRecord(billingProject.projectName.value, GoogleOperationNames.DeploymentManagerCreateProject, "opid", true, None, GoogleApiTypes.DeploymentManagerApi)
    }
  }

  it should "update the operations table and call google when projects are being added to the perimeter" in {
    // 3 billing projects in adding to perimeter, no operations, two in same perimeter, one in another -- should call google dao x2 and add operation x3
  }

  it should "do nothing if a polled operation is still running" in {
    // billing project in adding to perimeter, with still running operation -- should call google dao to poll but no state change
  }

  it should "set project status to 'Ready' and operation to done if polled operation is finished" in {
    // billing project in adding to perimeter, with complete operation -- should call google dao to poll and change state (op done, project ready)
  }

  it should "set project state to 'Error' when multiple operations exist for that project" in {
    // billing project in adding to perimeter, with more than 1 operation -- project should error
  }

  it should "set project state to 'Error' when its state is 'AddingToPerimeter' but no perimeter is saved in the DB" in {
    // billing project in adding to perimeter, no operations, no perimeter specified -- project should error
  }

  it should "raise an exception when an existing project in a perimeter has no project number" in {
    // billing project in adding to perimeter, no operations but there is an existing project in perimeter with no project number -- exception
  }
}
