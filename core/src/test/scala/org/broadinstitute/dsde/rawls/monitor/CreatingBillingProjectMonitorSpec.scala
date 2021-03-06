package org.broadinstitute.dsde.rawls.monitor

import java.sql.SQLException

import com.google.api.services.accesscontextmanager.v1.model.Operation
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectOperationRecord, TestDriverComponent}
import org.broadinstitute.dsde.rawls.google.AccessContextManagerDAO
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor.CheckDone
import org.scalatest.concurrent.Eventually
import org.scalatest.BeforeAndAfterEach
import org.mockito.{ArgumentMatcher, ArgumentMatchers}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class CreatingBillingProjectMonitorSpec extends MockitoSugar with AnyFlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterEach with Eventually {
  val defaultExecutionContext: ExecutionContext = executionContext

  val defaultServicePerimeterName: ServicePerimeterName = ServicePerimeterName("accessPolicies/policyName/servicePerimeters/servicePerimeterName")
  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultBillingProjectName = RawlsBillingProjectName("test-bp")

  def getCreatingBillingProjectMonitor(dataSource: SlickDataSource, mockGcsDAO: GoogleServicesDAO = new MockGoogleServicesDAO("test"))(implicit executionContext: ExecutionContext): CreatingBillingProjectMonitor = {
    new CreatingBillingProjectMonitor {
      override implicit val executionContext: ExecutionContext = defaultExecutionContext
      override val datasource: SlickDataSource = dataSource
      override val gcsDAO: GoogleServicesDAO = mockGcsDAO
      override val projectTemplate: ProjectTemplate = ProjectTemplate(Seq.empty, Seq.empty)
      override val samDAO: SamDAO = new MockSamDAO(dataSource)
      override val requesterPaysRole: String = "requesterPaysRole"
    }
  }

  "CreatingBillingProjectMonitor" should "set project status to 'AddingToPerimeter' when it's been successfully created and it has a service perimeter" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Creating, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(defaultGoogleProjectNumber))
      val creatingOperation = RawlsBillingProjectOperationRecord(billingProject.projectName.value, GoogleOperationNames.DeploymentManagerCreateProject, "opid", true, None, GoogleApiTypes.DeploymentManagerApi)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(rawlsBillingProjectQuery.insertOperations(Seq(creatingOperation)))

      val creatingBillingProjectMonitor = getCreatingBillingProjectMonitor(dataSource)

      assertResult(CheckDone(1)) {
        Await.result(creatingBillingProjectMonitor.checkCreatingProjects(), Duration.Inf)
      }

      // the only thing that should change is the status
      assertResult(Some(billingProject.copy(status = CreationStatuses.AddingToPerimeter))) {
        runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName))
      }
    }
  }

  it should "include projects that are already in the perimeter" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val newBillingProjectNumber = GoogleProjectNumber("1")
      val newBillingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(newBillingProjectNumber))
      val existingBillingProjectNumber = GoogleProjectNumber("2")
      val existingBillingProject = RawlsBillingProject(RawlsBillingProjectName("existing-project"), CreationStatuses.Ready, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(existingBillingProjectNumber))

      runAndWait(rawlsBillingProjectQuery.create(newBillingProject))
      runAndWait(rawlsBillingProjectQuery.create(existingBillingProject))

      val mockAcmDAO = mock[AccessContextManagerDAO](RETURNS_SMART_NULLS)
      when(mockAcmDAO.overwriteProjectsInServicePerimeter(ArgumentMatchers.any[ServicePerimeterName], ArgumentMatchers.any[Seq[String]])).thenReturn(Future.successful(new Operation().setDone(false).setName("test-op-id")))

      val mockGcsDAO = new MockGoogleServicesDAO("test", accessContextManagerDAO = mockAcmDAO)
      val creatingBillingProjectMonitor = getCreatingBillingProjectMonitor(dataSource, mockGcsDAO)

      assertResult(CheckDone(1)) {
        Await.result(creatingBillingProjectMonitor.checkCreatingProjects(), Duration.Inf)
      }

      // seqs are ordered, but we don't care about that so this will match regardless of order
      val seqMatcher = new ArgumentMatcher[Seq[String]] {
        override def matches(argument: Seq[String]): Boolean = {
          val expected = Seq(newBillingProjectNumber.value, existingBillingProjectNumber.value)
          expected.sorted == argument.sorted
        }
      }
      verify(mockAcmDAO, times(1)).overwriteProjectsInServicePerimeter(ArgumentMatchers.eq(defaultServicePerimeterName), ArgumentMatchers.argThat(seqMatcher))
    }
  }

  it should "update the operations table and call google when projects are being added to the perimeter" in {
    // 3 billing projects in adding to perimeter, no operations, two in same perimeter, one in another -- should call google dao x2 and add operation x3

    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val projectNumber1 = GoogleProjectNumber("1")
      val billingProject1 = RawlsBillingProject(RawlsBillingProjectName("test-bp1"), CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(projectNumber1))

      val projectNumber2 = GoogleProjectNumber("2")
      val billingProject2 = RawlsBillingProject(RawlsBillingProjectName("test-bp2"), CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(projectNumber2))

      val otherServicePerimeter = ServicePerimeterName("other-perimeter")
      val projectNumber3 = GoogleProjectNumber("3")
      val billingProject3 = RawlsBillingProject(RawlsBillingProjectName("test-bp3"), CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = Option(otherServicePerimeter), googleProjectNumber = Option(projectNumber3))

      runAndWait(rawlsBillingProjectQuery.create(billingProject1))
      runAndWait(rawlsBillingProjectQuery.create(billingProject2))
      runAndWait(rawlsBillingProjectQuery.create(billingProject3))

      val mockAcmDAO = mock[AccessContextManagerDAO](RETURNS_SMART_NULLS)

      val otherPerimeterOpId = "add-to-other-perimeter"
      when(mockAcmDAO.overwriteProjectsInServicePerimeter(otherServicePerimeter, Seq(projectNumber3.value))).thenReturn(Future.successful(new Operation().setDone(false).setName(otherPerimeterOpId)))

      // seqs are ordered, but we don't care about that so this will match regardless of order
      val seqMatcher = new ArgumentMatcher[Seq[String]] {
        override def matches(argument: Seq[String]): Boolean = {
          val expected = Seq(projectNumber1.value, projectNumber2.value)
          expected.sorted == argument.sorted
        }
      }
      val defaultPerimeterOpId = "two-projects-default-perimeter"
      when(mockAcmDAO.overwriteProjectsInServicePerimeter(ArgumentMatchers.eq(defaultServicePerimeterName), ArgumentMatchers.argThat(seqMatcher))).thenReturn(Future.successful(new Operation().setDone(false).setName(defaultPerimeterOpId)))

      val mockGcsDAO = new MockGoogleServicesDAO("test", accessContextManagerDAO = mockAcmDAO)
      val creatingBillingProjectMonitor = getCreatingBillingProjectMonitor(dataSource, mockGcsDAO)

      assertResult(CheckDone(3)) {
        Await.result(creatingBillingProjectMonitor.checkCreatingProjects(), Duration.Inf)
      }

      verify(mockAcmDAO, times(1)).overwriteProjectsInServicePerimeter(otherServicePerimeter, Seq(projectNumber3.value))
      verify(mockAcmDAO, times(1)).overwriteProjectsInServicePerimeter(ArgumentMatchers.eq(defaultServicePerimeterName), ArgumentMatchers.argThat(seqMatcher))
      runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(billingProject1.projectName, billingProject2.projectName, billingProject3.projectName), GoogleOperationNames.AddProjectToPerimeter)) should contain theSameElementsAs Seq(
        RawlsBillingProjectOperationRecord(billingProject1.projectName.value, GoogleOperationNames.AddProjectToPerimeter, defaultPerimeterOpId, false, None, GoogleApiTypes.AccessContextManagerApi),
        RawlsBillingProjectOperationRecord(billingProject2.projectName.value, GoogleOperationNames.AddProjectToPerimeter, defaultPerimeterOpId, false, None, GoogleApiTypes.AccessContextManagerApi),
        RawlsBillingProjectOperationRecord(billingProject3.projectName.value, GoogleOperationNames.AddProjectToPerimeter, otherPerimeterOpId, false, None, GoogleApiTypes.AccessContextManagerApi)
      )
    }
  }

  it should "do nothing if a polled operation is still running" in {
    // billing project in adding to perimeter, with still running operation -- should call google dao to poll but no state change

    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(defaultGoogleProjectNumber))
      val addingProjectToPerimeterOperation = RawlsBillingProjectOperationRecord(billingProject.projectName.value, GoogleOperationNames.AddProjectToPerimeter, "opid", false, None, GoogleApiTypes.AccessContextManagerApi)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(rawlsBillingProjectQuery.insertOperations(Seq(addingProjectToPerimeterOperation)))

      val stillRunningDao = new MockGoogleServicesDAO("no-change-operation") {
        override def pollOperation(operationId: OperationId): Future[OperationStatus] = Future.successful(OperationStatus(false, None))
      }

      val creatingBillingProjectMonitor = getCreatingBillingProjectMonitor(dataSource, stillRunningDao)

      assertResult(CheckDone(1)) {
        Await.result(creatingBillingProjectMonitor.checkCreatingProjects(), Duration.Inf)
      }

      assertResult(Seq(CreationStatuses.AddingToPerimeter)) {
        runAndWait(rawlsBillingProjectQuery.getBillingProjects(Set(billingProject.projectName))).map(_.status)
      }
    }
  }

  it should "set project status to 'Ready' and operation to done if polled operation is finished" in {
    // billing project in adding to perimeter, with complete operation -- should call google dao to poll and change state (op done, project ready)

    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(defaultGoogleProjectNumber))
      val addingProjectToPerimeterOperation = RawlsBillingProjectOperationRecord(billingProject.projectName.value, GoogleOperationNames.AddProjectToPerimeter, "opid", false, None, GoogleApiTypes.AccessContextManagerApi)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(rawlsBillingProjectQuery.insertOperations(Seq(addingProjectToPerimeterOperation)))

      // mock GcsDAO returns a done OperationStatus for pollOperation by default, so no need to override
      val creatingBillingProjectMonitor = getCreatingBillingProjectMonitor(dataSource)

      assertResult(CheckDone(1)) {
        Await.result(creatingBillingProjectMonitor.checkCreatingProjects(), Duration.Inf)
      }

      // assert project status is ready
      assertResult(Some(billingProject.copy(status = CreationStatuses.Ready))) {
        runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName))
      }

      // assert add project to perimeter operation is done
      assertResult(Seq(addingProjectToPerimeterOperation.copy(done = true))) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(billingProject.projectName), addingProjectToPerimeterOperation.operationName))
      }
    }
  }

  it should "raise an exception for duplicate primary keys when multiple operations with the same operation name exist for a single project" in {
    // billing project in adding to perimeter, with more than 1 operation -- project should error
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(defaultGoogleProjectNumber))
      val addingProjectToPerimeterOperation = RawlsBillingProjectOperationRecord(billingProject.projectName.value, GoogleOperationNames.AddProjectToPerimeter, "opid", false, None, GoogleApiTypes.AccessContextManagerApi)
      val extraAddProjectToPerimeterOperation = RawlsBillingProjectOperationRecord(billingProject.projectName.value, GoogleOperationNames.AddProjectToPerimeter, "opid", false, None, GoogleApiTypes.AccessContextManagerApi)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      intercept[SQLException] {
        runAndWait(rawlsBillingProjectQuery.insertOperations(Seq(addingProjectToPerimeterOperation, extraAddProjectToPerimeterOperation)))
      }
    }
  }

  it should "set project state to 'Error' when its state is 'AddingToPerimeter' but no perimeter is saved in the DB" in {
    // billing project in adding to perimeter, no operations, no perimeter specified -- project should error
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = None, googleProjectNumber = Option(defaultGoogleProjectNumber))

      runAndWait(rawlsBillingProjectQuery.create(billingProject))

      val creatingBillingProjectMonitor = getCreatingBillingProjectMonitor(dataSource)

      assertResult(CheckDone(1)) {
        Await.result(creatingBillingProjectMonitor.checkCreatingProjects(), Duration.Inf)
      }

      // the only thing that should change is the status
      assertResult(Some(CreationStatuses.Error)) {
        runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName)).map(_.status)
      }
    }
  }

  it should "raise an exception when an existing project in a perimeter has no project number" in {
    // billing project in adding to perimeter, no operations but there is an existing project in perimeter with no project number -- exception
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(defaultGoogleProjectNumber))
      val existingProjectWithoutNumber = RawlsBillingProject(RawlsBillingProjectName("no-google-project-number"), CreationStatuses.Ready, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = None)

      runAndWait(rawlsBillingProjectQuery.create(billingProject))
      runAndWait(rawlsBillingProjectQuery.create(existingProjectWithoutNumber))

      val creatingBillingProjectMonitor = getCreatingBillingProjectMonitor(dataSource)

      intercept[RawlsException] {
        Await.result(creatingBillingProjectMonitor.checkCreatingProjects(), Duration.Inf)
      }

      // statuses for these billing projects should be unchanged
      assertResult(Some(billingProject)) {
        runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName))
      }
    }
  }

  it should "set project state to 'Error' when call to Google fails" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val number = GoogleProjectNumber("1")
      val billingProject = RawlsBillingProject(RawlsBillingProjectName("test-bp"), CreationStatuses.AddingToPerimeter, None, None, servicePerimeter = Option(defaultServicePerimeterName), googleProjectNumber = Option(number))

      runAndWait(rawlsBillingProjectQuery.create(billingProject))

      val mockAcmDAO = mock[AccessContextManagerDAO](RETURNS_SMART_NULLS)
      when(mockAcmDAO.overwriteProjectsInServicePerimeter(defaultServicePerimeterName, Seq(number.value))).thenReturn(Future.failed(new Exception("this failed")))

      val mockGcsDAO = new MockGoogleServicesDAO("test", accessContextManagerDAO = mockAcmDAO)
      val creatingBillingProjectMonitor = getCreatingBillingProjectMonitor(dataSource, mockGcsDAO)

      assertResult(CheckDone(1)) {
        Await.result(creatingBillingProjectMonitor.checkCreatingProjects(), Duration.Inf)
      }

      assertResult(Some(CreationStatuses.Error)) {
        runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName)).map(_.status)
      }
    }
  }
}
