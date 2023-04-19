package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model._
import org.scalatest.OptionValues

import java.util.UUID
import scala.language.implicitConversions

/**
 * Created by dvoet on 2/8/16.
 */
class WorkspaceComponentSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with WorkspaceComponent
    with RawlsTestUtils
    with OptionValues {
  val workspaceId: UUID = UUID.randomUUID()
  val googleProjectId: GoogleProjectId = GoogleProjectId("test_google_project")
  val googleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("123456789")
  val workspaceVersion: WorkspaceVersions.V2.type = WorkspaceVersions.V2
  val workspaceBillingAccount: RawlsBillingAccountName = RawlsBillingAccountName("billing_account_name")

  val workspace: Workspace = Workspace(
    "test_namespace",
    "test_name",
    workspaceId.toString,
    "bucketname",
    Some("workflow-collection"),
    currentTime(),
    currentTime(),
    "me",
    Map(
      AttributeName.withDefaultNS("attributeString") -> AttributeString("value"),
      AttributeName.withDefaultNS("attributeBool") -> AttributeBoolean(true),
      AttributeName.withDefaultNS("attributeNum") -> AttributeNumber(3.14159)
    ),
    false,
    workspaceVersion,
    googleProjectId,
    Option(googleProjectNumber),
    Option(workspaceBillingAccount),
    None,
    Option(currentTime()),
    WorkspaceType.RawlsWorkspace
  )

  "WorkspaceComponent" should "crud workspaces" in withEmptyTestDatabase {

    assertResult(None) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertResult(None) {
      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
    }

    assertResult(None) {
      runAndWait(workspaceQuery.findV2WorkspaceByName(workspace.toWorkspaceName))
    }

    assertWorkspaceResult(workspace) {
      runAndWait(workspaceQuery.createOrUpdate(workspace))
    }

    assertWorkspaceResult(Option(workspace)) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertWorkspaceResult(Seq(workspace)) {
      runAndWait(workspaceQuery.listByIds(Seq(workspaceId)))
    }

    assertWorkspaceResult(Seq(workspace)) {
      runAndWait(workspaceQuery.listV2WorkspacesByIds(Seq(workspaceId)))
    }

    assertWorkspaceResult(Option(workspace)) {
      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
    }

    assertWorkspaceResult(Option(workspace)) {
      runAndWait(workspaceQuery.findV2WorkspaceByName(workspace.toWorkspaceName))
    }

    assertResult(Option(UUID.fromString(workspace.workspaceId))) {
      runAndWait(workspaceQuery.getWorkspaceId(workspace.toWorkspaceName))
    }

    assertResult(Option(UUID.fromString(workspace.workspaceId))) {
      runAndWait(workspaceQuery.getV2WorkspaceId(workspace.toWorkspaceName))
    }

    assertResult(1) {
      runAndWait(workspaceQuery.countByNamespace(RawlsBillingProjectName(workspace.namespace)))
    }

    val updatedWorkspace = workspace.copy(
      attributes = Map(AttributeName("default", "attributeString") -> AttributeString("value2"),
                       AttributeName("library", "attributeBool") -> AttributeBoolean(false)
      )
    )

    assertWorkspaceResult(updatedWorkspace) {
      runAndWait(workspaceQuery.createOrUpdate(updatedWorkspace))
    }

    assertWorkspaceResult(Option(updatedWorkspace)) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertWorkspaceResult(Option(updatedWorkspace)) {
      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
    }

    assertWorkspaceResult(Option(updatedWorkspace)) {
      runAndWait(workspaceQuery.findV2WorkspaceByName(workspace.toWorkspaceName))
    }

    assertResult(true) {
      runAndWait(workspaceQuery.delete(workspace.toWorkspaceName))
    }

    assertResult(None) {
      runAndWait(workspaceQuery.findById(workspaceId.toString))
    }

    assertResult(None) {
      runAndWait(workspaceQuery.findByName(workspace.toWorkspaceName))
    }

    assertResult(None) {
      runAndWait(workspaceQuery.findV2WorkspaceByName(workspace.toWorkspaceName))
    }

    assertResult(false) {
      runAndWait(workspaceQuery.delete(workspace.toWorkspaceName))
    }
  }

  it should "save workspaceVersion" in withEmptyTestDatabase {
    val savedWorkspace = runAndWait(workspaceQuery.createOrUpdate(workspace))
    savedWorkspace.workspaceVersion shouldBe workspaceVersion
  }

  it should "save googleProjectId" in withEmptyTestDatabase {
    val savedWorkspace = runAndWait(workspaceQuery.createOrUpdate(workspace))
    savedWorkspace.googleProjectId shouldBe googleProjectId
  }

  it should "save googleProjectNumber" in withEmptyTestDatabase {
    val savedWorkspace = runAndWait(workspaceQuery.createOrUpdate(workspace))
    savedWorkspace.googleProjectNumber.value shouldBe googleProjectNumber
  }

  // 5 Billing Projects total
  // 3 Billing Projects in the same Perimeter
  //    - BillingProject1 has 2 or more Workspaces
  //    - BillingProject2 has 1 Workspace
  //    - BillingProject3 has 0 Workspaces
  // 1 Billing Project with a DIFFERENT Perimeter and 1 Workspace
  // 1 Billing Project with NO Service Perimeter and 1 Workspace
  it should "should list Workspaces for a specific Service Perimeter" in withEmptyTestDatabase {
    // Use the same Billing Account for
    val billingAccountName = RawlsBillingAccountName("fakeBillingAccount")
    val servicePerimeterName = ServicePerimeterName("myPerimeter")
    val otherServicePerimeterName = ServicePerimeterName("someOtherPerimeter")

    // Billing Project names
    val billingProject1Name = RawlsBillingProjectName("project_1_InPerimeter")
    val billingProject2Name = RawlsBillingProjectName("project_2_InPerimeter")
    val billingProject3Name = RawlsBillingProjectName("project_3_InPerimeter")
    val billingProjectNameWithoutPerimeter = RawlsBillingProjectName("projectNotInPerimeter")
    val billingProjectNameInOtherPerimeter = RawlsBillingProjectName("projectInOtherPerimeter")

    // 3 Billing Projects with same Perimeter
    val billingProject1 = RawlsBillingProject(billingProject1Name,
                                              CreationStatuses.Ready,
                                              Option(billingAccountName),
                                              None,
                                              servicePerimeter = Option(servicePerimeterName)
    )
    val billingProject2 = billingProject1.copy(projectName = billingProject2Name)
    val billingProject3 = billingProject1.copy(projectName = billingProject3Name)

    // 1 Billing Project in a DIFFERENT Perimeter
    val billingProjectInOtherPerimeter = billingProject1.copy(projectName = billingProjectNameInOtherPerimeter,
                                                              servicePerimeter = Option(otherServicePerimeterName)
    )

    // 1 Billing Project without a Perimeter
    val billingProjectWithoutPerimeter =
      billingProject1.copy(projectName = billingProjectNameWithoutPerimeter, servicePerimeter = None)

    runAndWait(rawlsBillingProjectQuery.create(billingProject1))
    runAndWait(rawlsBillingProjectQuery.create(billingProject2))
    runAndWait(rawlsBillingProjectQuery.create(billingProject3))
    runAndWait(rawlsBillingProjectQuery.create(billingProjectInOtherPerimeter))
    runAndWait(rawlsBillingProjectQuery.create(billingProjectWithoutPerimeter))

    // Create 2 Workspaces in BillingProject1
    val workspacesInBillingProject1 = 1 to 2 map { n =>
      workspace.copy(
        namespace = billingProject1Name.value,
        name = s"workspace${n}",
        workspaceId = UUID.randomUUID().toString,
        googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString))
      )
    }
    workspacesInBillingProject1.foreach { workspace =>
      runAndWait(workspaceQuery.createOrUpdate(workspace))
    }

    // Create 1 Workspace in BillingProject2
    val workspaceInBillingProject2 = workspace.copy(
      namespace = billingProject2Name.value,
      name = "workspaceInBP2",
      workspaceId = UUID.randomUUID().toString,
      googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString))
    )
    runAndWait(workspaceQuery.createOrUpdate(workspaceInBillingProject2))

    // Create 1 Workspace in BillingProjectWithOtherPerimeter
    val workspaceInBillingProjectWithOtherPerimeter = workspace.copy(
      namespace = billingProjectNameInOtherPerimeter.value,
      name = "workspaceInOtherPerimeter",
      workspaceId = UUID.randomUUID().toString,
      googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString))
    )
    runAndWait(workspaceQuery.createOrUpdate(workspaceInBillingProjectWithOtherPerimeter))

    // Create 1 Workspace in BillingProjectWithoutPerimeter
    val workspaceInBillingProjectWithoutPerimeter = workspace.copy(
      namespace = billingProjectNameWithoutPerimeter.value,
      name = "IDontCareAnymore",
      workspaceId = UUID.randomUUID().toString,
      googleProjectNumber = Option(GoogleProjectNumber(UUID.randomUUID().toString))
    )
    runAndWait(workspaceQuery.createOrUpdate(workspaceInBillingProjectWithoutPerimeter))

    val expectedWorkspacesInPerimeter = workspacesInBillingProject1 :+ workspaceInBillingProject2

    // Done with test setup

    val actualWorkspacesInPerimeter = runAndWait(workspaceQuery.getWorkspacesInPerimeter(servicePerimeterName))
    actualWorkspacesInPerimeter.map(_.name) should contain theSameElementsAs expectedWorkspacesInPerimeter.map(_.name)
  }

  it should "list submission summary stats" in withDefaultTestDatabase {
    implicit def toWorkspaceId(ws: Workspace): UUID = UUID.fromString(ws.workspaceId)

    // no submissions: 0 successful, 0 failed, 0 running
    val wsIdNoSubmissions: UUID = testData.workspaceNoSubmissions
    assertResult(Map(wsIdNoSubmissions -> WorkspaceSubmissionStats(None, None, 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdNoSubmissions)))
    }

    // successful submission: 1 successful, 0 failed, 0 running
    val wsIdSuccessfulSubmission: UUID = testData.workspaceSuccessfulSubmission
    assertResult(Map(wsIdSuccessfulSubmission -> WorkspaceSubmissionStats(Some(testDate), None, 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdSuccessfulSubmission)))
    }

    // failed submission: 0 successful, 1 failed, 0 running
    val wsIdFailedSubmission: UUID = testData.workspaceFailedSubmission
    assertResult(Map(wsIdFailedSubmission -> WorkspaceSubmissionStats(None, Some(testDate), 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdFailedSubmission)))
    }

    // submitted submissions: 0 successful, 0 failed, 1 running
    val wsIdSubmittedSubmission: UUID = testData.workspaceSubmittedSubmission
    assertResult(Map(wsIdSubmittedSubmission -> WorkspaceSubmissionStats(None, None, 1))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdSubmittedSubmission)))
    }

    // mixed submissions: 0 successful, 1 failed, 1 running
    val wsIdMixedSubmission: UUID = testData.workspaceMixedSubmissions
    assertResult(Map(wsIdMixedSubmission -> WorkspaceSubmissionStats(None, Some(testDate), 1))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdMixedSubmission)))
    }

    // terminated submissions: 1 successful, 1 failed, 0 running
    val wsIdTerminatedSubmission: UUID = testData.workspaceTerminatedSubmissions
    assertResult(Map(wsIdTerminatedSubmission -> WorkspaceSubmissionStats(Some(testDate), Some(testDate), 0))) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdTerminatedSubmission)))
    }

    // interleaved submissions: 1 successful, 1 failed, 0 running
    val wsIdInterleavedSubmissions: UUID = testData.workspaceInterleavedSubmissions
    assertResult(
      Map(wsIdInterleavedSubmissions -> WorkspaceSubmissionStats(Option(testData.t4), Option(testData.t3), 0))
    ) {
      runAndWait(workspaceQuery.listSubmissionSummaryStats(Seq(wsIdInterleavedSubmissions)))
    }
  }
}
