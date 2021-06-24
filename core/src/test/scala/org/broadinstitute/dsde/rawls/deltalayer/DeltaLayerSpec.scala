package org.broadinstitute.dsde.rawls.deltalayer

import cats.effect.{Blocker, ContextShift, IO}
import com.google.cloud.bigquery.Acl
import com.google.cloud.bigquery.Acl.Entity
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, MockBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model.{SamPolicy, SamPolicyWithNameAndEmail, SamWorkspacePolicyNames, UserInfo, Workspace}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class DeltaLayerSpec extends AnyFlatSpec with TestDriverComponent with PrivateMethodTester with Matchers {

  implicit val ec = TestExecutionContext.testExecutionContext
  implicit lazy val blocker = Blocker.liftExecutionContext(TestExecutionContext.testExecutionContext)
  implicit lazy val contextShift: ContextShift[IO] = cats.effect.IO.contextShift(executionContext)
  /*
    implicit lazy val logger: _root_.io.chrisdavenport.log4cats.StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit lazy val contextShift: ContextShift[IO] = cats.effect.IO.contextShift(executionContext)
  implicit lazy val timer: Timer[IO] = cats.effect.IO.timer(executionContext)
   */

//  new MockBigQueryServiceFactory("dummy-credential-path", blocker, queryResponse)

  behavior of "DeltaLayer object"

  it should "generate expected name of companion dataset for a workspace" in {
    val inputUuid = "123e4567-e89b-12d3-a456-426614174000"
    val expected = "deltalayer_forworkspace_123e4567_e89b_12d3_a456_426614174000"

    val dummyWorkspace = Workspace("namespace", "name", inputUuid, "bucketName", None, DateTime.now(), DateTime.now(), "createdBy", Map())
    val actual = DeltaLayer.generateDatasetNameForWorkspace(dummyWorkspace)

    assertResult(expected) { actual }
  }

  behavior of "DeltaLayer class"

  it should "add client and streamer ACLs in calculateDatasetAcl" in {
    // create unique "clientEmail" and "deltaLayerStreamerEmail" values for this test
    val clientEmailUnderTest = WorkbenchEmail("Hugo")
    val deltaLayerStreamerEmailUnderTest = WorkbenchEmail("Nemo")

    // create the Delta Layer object and grab the private method "calculateDatasetAcl"
    val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
    val deltaLayer = testDeltaLayer(bqFactory,
      clientEmail = clientEmailUnderTest,
      deltaLayerStreamerEmail = deltaLayerStreamerEmailUnderTest)
    val methodUnderTest = PrivateMethod[Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]]]('calculateDatasetAcl)

    // minimal set of existing policies to pass to calculateDatasetAcl() - it will fail without a projectOwner policy
    val existingPolicies = Set(
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(), Set(), Set()), WorkbenchEmail("dummy-policy-1"))
    )

    // assert that the calculated ACLs include OWNER:clientEmail and WRITER:deltaLayerStreamerEmail
    val actual = deltaLayer invokePrivate methodUnderTest(existingPolicies)
    actual should contain (Acl.Role.OWNER -> Seq((clientEmailUnderTest, Acl.Entity.Type.USER)))
    actual should contain (Acl.Role.WRITER -> Seq((deltaLayerStreamerEmailUnderTest, Acl.Entity.Type.USER)))
  }

  it should "calculate proper ACLs in calculateDatasetAcl" in {
    // create unique "clientEmail" and "deltaLayerStreamerEmail" values for this test
    val clientEmailUnderTest = WorkbenchEmail("George")
    val deltaLayerStreamerEmailUnderTest = WorkbenchEmail("Mugsy")

    // create the Delta Layer object and grab the private method "calculateDatasetAcl"
    val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
    val deltaLayer = testDeltaLayer(bqFactory,
      clientEmail = clientEmailUnderTest,
      deltaLayerStreamerEmail = deltaLayerStreamerEmailUnderTest)
    val methodUnderTest = PrivateMethod[Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]]]('calculateDatasetAcl)

    // minimal set of existing policies to pass to calculateDatasetAcl() - it will fail without a projectOwner policy
    Seq(SamWorkspacePolicyNames.owner, SamWorkspacePolicyNames.writer, SamWorkspacePolicyNames.reader) // included

    // these should be dropped from the calculated ACLs
    val ignoredPolicies = Set(
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.canCompute, SamPolicy(Set(), Set(), Set()), WorkbenchEmail("can-compute")),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.canCatalog, SamPolicy(Set(), Set(), Set()), WorkbenchEmail("can-catalog")),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareReader, SamPolicy(Set(), Set(), Set()), WorkbenchEmail("share-reader")),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.shareWriter, SamPolicy(Set(), Set(), Set()), WorkbenchEmail("share-writer"))
    )

    // these should be included in the calculated ACLs, as readers. The algorithm grabs the _.email, which is the group name
    val propagatedPolicies = Set(
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.reader, SamPolicy(Set(), Set(), Set()), WorkbenchEmail("reader-group")),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.writer, SamPolicy(Set(), Set(), Set()), WorkbenchEmail("writer-group")),
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.owner, SamPolicy(Set(), Set(), Set()), WorkbenchEmail("owner-group"))
    )

    // the first project owner should be included in the calculated ACL, as a reader. The algorithm grabs all _.policy.memberEmails
    val projectOwnerPolicy = Set(
      SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.projectOwner, SamPolicy(Set(WorkbenchEmail("projowner-1"), WorkbenchEmail("projowner-2")), Set(), Set()), WorkbenchEmail("projowner-group-3"))
    )

    val existingPolicies = ignoredPolicies ++ propagatedPolicies ++ projectOwnerPolicy

    val expected = Map(
      Acl.Role.OWNER -> Seq((clientEmailUnderTest, Acl.Entity.Type.USER)),
      Acl.Role.WRITER -> Seq((deltaLayerStreamerEmailUnderTest, Acl.Entity.Type.USER)),
      Acl.Role.READER -> Seq(
        (WorkbenchEmail("reader-group"), Acl.Entity.Type.GROUP),
        (WorkbenchEmail("writer-group"), Acl.Entity.Type.GROUP),
        (WorkbenchEmail("owner-group"), Acl.Entity.Type.GROUP),
        (WorkbenchEmail("projowner-1"), Acl.Entity.Type.GROUP),
        (WorkbenchEmail("projowner-2"), Acl.Entity.Type.GROUP)
      )
    )

    val actual = deltaLayer invokePrivate methodUnderTest(existingPolicies)

    // actual and expected contain lists/vectors as values, which will return unequal if ordered differently.
    // for the sake of comparison, transform to unordered.
    val expectedMap = expected.map { kv => kv._1 -> kv._2.toSet }
    val actualMap = actual.map { kv => kv._1 -> kv._2.toSet }
    assertResult (expectedMap) { actualMap }
  }

  it should "catch/ignore 409s from BigQuery in createDatasetIfNotExist" is (pending)

  it should "bubble up 409s from BigQuery in createDataset" is (pending)

  List("createDatasetIfNotExist", "createDataset") foreach { method =>
    it should s"add appropriate labels to the companion dataset in $method" in {
      val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
      val deltaLayer = testDeltaLayer(bqFactory)
      val methodUnderTest = deltaLayer.getClass.getMethod(method, testData.workspace.getClass, userInfo.getClass)
      methodUnderTest.invoke(deltaLayer, testData.workspace, userInfo)
    }

    it should s"specify some ACLs for the companion dataset in $method" is (pending)

    it should s"use the specified workspace's project for the companion dataset in $method" is (pending)

    it should s"bubble up non-BigQuery 409s in $method" is (pending)

    it should s"bubble up Sam errors in $method" is (pending)

    it should s"create the companion dataset if it doesn't exist in $method" in {
      val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
      val deltaLayer = testDeltaLayer(bqFactory)
      val methodUnderTest = deltaLayer.getClass.getMethod(method, testData.workspace.getClass, userInfo.getClass)
      methodUnderTest.invoke(deltaLayer, testData.workspace, userInfo)
    }
  }


  private def testDeltaLayer(bqServiceFactory: GoogleBigQueryServiceFactory,
                             deltaLayerWriter: DeltaLayerWriter = new MockDeltaLayerWriter,
                             samDAO: SamDAO = new MockSamDAO(slickDataSource),
                             clientEmail: WorkbenchEmail = WorkbenchEmail("unittest-clientEmail"),
                             deltaLayerStreamerEmail: WorkbenchEmail = WorkbenchEmail("unittest-detlaLayerStreamerEmail"))
                            (implicit executionContext: ExecutionContext, contextShift: ContextShift[IO]): DeltaLayer = {

    new DeltaLayer(bqServiceFactory, deltaLayerWriter, samDAO, clientEmail, deltaLayerStreamerEmail)
  }


}
