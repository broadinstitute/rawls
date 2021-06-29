package org.broadinstitute.dsde.rawls.deltalayer

import cats.effect.{Blocker, ContextShift, IO, Resource}
import com.google.cloud.bigquery.Acl.Entity
import com.google.cloud.bigquery.{Acl, BigQueryException, DatasetId}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, MockBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, SamPolicy, SamPolicyWithNameAndEmail, SamWorkspacePolicyNames, Workspace}
import org.broadinstitute.dsde.workbench.google2.GoogleBigQueryService
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.lang.reflect.InvocationTargetException
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class DeltaLayerSpec extends AsyncFreeSpec with TestDriverComponent with PrivateMethodTester with MockitoSugar with Matchers with Eventually  {

  override implicit val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext
  implicit lazy val blocker: Blocker = Blocker.liftExecutionContext(TestExecutionContext.testExecutionContext)
  implicit lazy val contextShift: ContextShift[IO] = cats.effect.IO.contextShift(executionContext)

  // durations for async/eventually calls
  val timeout: FiniteDuration = 10000.milliseconds
  val interval: FiniteDuration = 500.milliseconds

  "DeltaLayer object" - {
    "should generate expected name of companion dataset for a workspace" in {
      val inputUuid = "123e4567-e89b-12d3-a456-426614174000"
      val expected = "deltalayer_forworkspace_123e4567_e89b_12d3_a456_426614174000"

      val dummyWorkspace = Workspace("namespace", "name", inputUuid, "bucketName", None, DateTime.now(), DateTime.now(), "createdBy", Map())
      val actual = DeltaLayer.generateDatasetNameForWorkspace(dummyWorkspace)

      assertResult(expected) { actual }
      succeed
    }
  }


  "DeltaLayer class" - {

    "should add client and streamer ACLs in calculateDatasetAcl" in {
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
  
    "should calculate proper ACLs in calculateDatasetAcl" in {
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
  
    "should catch/ignore 409s from BigQuery in createDatasetIfNotExist" in {
      // mock GoogleBigQueryService that throws error on createDataset
      val throwingBQService = mock[GoogleBigQueryService[IO]]
      when(throwingBQService.createDataset(any(), any(), any())).thenThrow(new BigQueryException(409, s"BigQuery 409 errors via createDataset should bubble up"))
      // mock GoogleBigQueryServiceFactory that returns the mock GoogleBigQueryService
      val mockBQFactory = mock[GoogleBigQueryServiceFactory]
      when(mockBQFactory.getServiceForProject(any())).thenReturn(Resource.pure[IO, GoogleBigQueryService[IO]](throwingBQService))

      val deltaLayer = testDeltaLayer(mockBQFactory)

      deltaLayer.createDatasetIfNotExist(constantData.workspace, userInfo) map {
        case (didCreate, datasetId) =>
          didCreate shouldBe false
          datasetId shouldBe DatasetId.of(
            constantData.workspace.googleProject.value,
            s"deltalayer_forworkspace_${constantData.workspace.workspaceId.replace("-","_")}")
      }
    }
  
    "should bubble up 409s from BigQuery in createDataset" in {
      // mock GoogleBigQueryService that throws error on createDataset
      val throwingBQService = mock[GoogleBigQueryService[IO]]
      when(throwingBQService.createDataset(any(), any(), any())).thenThrow(new BigQueryException(409, s"BigQuery 409 errors via createDataset should bubble up"))
      // mock GoogleBigQueryServiceFactory that returns the mock GoogleBigQueryService
      val mockBQFactory = mock[GoogleBigQueryServiceFactory]
      when(mockBQFactory.getServiceForProject(any())).thenReturn(Resource.pure[IO, GoogleBigQueryService[IO]](throwingBQService))

      val deltaLayer = testDeltaLayer(mockBQFactory)

      val futureEx = recoverToExceptionIf[BigQueryException] {
        deltaLayer.createDataset(constantData.workspace, userInfo)
      }

      futureEx map { caught =>
        caught.getMessage shouldBe (s"BigQuery 409 errors via createDataset should bubble up")
        caught.getCode shouldBe (409)
      }
    }
  
    List("createDatasetIfNotExist", "createDataset") foreach { method =>
      s"$method method" - {
      
        s"should create the companion dataset if it doesn't exist in $method" in {
          // for unit tests, we don't actually check to see if a call goes out to the cloud to create the dataset.
          // we just test that we properly call the low-level DeltaLayer.bqCreate method, which contains the call to the cloud
    
          // create the Delta Layer instance and spy on it
          val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
          val deltaLayerSpy = spy(testDeltaLayer(bqFactory))
          // find the createDataset/createDatasetIfNotExist method we want to test, then invoke it
          val methodUnderTest = deltaLayerSpy.getClass.getMethod(method, constantData.workspace.getClass, userInfo.getClass)
          methodUnderTest.invoke(deltaLayerSpy, constantData.workspace, userInfo)
    
          // wait on futures until we see the bqCreate method being invoked
          eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
            verify(deltaLayerSpy, times(1)).bqCreate(any(), any(), any(), any())
          }
          succeed
        }
    
        s"should add appropriate labels to the companion dataset in $method" in {
          // create the Delta Layer instance and spy on it
          val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
          val deltaLayerSpy = spy(testDeltaLayer(bqFactory))
          // find the createDataset/createDatasetIfNotExist method we want to test, then invoke it
          val methodUnderTest = deltaLayerSpy.getClass.getMethod(method, constantData.workspace.getClass, userInfo.getClass)
          methodUnderTest.invoke(deltaLayerSpy, constantData.workspace, userInfo)
          // capture the labels being sent to the lowlevel create-bigquery-dataset call
          val datasetLabelsCaptor: ArgumentCaptor[Map[String, String]] = ArgumentCaptor.forClass(classOf[Map[String, String]])
          // wait on futures until we see the bqCreate method being invoked
          eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
            verify(deltaLayerSpy, times(1)).bqCreate(any(), any(), datasetLabelsCaptor.capture(), any())
          }
          // assert the labels actually in use match what we want
          val actual = datasetLabelsCaptor.getValue
          val expected = Map("workspace_id" -> constantData.workspace.workspaceId)
          assertResult(expected) { actual }
        }
    
        s"should specify some ACLs for the companion dataset in $method" in {
          // create unique "clientEmail" and "deltaLayerStreamerEmail" values for this test
          val clientEmailUnderTest = WorkbenchEmail("Fluffy")
          val deltaLayerStreamerEmailUnderTest = WorkbenchEmail("Patches")
    
          // create the Delta Layer instance and spy on it
          val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
          val deltaLayerSpy = spy(testDeltaLayer(bqFactory,
            clientEmail = clientEmailUnderTest,
            deltaLayerStreamerEmail = deltaLayerStreamerEmailUnderTest))
          // find the createDataset/createDatasetIfNotExist method we want to test, then invoke it
          val methodUnderTest = deltaLayerSpy.getClass.getMethod(method, constantData.workspace.getClass, userInfo.getClass)
          methodUnderTest.invoke(deltaLayerSpy, constantData.workspace, userInfo)
          // capture the ACLs being sent to the lowlevel create-bigquery-dataset call
          val aclBindingsCaptor: ArgumentCaptor[Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]]] = ArgumentCaptor.forClass(classOf[Map[Acl.Role, Seq[(WorkbenchEmail, Entity.Type)]]])
          // wait on futures until we see the bqCreate method being invoked
          eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
            verify(deltaLayerSpy, times(1)).bqCreate(any(), any(), any(), aclBindingsCaptor.capture())
          }
          // assert the ACLs actually in use matches what we want
          val actual = aclBindingsCaptor.getValue
    
          // expected ACLs add the clientEmail as owner, deltaLayerStreamerEmail as writer, and the pre-existing workspace owner/writer/reader as reader
          val expected = Map(
            Acl.Role.OWNER -> Seq((clientEmailUnderTest, Acl.Entity.Type.USER)),
            Acl.Role.WRITER -> Seq((deltaLayerStreamerEmailUnderTest, Acl.Entity.Type.USER)),
            Acl.Role.READER -> Seq(
              // the *@example.com values below match what is returned from MockSamDAO's listPoliciesForResource
              (WorkbenchEmail("owner@example.com"), Acl.Entity.Type.GROUP),
              (WorkbenchEmail("writer@example.com"), Acl.Entity.Type.GROUP),
              (WorkbenchEmail("reader@example.com"), Acl.Entity.Type.GROUP)
            )
          )
    
          // test the ordered seqs/lists/vectors individually
          actual.keys should contain theSameElementsAs expected.keys
          actual.keys foreach { key =>
            actual(key) should contain theSameElementsAs expected(key)
          }
          succeed
        }
    
        s"should use the specified workspace's project for the companion dataset in $method" in {
          // create the Delta Layer instance and spy on it
          val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
          val deltaLayerSpy = spy(testDeltaLayer(bqFactory))
          // find the createDataset/createDatasetIfNotExist method we want to test, then invoke it
          val methodUnderTest = deltaLayerSpy.getClass.getMethod(method, constantData.workspace.getClass, userInfo.getClass)
          methodUnderTest.invoke(deltaLayerSpy, constantData.workspace, userInfo)
          // capture the google project being sent to the lowlevel create-bigquery-dataset call
          val googleProjectIdCaptor: ArgumentCaptor[GoogleProjectId] = ArgumentCaptor.forClass(classOf[GoogleProjectId])
          // wait on futures until we see the bqCreate method being invoked
          eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
            verify(deltaLayerSpy, times(1)).bqCreate(googleProjectIdCaptor.capture(), any(), any(), any())
          }
          // assert the google project actually in use matches what we want
          val actual = googleProjectIdCaptor.getValue
          val expected = constantData.workspace.googleProject
          assertResult(expected) { actual }
        }

        s"should bubble up synchronous GoogleBigQueryServiceFactory exceptions in $method" in {
          // mock GoogleBigQueryServiceFactory that returns the mock GoogleBigQueryService
          val mockBQFactory = mock[GoogleBigQueryServiceFactory]
          when(mockBQFactory.getServiceForProject(any())).thenThrow(new RuntimeException(s"Errors getting the GoogleBigQueryService via $method should bubble up"))

          val deltaLayer = testDeltaLayer(mockBQFactory)

          val futureEx = recoverToExceptionIf[RuntimeException] {
            method match {
              case "createDataset" => deltaLayer.createDataset(constantData.workspace, userInfo)
              case "createDatasetIfNotExist" => deltaLayer.createDatasetIfNotExist(constantData.workspace, userInfo)
              case _ => fail(s"test encountered unexpected method '$method'")
            }
          }

          futureEx map { caught =>
            caught.getMessage shouldBe (s"Errors getting the GoogleBigQueryService via $method should bubble up")
          }
        }

        s"should bubble up BigQuery non-409 async exceptions in $method" in {
          // mock GoogleBigQueryService that throws error on createDataset
          val throwingBQService = mock[GoogleBigQueryService[IO]]
          when(throwingBQService.createDataset(any(), any(), any())).thenThrow(new BigQueryException(444, s"BigQuery non-409 errors via $method should bubble up"))
          // mock GoogleBigQueryServiceFactory that returns the mock GoogleBigQueryService
          val mockBQFactory = mock[GoogleBigQueryServiceFactory]
          when(mockBQFactory.getServiceForProject(any())).thenReturn(Resource.pure[IO, GoogleBigQueryService[IO]](throwingBQService))

          val deltaLayer = testDeltaLayer(mockBQFactory)

          val futureEx = recoverToExceptionIf[BigQueryException] {
            method match {
              case "createDataset" => deltaLayer.createDataset(constantData.workspace, userInfo)
              case "createDatasetIfNotExist" => deltaLayer.createDatasetIfNotExist(constantData.workspace, userInfo)
              case _ => fail(s"test encountered unexpected method '$method'")
            }
          }

          futureEx map { caught =>
            caught.getMessage shouldBe (s"BigQuery non-409 errors via $method should bubble up")
            caught.getCode shouldBe (444)
          }
        }
    
        s"should bubble up synchronous Sam exceptions in $method" in {
          // create a mock Sam dao that will throw an error
          val throwingSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
          when(throwingSamDAO.listPoliciesForResource(any(), any(), any())).thenThrow(new RuntimeException(s"Sam synchronous errors should bubble up in $method"))
          // create the Delta Layer instance (no need to spy), using the throwing SamDAO
          val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
          val deltaLayer = testDeltaLayer(bqFactory, samDAO = throwingSamDAO)
          // find the createDataset/createDatasetIfNotExist method we want to test, then invoke it
          val methodUnderTest = deltaLayer.getClass.getMethod(method, constantData.workspace.getClass, userInfo.getClass)
    
          // wait on futures until we see the bqCreate method being invoked
          eventually(Timeout(scaled(timeout)), Interval(scaled(interval))) {
            val caught = intercept[InvocationTargetException] {
              methodUnderTest.invoke(deltaLayer, constantData.workspace, userInfo)
            }
    
            Option(caught.getCause) should not be empty
            caught.getCause shouldBe a [RuntimeException]
            caught.getCause.getMessage shouldBe (s"Sam synchronous errors should bubble up in $method")
          }
        }

        s"should bubble up async Sam Future.failures in $method" in {
          // create a mock Sam dao that will throw an error
          val throwingSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
          when(throwingSamDAO.listPoliciesForResource(any(), any(), any())).thenReturn(Future.failed(new RuntimeException(s"Sam async errors should bubble up in $method")))
          // create the Delta Layer instance (no need to spy), using the throwing SamDAO
          val bqFactory = new MockBigQueryServiceFactory("credentialPath", blocker, Left(new RuntimeException))
          val deltaLayer = testDeltaLayer(bqFactory, samDAO = throwingSamDAO)

          val futureEx = recoverToExceptionIf[RuntimeException] {
            method match {
              case "createDataset" => deltaLayer.createDataset(constantData.workspace, userInfo)
              case "createDatasetIfNotExist" => deltaLayer.createDatasetIfNotExist(constantData.workspace, userInfo)
              case _ => fail(s"test encountered unexpected method '$method'")
            }
          }

          futureEx map { caught =>
            caught.getMessage shouldBe (s"Sam async errors should bubble up in $method")
          }
        }
      }
    }
  }
  // end tests common to both createDatasetIfNotExist and createDataset


  private def testDeltaLayer(bqServiceFactory: GoogleBigQueryServiceFactory,
                             deltaLayerWriter: DeltaLayerWriter = new MockDeltaLayerWriter,
                             samDAO: SamDAO = new MockSamDAO(slickDataSource),
                             clientEmail: WorkbenchEmail = WorkbenchEmail("unittest-clientEmail"),
                             deltaLayerStreamerEmail: WorkbenchEmail = WorkbenchEmail("unittest-detlaLayerStreamerEmail"))
                            (implicit executionContext: ExecutionContext, contextShift: ContextShift[IO]): DeltaLayer = {

    new DeltaLayer(bqServiceFactory, deltaLayerWriter, samDAO, clientEmail, deltaLayerStreamerEmail)
  }

}
