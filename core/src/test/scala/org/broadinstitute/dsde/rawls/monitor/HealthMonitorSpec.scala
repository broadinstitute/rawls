package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import bio.terra.profile.model.{SystemStatus, SystemStatusSystems}
import bio.terra.workspace.client.ApiException
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.google.{GooglePubSubDAO, MockGooglePubSubDAO}
import org.broadinstitute.dsde.rawls.model.Subsystems._
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.jdk.CollectionConverters._

/**
  * Created by rtitle on 5/19/17.
  */
class HealthMonitorSpec
    extends TestKit(ActorSystem("system"))
    with ScalaFutures
    with Eventually
    with AnyFlatSpecLike
    with MockitoSugar
    with Matchers
    with TestDriverComponent
    with BeforeAndAfterAll {

  // actor ask timeout
  implicit val timeout = Timeout(5 seconds)

  // This configures how long the calls to `whenReady(Future)` and `eventually` will wait
  // before giving up and failing the test.
  // See: http://doc.scalatest.org/2.2.4/index.html#org.scalatest.concurrent.Futures
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  // Cromwell reports the status of its subsystems

  val execSubsystems = Seq("DockerHub", "Engine Database", "PAPI", "GCS")
  val okExecSubsytems: Map[String, SubsystemStatus] = (execSubsystems map { _ -> HealthMonitor.OkStatus }).toMap

  val sadExecSubsystems: Map[String, SubsystemStatus] = (execSubsystems map { sub =>
    sub -> SubsystemStatus(false, Option(List(s"""{"$sub": "is unhappy"}""")))
  }).toMap

  "HealthMonitor" should "start with unknown status for all subsystems" in {
    checkCurrentStatus(newHealthMonitorActor(), false, unknowns = AllSubsystems)
  }

  it should "return ok status for all subsystems" in {
    val actor = newHealthMonitorActor()
    actor ! CheckAll
    checkCurrentStatus(actor, true, successes = AllSubsystems)
  }

  it should "not return stale data" in {
    val actor = newHealthMonitorActor()
    actor ! CheckAll
    checkCurrentStatus(actor, true, successes = AllSubsystems)
    // after 3 second stale threshold
    val afterStaleMillis = 5000
    Thread.sleep(afterStaleMillis)
    checkCurrentStatus(actor, false, unknowns = AllSubsystems)
  }

  it should "handle exceptional futures" in {
    val actor = newHealthMonitorActor(methodRepoDAO = exceptionalMethodRepoDAO)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == Agora),
      failures = Set(Agora),
      errorMessages = { case (Agora, Some(messages)) =>
        messages.size should be(1)
        messages(0) should equal("test exception")
      }
    )
  }

  it should "handle timed out futures" in {
    val actor = newHealthMonitorActor(methodRepoDAO = timedOutMethodRepoDAO)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == Agora),
      failures = Set(Agora),
      errorMessages = { case (Agora, Some(messages)) =>
        messages.size should be(1)
        messages(0) should startWith("Timed out")
      }
    )
  }

  // Individual negative tests for each subsystem

  it should "return a non-ok for Agora" in {
    val actor = newHealthMonitorActor(methodRepoDAO = failingMethodRepoDAO)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == Agora),
      failures = Set(Agora),
      errorMessages = { case (Agora, Some(messages)) =>
        messages.size should be(1)
        messages(0) should equal("agora failed")
      }
    )
  }

  it should "return a non-ok for Sam" in {
    val actor = newHealthMonitorActor(samDAO = failingSamDAO)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == Sam),
      failures = Set(Sam),
      errorMessages = { case (Sam, Some(messages)) =>
        messages.size should be(1)
        messages(0) should equal("""{"some": "json"}""")
      }
    )
  }

  it should "return a non-ok for BillingProfileManager" in {
    val actor = newHealthMonitorActor(billingProfileManagerDAO = failingBillingProfileManagerDAO)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == BillingProfileManager),
      failures = Set(BillingProfileManager),
      errorMessages = { case (BillingProfileManager, Some(messages)) =>
        messages.size should be(1)
      }
    )
  }

  it should "return a non-ok for WorkspaceManager" in {
    val actor = newHealthMonitorActor(workspaceManagerDAO = failingWorkspaceManagerDAO)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == WorkspaceManager),
      failures = Set(WorkspaceManager),
      errorMessages = { case (WorkspaceManager, Some(messages)) =>
        messages.size should be(1)
      }
    )
  }

  it should "return a non-ok for Cromwell" in {
    val expectedMessages = sadExecSubsystems.keys map { sub =>
      s"""sadCrom-$sub: {"$sub": "is unhappy"}"""
    }

    val actor = newHealthMonitorActor(executionServiceServers = failingExecutionServiceServers)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == Cromwell),
      failures = Set(Cromwell),
      errorMessages = { case (Cromwell, Some(messages)) =>
        messages.size should be(4)
        messages should contain theSameElementsAs expectedMessages
      }
    )
  }

  it should "return a non-ok for Google Billing" in {
    val actor = newHealthMonitorActor(mockGoogleServicesDAO_noBillingAccts)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == GoogleBilling),
      failures = Set(GoogleBilling),
      errorMessages = { case (GoogleBilling, Some(messages)) =>
        messages.size should be(1)
        messages(0) should startWith("Could not find")
      }
    )
  }

  it should "return a non-ok for Google Buckets" in {
    val actor = newHealthMonitorActor(googleServicesDAO = mockGoogleServicesDAO_noBuckets)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == GoogleBuckets),
      failures = Set(GoogleBuckets),
      errorMessages = { case (GoogleBuckets, Some(messages)) =>
        messages.size should be(2)
        messages(0) should be("Could not find bucket: bucket1. No buckets in this mock")
        messages(1) should be("Could not find bucket: bucket2. No buckets in this mock")
      }
    )
  }

  it should "return a non-ok for Google Groups" in {
    val actor = newHealthMonitorActor(googleServicesDAO = mockGoogleServicesDAO_noGroups)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == GoogleGroups),
      failures = Set(GoogleGroups),
      errorMessages = { case (GoogleGroups, Some(messages)) =>
        messages.size should be(2)
        messages.foreach(_ should startWith("Could not find"))
        messages(0) should endWith("group1")
        messages(1) should endWith("group2")
      }
    )
  }

  it should "return a non-ok for Google PubSub" in {
    val actor = newHealthMonitorActor(googlePubSubDAO = mockGooglePubSubDAO_noTopics)
    actor ! CheckAll
    checkCurrentStatus(
      actor,
      false,
      successes = AllSubsystems.filterNot(_ == GooglePubSub),
      failures = Set(GooglePubSub),
      errorMessages = { case (GooglePubSub, Some(messages)) =>
        messages.size should be(2)
        messages.foreach(_ should startWith("Could not find"))
        messages(0) should endWith("topic1")
        messages(1) should endWith("topic2")
      }
    )
  }

  def checkCurrentStatus(actor: ActorRef,
                         overall: Boolean,
                         successes: Set[Subsystem] = Set.empty,
                         failures: Set[Subsystem] = Set.empty,
                         unknowns: Set[Subsystem] = Set.empty,
                         errorMessages: PartialFunction[(Subsystem, Option[List[String]]), Unit] = PartialFunction.empty
  ): Unit = {
    var actual: StatusCheckResponse = null

    eventually {
      whenReady(actor ? GetCurrentStatus) { resp =>
        resp shouldBe a[StatusCheckResponse]
        actual = resp.asInstanceOf[StatusCheckResponse]
        actual.ok should equal(overall)
        actual.systems.filter(_._2.ok).keySet should equal(successes)
        actual.systems.filterNot(_._2.ok).filterNot(_._2 == UnknownStatus).keySet should equal(failures)
        actual.systems.filterNot(_._2.ok).filter(_._2 == UnknownStatus).keySet should equal(unknowns)
      }
    }

    actual.systems.foreach { case (sub, SubsystemStatus(_, messages)) =>
      val fallback: PartialFunction[(Subsystem, Option[List[String]]), Unit] = {
        case (s, None) =>
          successes should contain(s)
        case (s, Some(messages)) =>
          successes should not contain s
          messages should not be empty
          messages.size should be(1)
          if (unknowns.contains(s)) {
            messages should equal(UnknownStatus.messages.get)
          } else {
            messages(0) should not be empty
          }
      }
      errorMessages.orElse(fallback).apply((sub, messages))
    }
  }

  // health monitor subsystem wait time
  val futureTimeout: FiniteDuration = 1 second

  // after this period, subsystem status revert to Unknown
  val staleThreshold: FiniteDuration = 3 seconds

  def newHealthMonitorActor(googleServicesDAO: => GoogleServicesDAO = mockGoogleServicesDAO,
                            googlePubSubDAO: => GooglePubSubDAO = mockGooglePubSubDAO,
                            methodRepoDAO: => MethodRepoDAO = mockMethodRepoDAO,
                            samDAO: SamDAO = mockSamDAO,
                            billingProfileManagerDAO: BillingProfileManagerDAO = mockBillingProfileManagerDAO,
                            workspaceManagerDAO: WorkspaceManagerDAO = mockWorkspaceManagerDAO,
                            executionServiceServers: Map[ExecutionServiceId, ExecutionServiceDAO] =
                              mockExecutionServiceServers
  ): ActorRef =
    system.actorOf(
      HealthMonitor.props(
        slickDataSource,
        googleServicesDAO,
        googlePubSubDAO,
        methodRepoDAO,
        samDAO,
        billingProfileManagerDAO,
        workspaceManagerDAO,
        executionServiceServers,
        Seq("group1", "group2"),
        Seq("topic1", "topic2"),
        Seq("bucket1", "bucket2"),
        futureTimeout,
        staleThreshold
      )
    )

  def mockGoogleServicesDAO: GoogleServicesDAO = new MockGoogleServicesDAO("group")

  def mockGoogleServicesDAO_noBillingAccts: GoogleServicesDAO = {
    val dao = spy(mockGoogleServicesDAO)
    when {
      dao.listBillingAccountsUsingServiceCredential(any[ExecutionContext])
    } thenReturn Future.successful(Seq.empty)
    dao
  }

  def mockGoogleServicesDAO_noBuckets: GoogleServicesDAO = {
    val dao = spy(mockGoogleServicesDAO)
    when {
      dao.getBucket(anyString, any[Option[GoogleProjectId]])(any[ExecutionContext])
    } thenReturn Future.successful(Left("No buckets in this mock"))
    dao
  }

  def mockGoogleServicesDAO_noGroups: GoogleServicesDAO = {
    val dao = spy(mockGoogleServicesDAO)
    when {
      dao.getGoogleGroup(anyString)(any[ExecutionContext])
    } thenReturn Future.successful(None)
    dao
  }

  def mockGooglePubSubDAO: GooglePubSubDAO = {
    val dao = new MockGooglePubSubDAO
    dao.createTopic("topic1")
    dao.createTopic("topic2")
    dao
  }

  def mockGooglePubSubDAO_noTopics: GooglePubSubDAO =
    new MockGooglePubSubDAO

  def mockMethodRepoDAO: MethodRepoDAO = {
    val dao = mock[MethodRepoDAO](RETURNS_SMART_NULLS)
    when {
      dao.getStatus(any[ExecutionContext])
    } thenReturn Future.successful(HealthMonitor.OkStatus)
    dao
  }

  def exceptionalMethodRepoDAO: MethodRepoDAO = {
    val dao = mock[MethodRepoDAO](RETURNS_SMART_NULLS)
    when {
      dao.getStatus(any[ExecutionContext])
    } thenReturn Future.failed(new Exception("test exception"))
    dao
  }

  def timedOutMethodRepoDAO: MethodRepoDAO = {
    val dao = mock[MethodRepoDAO](RETURNS_SMART_NULLS)
    when {
      dao.getStatus(any[ExecutionContext])
    } thenReturn Future {
      Thread.sleep((1 minute).toMillis)
      HealthMonitor.OkStatus
    }
    dao
  }

  def failingMethodRepoDAO: MethodRepoDAO = {
    val dao = mock[MethodRepoDAO](RETURNS_SMART_NULLS)
    when {
      dao.getStatus(any[ExecutionContext])
    } thenReturn Future.successful(SubsystemStatus(false, Some(List("agora failed"))))
    dao
  }

  def mockSamDAO: SamDAO = {
    val dao = mock[SamDAO](RETURNS_SMART_NULLS)
    when {
      dao.getStatus()
    } thenReturn Future.successful(HealthMonitor.OkStatus)
    dao
  }

  def failingSamDAO: SamDAO = {
    val dao = mock[SamDAO](RETURNS_SMART_NULLS)
    when {
      dao.getStatus()
    } thenReturn Future.successful(SubsystemStatus(false, Option(List("""{"some": "json"}"""))))
    dao
  }

  def mockBillingProfileManagerDAO: BillingProfileManagerDAO = {
    val dao = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when {
      dao.getStatus()
    } thenReturn new SystemStatus().ok(true)
    dao
  }

  def failingBillingProfileManagerDAO: BillingProfileManagerDAO = {
    val failingSubsystems = Map(
      "exampleSystem" -> new SystemStatusSystems().ok(false).messages(List("messages").asJava)
    ).asJava
    val dao = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when {
      dao.getStatus()
    } thenReturn new SystemStatus().ok(false).systems(failingSubsystems)
    dao
  }

  def mockWorkspaceManagerDAO: WorkspaceManagerDAO = {
    val dao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    doNothing.when(dao).throwWhenUnavailable()
    dao
  }

  def failingWorkspaceManagerDAO: WorkspaceManagerDAO = {
    val dao = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS)
    when(dao.throwWhenUnavailable()).thenThrow(new ApiException())
    dao
  }

  def mockExecutionServiceServers: Map[ExecutionServiceId, ExecutionServiceDAO] = {
    val dao = mock[ExecutionServiceDAO](RETURNS_SMART_NULLS)
    when {
      dao.getStatus()
    } thenReturn Future.successful(okExecSubsytems)

    Map(ExecutionServiceId("crom1") -> dao, ExecutionServiceId("crom2") -> dao)
  }

  def failingExecutionServiceServers: Map[ExecutionServiceId, ExecutionServiceDAO] = {
    val happyDao = mock[ExecutionServiceDAO](RETURNS_SMART_NULLS)
    when {
      happyDao.getStatus()
    } thenReturn Future.successful(okExecSubsytems)

    val sadDao = mock[ExecutionServiceDAO](RETURNS_SMART_NULLS)
    when {
      sadDao.getStatus()
    } thenReturn Future.successful(sadExecSubsystems)

    Map(ExecutionServiceId("happyCrom") -> happyDao, ExecutionServiceId("sadCrom") -> sadDao)
  }

}
