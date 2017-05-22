package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.google.{GooglePubSubDAO, MockGooglePubSubDAO}
import org.broadinstitute.dsde.rawls.model.Subsystems._
import org.broadinstitute.dsde.rawls.model.{AgoraStatus, RawlsUserSubjectId, StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 5/19/17.
  */
class HealthMonitorSpec extends TestKit(ActorSystem("system")) with ScalaFutures with Eventually with FlatSpecLike with MockitoSugar with Matchers with TestDriverComponent with BeforeAndAfterAll {
  implicit val timeout = Timeout(5 seconds)

  // This configures how long the calls to `whenReady(Future)` and `eventually` will wait
  // before giving up and failing the test.
  // See: http://doc.scalatest.org/2.2.4/index.html#org.scalatest.concurrent.Futures
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

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
    // The stale threshold is 3 seconds
    Thread.sleep(5000)
    checkCurrentStatus(actor, false, unknowns = AllSubsystems)
  }

  it should "handle exceptional futures" in {
    val actor = newHealthMonitorActor(methodRepoDAO = exceptionalMethodRepoDAO)
    actor ! CheckAll
    checkCurrentStatus(actor, false,
      successes = AllSubsystems.filterNot(_ == Agora),
      failures = Set(Agora),
      errorMessages = {
        case (Agora, messages) =>
          messages.size should be(1)
          messages(0) should equal("test exception")
      })
  }

  it should "handle timed out futures" in {
    val actor = newHealthMonitorActor(methodRepoDAO = timedOutMethodRepoDAO)
    actor ! CheckAll
    checkCurrentStatus(actor, false,
      successes = AllSubsystems.filterNot(_ == Agora),
      failures = Set(Agora),
      errorMessages = {
        case (Agora, messages) =>
          messages.size should be(1)
          messages(0) should startWith ("Timed out")
      })
  }


  // Individual negative tests for each subsystem

  it should "return a non-ok for Agora" in {
    val actor = newHealthMonitorActor(methodRepoDAO = failingMethodRepoDAO)
    actor ! CheckAll
    checkCurrentStatus(actor, false,
      successes = AllSubsystems.filterNot(_ == Agora),
      failures = Set(Agora),
      errorMessages = {
        case (Agora, messages) =>
          messages.size should be(1)
          messages(0) should equal("agora failed")
      })
  }

  it should "return a non-ok for Cromwell" ignore {
    // Stubbed; Cromwell doesn't have a health check yet
  }

  it should "return a non-ok for Google Billing" in {
    val actor = newHealthMonitorActor(mockGoogleServicesDAO_noBillingAccts)
    actor ! CheckAll
    checkCurrentStatus(actor, false,
      successes = AllSubsystems.filterNot(_ == GoogleBilling),
      failures = Set(GoogleBilling),
      errorMessages = {
        case (GoogleBilling, messages) =>
          messages.size should be (1)
          messages(0) should startWith ("Could not find")
      })
  }

  it should "return a non-ok for Google Buckets" in {
    val actor = newHealthMonitorActor(googleServicesDAO = mockGoogleServicesDAO_noBuckets)
    actor ! CheckAll
    checkCurrentStatus(actor, false,
      successes = AllSubsystems.filterNot(_ == GoogleBuckets),
      failures = Set(GoogleBuckets),
      errorMessages = {
        case (GoogleBuckets, messages) =>
          messages.size should be (2)
          messages.foreach(_ should startWith ("Could not find"))
          messages(0) should endWith ("bucket1")
          messages(1) should endWith ("bucket2")
      })
  }

  it should "return a non-ok for Google Groups" in {
    val actor = newHealthMonitorActor(googleServicesDAO = mockGoogleServicesDAO_noGroups)
    actor ! CheckAll
    checkCurrentStatus(actor, false,
      successes = AllSubsystems.filterNot(_ == GoogleGroups),
      failures = Set(GoogleGroups),
      errorMessages = {
        case (GoogleGroups, messages) =>
          messages.size should be (2)
          messages.foreach(_ should startWith ("Could not find"))
          messages(0) should endWith ("group1")
          messages(1) should endWith ("group2")
      })
  }

  it should "return a non-ok for Google PubSub" in {
    val actor = newHealthMonitorActor(googlePubSubDAO = mockGooglePubSubDAO_noTopics)
    actor ! CheckAll
    checkCurrentStatus(actor, false,
      successes = AllSubsystems.filterNot(_ == GooglePubSub),
      failures = Set(GooglePubSub),
      errorMessages = {
        case (GooglePubSub, messages) =>
          messages.size should be (2)
          messages.foreach(_ should startWith ("Could not find"))
          messages(0) should endWith ("topic1")
          messages(1) should endWith ("topic2")
      })
  }

  it should "return a non-ok for LDAP" in {
    val actor = newHealthMonitorActor(userDirectoryDAO = mockUserDirectoryDAO_noUsers)
    actor ! CheckAll
    checkCurrentStatus(actor, false,
      successes = AllSubsystems.filterNot(_ == LDAP),
      failures = Set(LDAP),
      errorMessages = {
        case (LDAP, messages) =>
          messages.size should be (1)
          messages(0) should be ("Could not find any users in LDAP")
      })
  }

  def checkCurrentStatus(actor: ActorRef,
                         overall: Boolean,
                         successes: Set[Subsystem] = Set.empty,
                         failures: Set[Subsystem] = Set.empty,
                         unknowns: Set[Subsystem] = Set.empty,
                         errorMessages: PartialFunction[(Subsystem, Seq[String]), Unit] = PartialFunction.empty): Unit = {
    eventually {
      whenReady(actor ? GetCurrentStatus) { resp =>
        resp shouldBe a[StatusCheckResponse]
        val actual = resp.asInstanceOf[StatusCheckResponse]
        actual.ok should equal(overall)
        actual.systems.filter(_._2.ok).keySet should equal(successes)
        actual.systems.filterNot(_._2.ok).filterNot(_._2 == UnknownStatus).keySet should equal(failures)
        actual.systems.filterNot(_._2.ok).filter(_._2 == UnknownStatus).keySet should equal(unknowns)
        actual.systems.foreach { case (sub, SubsystemStatus(_, messages)) =>
          val fallback: PartialFunction[(Subsystem, Seq[String]), Unit] = {
            case (s, messages) =>
              if (successes.contains(s)) {
                messages shouldBe empty
              } else if (unknowns.contains(s)) {
                messages.size should be(1)
                messages(0) should equal("Unknown")
              } else {
                messages.size should be(1)
                messages(0) should not be empty
              }
          }
          errorMessages.orElse(fallback).apply((sub, messages))
        }
      }
    }
  }

  def newHealthMonitorActor(googleServicesDAO: => GoogleServicesDAO = mockGoogleServicesDAO, googlePubSubDAO: => GooglePubSubDAO = mockGooglePubSubDAO,
                            methodRepoDAO: => MethodRepoDAO = mockMethodRepoDAO, userDirectoryDAO: => UserDirectoryDAO = mockUserDirectoryDAO): ActorRef = {
    system.actorOf(HealthMonitor.props(slickDataSource, googleServicesDAO, googlePubSubDAO, userDirectoryDAO, methodRepoDAO,
      Seq("group1", "group2"), Seq("topic1", "topic2"), Seq("bucket1", "bucket2"), 1 second, 3 seconds))
  }

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
      dao.getBucket(anyString)(any[ExecutionContext])
    } thenReturn Future.successful(None)
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

  def mockGooglePubSubDAO_noTopics: GooglePubSubDAO = {
    new MockGooglePubSubDAO
  }

  def mockUserDirectoryDAO: UserDirectoryDAO = {
    val mockUserDirectoryDAO = new MockUserDirectoryDAO
    mockUserDirectoryDAO.createUser(new RawlsUserSubjectId("me"))
    mockUserDirectoryDAO
  }

  def mockUserDirectoryDAO_noUsers: UserDirectoryDAO = {
    new MockUserDirectoryDAO
  }

  def mockMethodRepoDAO: MethodRepoDAO = {
    val dao = mock[MethodRepoDAO]
    when {
      dao.getStatus(any[ExecutionContext])
    } thenReturn Future.successful(AgoraStatus(true, Seq.empty))
    dao
  }

  def exceptionalMethodRepoDAO: MethodRepoDAO = {
    val dao = mock[MethodRepoDAO]
    when {
      dao.getStatus(any[ExecutionContext])
    } thenReturn Future.failed(new Exception("test exception"))
    dao
  }

  def timedOutMethodRepoDAO: MethodRepoDAO = {
    val dao = mock[MethodRepoDAO]
    when {
      dao.getStatus(any[ExecutionContext])
    } thenReturn Future {
      Thread.sleep((1 minute).toMillis)
      AgoraStatus(true, Seq.empty)
    }
    dao
  }

  def failingMethodRepoDAO: MethodRepoDAO = {
    val dao = mock[MethodRepoDAO]
    when {
      dao.getStatus(any[ExecutionContext])
    } thenReturn Future.successful(AgoraStatus(false, Seq("agora failed")))
    dao
  }
}
