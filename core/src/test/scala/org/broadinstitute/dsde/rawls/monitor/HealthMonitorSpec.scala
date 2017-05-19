package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{MethodRepoDAO, MockGoogleServicesDAO, MockUserDirectoryDAO, UserDirectoryDAO}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Subsystems.{Agora, Database, Subsystem}
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

  // This configures how long the calls to `whenReady(Future)` will wait for the Future
  // before giving up and failing the test.
  // See: http://doc.scalatest.org/2.2.4/index.html#org.scalatest.concurrent.Futures
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "HealthMonitor" should "start with unknown status for all subsystems" in {
    checkCurrentStatus(newHealthMonitorActor(), false, unknowns = AllSubsystems)
  }

  it should "fetch ok status for all subsystems" in {
    checkCurrentStatus(newHealthMonitorActor(), true, successes = AllSubsystems)
  }

  it should "not return stale data" in {
    val actor = newHealthMonitorActor()
    actor ! CheckAll
    checkCurrentStatus(actor, true, successes = AllSubsystems)
    Thread.sleep(5000)
    checkCurrentStatus(actor, false, unknowns = AllSubsystems)
  }

  it should "handle exceptional futures" in {
    val actor = newHealthMonitorActor(exceptionalMethodRepoDAO)
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
    val actor = newHealthMonitorActor(timedOutMethodRepoDAO)
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

  it should "fetch a non-ok for Agora" in {
    val actor = newHealthMonitorActor(failingMethodRepoDAO)
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

  it should "fetch a non-ok for Cromwell" ignore {
    // Stubbed; Cromwell doesn't have a health check yet
  }

  it should "fetch a non-ok for DB" in {
    slickDataSource.database.close()
    val actor = newHealthMonitorActor()
    actor ! CheckAll
    checkCurrentStatus(actor, false,
      successes = AllSubsystems.filterNot(_ == Database),
      failures = Set(Database),
      errorMessages = {
        case (Database, messages) =>
          messages.size should be(1)
          println(s"Messages are $messages")
          messages(0) should not be 'empty
      })
  }

  def constPf[A]: PartialFunction[A, Unit] = { case _ => () }
  def checkCurrentStatus(actor: ActorRef,
                         overall: Boolean,
                         successes: Set[Subsystem] = Set.empty,
                         failures: Set[Subsystem] = Set.empty,
                         unknowns: Set[Subsystem] = Set.empty,
                         errorMessages: PartialFunction[(Subsystem, Seq[String]), Unit] = constPf): Unit = {
    eventually {
      whenReady(actor ? GetCurrentStatus) { resp =>
        //resp shouldBe a[GetCurrentStatusResponse]
        val GetCurrentStatusResponse(actual) = resp
        actual.ok should equal(overall)
        actual.systems.filter(_._2.ok).keySet should equal(successes)
        actual.systems.filterNot(_._2.ok).filterNot(_._2 == UnknownStatus).keySet should equal(failures)
        actual.systems.filterNot(_._2.ok).filter(_._2 == UnknownStatus).keySet should equal(unknowns)
        actual.systems.foreach { case (sub, SubsystemStatus(_, messages)) =>
          errorMessages.orElse(constPf).apply((sub, messages))
        }
      }
    }
  }


  def newHealthMonitorActor(methodRepoDAO: => MethodRepoDAO = mockMethodRepoDAO): ActorRef = {
    system.actorOf(HealthMonitor.props(slickDataSource, new MockGoogleServicesDAO("grp"), new MockGooglePubSubDAO, mockUserDirectoryDAO, methodRepoDAO,
      Seq.empty, Seq.empty, Seq.empty, 1 second, 3 seconds))
  }

  def mockUserDirectoryDAO: UserDirectoryDAO = {
    val mockUserDirectoryDAO = new MockUserDirectoryDAO
    mockUserDirectoryDAO.createUser(new RawlsUserSubjectId("me"))
    mockUserDirectoryDAO
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

  def FailedStatus = SubsystemStatus(false, Seq.empty)
}
