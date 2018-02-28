package org.broadinstitute.dsde.rawls.monitor

import java.util
import java.util.Collections

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpecLike}
import akka.http.scaladsl.model.StatusCodes

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import spray.json._
import UserModelJsonSupport._
import scala.collection.convert.decorateAsScala._

/**
 * Created by dvoet on 12/12/16.
 */
class GoogleGroupSyncMonitorSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll {
  def this() = this(ActorSystem("GoogleGroupSyncMonitorSpec"))

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "GoogleGroupSyncMonitor" should "sync google groups" in {
    val pubsubDao = new MockGooglePubSubDAO
    val topic = "topic"

    val syncedGroups = Collections.synchronizedSet(new util.HashSet[RawlsGroupRef]()).asScala

    val userServiceConstructor = (userInfo: UserInfo) => {
      new UserService(userInfo, null, null, null, null, null, null, null) {
        override def InternalSynchronizeGroupMembers(rawlsGroupRef: RawlsGroupRef) = {
          syncedGroups.add(rawlsGroupRef)
          Future.successful(SyncReport(RawlsGroupEmail("foo@bar.com"), Seq.empty))
        }
      }
    }

    val workerCount = 10
    system.actorOf(GoogleGroupSyncMonitorSupervisor.props(10 milliseconds, 0 milliseconds, pubsubDao, topic, "subscription", workerCount, userServiceConstructor))

    // GoogleGroupSyncMonitorSupervisor creates the topic, need to wait for it to exist before publishing messages
    awaitCond(pubsubDao.topics.contains(topic), 10 seconds)
    val testGroups = (for (i <- 0 until workerCount * 4) yield RawlsGroupRef(RawlsGroupName(s"testgroup_$i")))

    // wait for all the messages to be published and throw an error if one happens (i.e. use Await.result not Await.ready)
    Await.result(pubsubDao.publishMessages(topic, testGroups.map(_.toJson.compactPrint)), Duration.Inf)

    awaitAssert(assertResult(testGroups.toSet) { syncedGroups }, 10 seconds)
    awaitAssert(assertResult(testGroups.size) { pubsubDao.acks.size() }, 10 seconds)
  }

  it should "handle failures syncing google groups" in {
    val pubsubDao = new MockGooglePubSubDAO
    val topic = "topic"

    val userServiceConstructor = (userInfo: UserInfo) => {
      new UserService(userInfo, null, null, null, null, null, null, null) {
        override def InternalSynchronizeGroupMembers(rawlsGroupRef: RawlsGroupRef) = {
          Future.failed(new RawlsException("I am a failure"))
        }
      }
    }

    val workerCount = 10
    system.actorOf(GoogleGroupSyncMonitorSupervisor.props(100 milliseconds, 0 milliseconds, pubsubDao, topic, "subscription", workerCount, userServiceConstructor))

    // GoogleGroupSyncMonitorSupervisor creates the topic, need to wait for it to exist before publishing messages
    awaitCond(pubsubDao.topics.contains(topic), 10 seconds)

    val testGroups = (for(i <- 0 until workerCount*4) yield RawlsGroupRef(RawlsGroupName(s"testgroup_$i")))
    Await.result(pubsubDao.publishMessages(topic, testGroups.map(_.toJson.compactPrint)), Duration.Inf)

    awaitAssert(assert(pubsubDao.subscriptionsByName("subscription").queue.isEmpty), 10 seconds)
    assertResult(0) {
      pubsubDao.acks.size()
    }
  }

  it should "handle other failures syncing google groups" in {
    val pubsubDao = new MockGooglePubSubDAO
    val topic = "topic"

    val userServiceConstructor = (userInfo: UserInfo) => {
      new UserService(userInfo, null, null, null, null, null, null, null) {
        override def InternalSynchronizeGroupMembers(rawlsGroupRef: RawlsGroupRef) = {
          Future.successful(SyncReport(RawlsGroupEmail("foo@bar.com"), Seq(SyncReportItem("op", "email", Option(ErrorReport("I am another failure"))))))
        }
      }
    }

    val workerCount = 10
    system.actorOf(GoogleGroupSyncMonitorSupervisor.props(100 milliseconds, 0 milliseconds, pubsubDao, topic, "subscription", workerCount, userServiceConstructor))

    // GoogleGroupSyncMonitorSupervisor creates the topic, need to wait for it to exist before publishing messages
    awaitCond(pubsubDao.topics.contains(topic), 10 seconds)

    val testGroups = (for(i <- 0 until workerCount*4) yield RawlsGroupRef(RawlsGroupName(s"testgroup_$i")))
    Await.result(pubsubDao.publishMessages(topic, testGroups.map(_.toJson.compactPrint)), Duration.Inf)

    awaitAssert(assert(pubsubDao.subscriptionsByName("subscription").queue.isEmpty), 10 seconds)
    assertResult(0) {
      pubsubDao.acks.size()
    }
  }

  it should "handle group not found syncing google groups" in {
    val pubsubDao = new MockGooglePubSubDAO
    val topic = "topic"

    import akka.pattern._

    val userServiceConstructor = (userInfo: UserInfo) => {
      new UserService(userInfo, null, null, null, null, null, null, null) {
        override def InternalSynchronizeGroupMembers(rawlsGroupRef: RawlsGroupRef) = {
          Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "group not found")))
        }
      }
    }

    val workerCount = 10
    system.actorOf(GoogleGroupSyncMonitorSupervisor.props(100 milliseconds, 0 milliseconds, pubsubDao, topic, "subscription", workerCount, userServiceConstructor))

    // GoogleGroupSyncMonitorSupervisor creates the topic, need to wait for it to exist before publishing messages
    awaitCond(pubsubDao.topics.contains(topic), 10 seconds)

    val testGroups = (for(i <- 0 until workerCount*4) yield RawlsGroupRef(RawlsGroupName(s"testgroup_$i")))
    Await.result(pubsubDao.publishMessages(topic, testGroups.map(_.toJson.compactPrint)), Duration.Inf)

    awaitAssert(assert(pubsubDao.subscriptionsByName("subscription").queue.isEmpty), 10 seconds)
    assertResult(testGroups.size) {
      pubsubDao.acks.size()
    }
  }
}
