package org.broadinstitute.dsde.rawls.monitor

import java.util
import java.util.Collections

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpecLike}

import scala.concurrent.Await
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
      new UserService(userInfo, null, null, null, null, null) {
        override def receive = {
          case UserService.InternalSynchronizeGroupMembers(rawlsGroupRef) =>
            syncedGroups.add(rawlsGroupRef)
            sender() ! SyncReport(RawlsGroupEmail("foo@bar.com"), Seq.empty)
          case x => throw new RawlsException(s"test user service does not handle this message: $x")
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
      new UserService(userInfo, null, null, null, null, null) {
        override def receive = {
          case _ => throw new RawlsException("I am a failure")
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
}
