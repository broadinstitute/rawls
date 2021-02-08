package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.{ContextShift, IO}
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor.{HandleBacklog, ScheduleDelayedSweep, Sweep}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps

class EntityStatisticsCacheMonitorSpec(_system: ActorSystem) extends TestKit(_system) with MockitoSugar with AnyFlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually with ScalaFutures {
  implicit val globalCs = IO.contextShift(global)
  val defaultExecutionContext: ExecutionContext = executionContext

  def this() = this(ActorSystem("EntityStatisticsCacheMonitorSpec"))

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }


  "EntityStatisticsCacheMonitor" should "schedule a delayed sweep after handling the backlog" in withLocalEntityProviderTestDatabase { slickDataSource: SlickDataSource =>
    val monitor = new EntityStatisticsCacheMonitor {
      override val dataSource: SlickDataSource = slickDataSource
      override val limit: Int = 1
      override implicit val executionContext: ExecutionContext = defaultExecutionContext
      override implicit val cs: ContextShift[IO] = globalCs
    }

    assertResult(ScheduleDelayedSweep) {
      Await.result(monitor.handleBacklog(), Duration.Inf)
    }
  }

  it should "handle the backlog if the last sweep was under capacity" in withLocalEntityProviderTestDatabase { slickDataSource: SlickDataSource =>
    val monitor = new EntityStatisticsCacheMonitor {
      override val dataSource: SlickDataSource = slickDataSource
      override val limit: Int = 10000 //choose a number that will force sweep to handle the backlog next
      override implicit val executionContext: ExecutionContext = defaultExecutionContext
      override implicit val cs: ContextShift[IO] = globalCs
    }

    assertResult(HandleBacklog) {
      Await.result(monitor.sweep(), Duration.Inf)
    }
  }

  it should "continue to sweep if the last sweep reached the limit" in withLocalEntityProviderTestDatabase { slickDataSource: SlickDataSource =>
    val monitor = new EntityStatisticsCacheMonitor {
      override val dataSource: SlickDataSource = slickDataSource
      override val limit: Int = 0 //choose a number that will force sweep to do another sweep
      override implicit val executionContext: ExecutionContext = defaultExecutionContext
      override implicit val cs: ContextShift[IO] = globalCs
    }

    assertResult(Sweep) {
      Await.result(monitor.sweep(), Duration.Inf)
    }
  }

}
