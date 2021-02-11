package org.broadinstitute.dsde.rawls.monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.{ContextShift, IO}
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor.{ScheduleDelayedSweep, Sweep}
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
  val defaultExecutionContext: ExecutionContext = executionContext

  def this() = this(ActorSystem("EntityStatisticsCacheMonitorSpec"))

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }


  "EntityStatisticsCacheMonitor" should "schedule a delayed sweep if the previous sweep was empty" in withLocalEntityProviderTestDatabase { slickDataSource: SlickDataSource =>
    val monitor = new EntityStatisticsCacheMonitor {
      override val dataSource: SlickDataSource = slickDataSource
      override implicit val executionContext: ExecutionContext = defaultExecutionContext
    }

    assertResult(ScheduleDelayedSweep) {
      Await.result(monitor.sweep(), Duration.Inf)
    }
  }

  it should "continue to immediately sweep if the last sweep was not empty" in withLocalEntityProviderTestDatabase { slickDataSource: SlickDataSource =>
    val monitor = new EntityStatisticsCacheMonitor {
      override val dataSource: SlickDataSource = slickDataSource
      override implicit val executionContext: ExecutionContext = defaultExecutionContext
    }

    assertResult(Sweep) {
      Await.result(monitor.sweep(), Duration.Inf)
    }
  }

}
