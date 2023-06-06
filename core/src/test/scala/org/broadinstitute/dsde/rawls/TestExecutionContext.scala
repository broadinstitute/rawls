package org.broadinstitute.dsde.rawls

import akka.dispatch.{DispatcherPrerequisites, ExecutorServiceConfigurator, ExecutorServiceFactory}
import com.typesafe.config.Config

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContextExecutor

/**
 * Created by dvoet on 10/9/15.
 */
object TestExecutionContext {
  implicit val testExecutionContext = new TestExecutionContext()
}
class TestExecutionContext() extends ExecutionContextExecutor {
  val pool = Executors.newCachedThreadPool()
  val count = new AtomicInteger(0)
  override def execute(runnable: Runnable): Unit =
    pool.execute(runnable)

  override def reportFailure(cause: Throwable): Unit =
    cause.printStackTrace()
}

class TestExecutorServiceConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
    extends ExecutorServiceConfigurator(config, prerequisites) {
  override def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory =
    return new ExecutorServiceFactory {
      override def createExecutorService: ExecutorService = new AbstractExecutorService {
        var terminated: Boolean = false

        override def shutdown(): Unit = terminated = true

        override def isTerminated: Boolean = terminated

        override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = !terminated

        override def shutdownNow(): java.util.List[Runnable] = new java.util.ArrayList[Runnable]()

        override def isShutdown: Boolean = terminated

        override def execute(command: Runnable): Unit = TestExecutionContext.testExecutionContext.execute(command)

      }
    }
}
