package org.broadinstitute.dsde.rawls.util

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

import akka.dispatch.{DispatcherPrerequisites, ExecutorServiceConfigurator, ExecutorServiceFactory}
import com.typesafe.config.Config

class CachedThreadPoolExecutorConfigurator(config: Config, prerequisites: DispatcherPrerequisites) extends ExecutorServiceConfigurator(config, prerequisites) {

  private class CachedThreadPoolExecutorServiceFactory(threadFactory: ThreadFactory) extends ExecutorServiceFactory {
    def createExecutorService(): ExecutorService = {
      Executors.newCachedThreadPool(threadFactory)
    }
  }

  def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory = {
    new CachedThreadPoolExecutorServiceFactory(threadFactory)
  }
}
