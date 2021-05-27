package org.broadinstitute.dsde.rawls.deltalayer

import akka.actor.ActorSystem
import cats.effect.{Blocker, IO}
import cats.effect.concurrent.Semaphore
import com.google.cloud.storage.Storage
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext
import org.broadinstitute.dsde.workbench.google2.GoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

trait GcsStorageTestSupport {

  def getGcsWriter(bucket: GcsBucketName, storage: Option[Storage] = None)(implicit actorSystem: ActorSystem): GcsDeltaLayerWriter = {
    implicit val cs = IO.contextShift(testExecutionContext)
    implicit val timer = IO.timer(testExecutionContext)
    implicit val logger = Slf4jLogger.getLogger[IO]

    val db = storage.getOrElse(LocalStorageHelper.getOptions().getService())
    val blocker = Blocker.liftExecutionContext(testExecutionContext)
    val semaphore = Semaphore[IO](1).unsafeRunSync

    val localStorage = GoogleStorageInterpreter[IO](db, blocker, Some(semaphore))

    new GcsDeltaLayerWriter(localStorage, bucket, "metricsPrefix")
  }

}
