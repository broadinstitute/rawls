package org.broadinstitute.dsde.rawls.deltalayer

import akka.actor.ActorSystem
import cats.effect.IO
import cats.effect.std.Semaphore
import com.google.cloud.storage.Storage
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.typelevel.log4cats.slf4j.Slf4jLogger

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
