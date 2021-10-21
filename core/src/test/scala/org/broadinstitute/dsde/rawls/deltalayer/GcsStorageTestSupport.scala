package org.broadinstitute.dsde.rawls.deltalayer

import akka.actor.ActorSystem
import cats.effect.IO
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import com.google.cloud.storage.Storage
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.broadinstitute.dsde.rawls.TestExecutionContext.testExecutionContext
import org.broadinstitute.dsde.workbench.google2.GoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait GcsStorageTestSupport {

  def getGcsWriter(bucket: GcsBucketName, storage: Option[Storage] = None)(implicit actorSystem: ActorSystem): GcsDeltaLayerWriter = {
    implicit val logger = Slf4jLogger.getLogger[IO]

    val db = storage.getOrElse(LocalStorageHelper.getOptions().getService())
    val semaphore = Semaphore[IO](1).unsafeRunSync

    val localStorage = GoogleStorageInterpreter[IO](db, Some(semaphore))

    new GcsDeltaLayerWriter(localStorage, bucket, "metricsPrefix")
  }

}
