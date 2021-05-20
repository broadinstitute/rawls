package org.broadinstitute.dsde.rawls.deltalayer

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import cats.effect.IO
import com.google.cloud.storage.StorageException
import fs2._
import org.broadinstitute.dsde.rawls.google.GoogleUtilities
import org.broadinstitute.dsde.rawls.metrics.{GoogleInstrumented, GoogleInstrumentedService}
import org.broadinstitute.dsde.rawls.model.DeltaInsert
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

class GcsDeltaLayerWriter(val storageService: GoogleStorageService[IO],
                          val sourceBucket: GcsBucketName,
                          override val workbenchMetricBaseName: String)
                         (implicit val system: ActorSystem, implicit val executionContext: ExecutionContext)
  extends DeltaLayerWriter with GoogleUtilities with GoogleInstrumented {

  implicit val service: GoogleInstrumentedService.Value = GoogleInstrumentedService.Storage

  override def writeFile(writeObject: DeltaInsert): Future[Uri] = {

    val destinationPath = filePath(writeObject)
    val fileContents = serializeFile(writeObject)

    retryWithRecoverWhen500orGoogleError(() => {
      // create the destination pipe to which we will write the file
      // set traceId equal to insertId for easy correlation
      val writePipe = storageService.streamUploadBlob(sourceBucket, destinationPath,
        traceId = Some(TraceId(writeObject.insertId)))

      // stream the file contents to the destination GcsBlob pipe
      text.utf8Encode(Stream.emit(fileContents)).through(writePipe)
        .compile.drain.unsafeRunSync()

      // return Uri to the file we just wrote
      Uri.apply(s"gs://${sourceBucket.value}/${destinationPath.value}")

    }) {
      // additional logging, rethrow original error
      case se: StorageException =>
        logger.warn(s"encountered storage error [${se.getMessage}] with status code [${se.getCode}] when writing delta file [${sourceBucket.value}/${destinationPath.value}]")
        throw se
      case t: Throwable =>
        logger.warn(s"encountered error [${t.getMessage}] when writing delta file [${sourceBucket.value}/${destinationPath.value}]")
        throw t
    }
  }

  // calculate the GCS path to which we should save the file
  private[deltalayer] def filePath(writeObject: DeltaInsert): GcsBlobName = {
    // workspace/${workspaceId}/reference/${referenceId}/insert/${insertId}.json
    GcsBlobName(s"workspace/${writeObject.destination.workspaceId}/reference/${writeObject.destination.referenceId}/insert/${writeObject.insertId}.json")
  }

  // generate the string representation (json) of the file
  private[deltalayer] def serializeFile(writeObject: DeltaInsert): String = {
    val updateJson = writeObject.inserts.toJson.prettyPrint

    // TODO AS-770: use real JsonFormat classes! For now, since the model is in such flux, just hand-roll
    // the json
    s"""{
       |  "insertId": "${writeObject.insertId}",
       |  "workspaceId": "${writeObject.destination.workspaceId}",
       |  "referenceId": "${writeObject.destination.referenceId}",
       |  "insertTimestamp": "${writeObject.insertTimestamp.toString}",
       |  "insertingUser": "${writeObject.insertingUser.value}",
       |  "inserts": $updateJson
       |}
       |""".stripMargin.parseJson.prettyPrint
  }

}
