package org.broadinstitute.dsde.rawls.deltalayer

import akka.actor.ActorSystem
import cats.effect.IO
import com.google.api.client.http.HttpResponseException
import fs2._
import org.broadinstitute.dsde.rawls.google.GoogleUtilities
import org.broadinstitute.dsde.rawls.metrics.{GoogleInstrumented, GoogleInstrumentedService}
import org.broadinstitute.dsde.rawls.model.DeltaInsert
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext

class GcsDeltaLayerWriter(val storageService: GoogleStorageService[IO],
                          val sourceBucket: GcsBucketName,
                          override val workbenchMetricBaseName: String)
                         (implicit val system: ActorSystem, implicit val executionContext: ExecutionContext)
  extends DeltaLayerWriter with GoogleUtilities with GoogleInstrumented {

  implicit val service: GoogleInstrumentedService.Value = GoogleInstrumentedService.Storage

  override def writeFile(writeObject: DeltaInsert): Unit = {

    val destinationPath = filePath(writeObject)

    // set traceId equal to insertId for easy correlation
    val writePipe = storageService.streamUploadBlob(sourceBucket, destinationPath,
      overwrite = false,
      traceId = Some(TraceId(writeObject.insertId)))

    val fileContents = serializeFile(writeObject)

    retryWithRecoverWhen500orGoogleError(() =>
      text.utf8Encode(Stream.emit(fileContents)).through(writePipe).compile.drain.unsafeRunSync()
    ) {
      // are there any special error-handling cases we should include? Maybe 409 in case we've written the
      // file multiple times?
      case t: HttpResponseException =>
        logger.warn(s"encountered error [${t.getStatusMessage}] with status code [${t.getStatusCode}] when writing delta file [${sourceBucket.value}/${destinationPath.value}]")
        throw t
      case e: Exception =>
        logger.warn(s"encountered error [${e.getMessage}] when writing delta file [${sourceBucket.value}/${destinationPath.value}]")
        throw e
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
