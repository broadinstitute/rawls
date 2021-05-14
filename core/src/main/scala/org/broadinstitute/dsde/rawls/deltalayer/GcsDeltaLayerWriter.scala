package org.broadinstitute.dsde.rawls.deltalayer

import cats.effect.IO
import fs2._
import org.broadinstitute.dsde.rawls.model.DeltaInsert
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import spray.json._
import spray.json.DefaultJsonProtocol._

class GcsDeltaLayerWriter(val storageService: GoogleStorageService[IO],
                          val sourceBucket: GcsBucketName)
  extends DeltaLayerWriter {

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

  override def writeFile(writeObject: DeltaInsert): Unit = {
    // TODO: set overwrite value if desired and traceId value for tracing
    val writePipe = storageService.streamUploadBlob(sourceBucket, filePath(writeObject))
    val fileContents = serializeFile(writeObject)
    // TODO: retries and error-handling
    text.utf8Encode(Stream.emit(fileContents)).through(writePipe).compile.drain.unsafeRunSync()
  }

}
