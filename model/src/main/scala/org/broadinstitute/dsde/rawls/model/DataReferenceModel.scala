package org.broadinstitute.dsde.rawls.model

import bio.terra.workspace.model._
import com.fasterxml.jackson.databind.ObjectMapper
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsValue, RootJsonFormat, enrichAny}

case class DataReferenceName(value: String) extends ValueObject
case class NamedDataRepoSnapshot(name: DataReferenceName, snapshotId: String)

object DataReferenceModelJsonSupport {
  val objectMapper = new ObjectMapper()

  implicit object DataRepoSnapshotFormat extends RootJsonFormat[DataRepoSnapshot] {
    def write(snap: DataRepoSnapshot) = objectMapper.writeValueAsString(snap).toJson
    def read(json: JsValue) = objectMapper.readValue(json.compactPrint, classOf[DataRepoSnapshot])
  }

  implicit object DataReferenceDescriptionFormat extends RootJsonFormat[DataReferenceDescription] {
    def write(description: DataReferenceDescription) = objectMapper.writeValueAsString(description).toJson
    def read(json: JsValue): DataReferenceDescription = {
      println(json.toString)
      objectMapper.readValue(json.compactPrint, classOf[DataReferenceDescription])
    }
  }

  implicit object DataReferenceListFormat extends RootJsonFormat[DataReferenceList] {
    def write(refList: DataReferenceList) = objectMapper.writeValueAsString(refList).toJson
    def read(json: JsValue): DataReferenceList = objectMapper.readValue(json.compactPrint, classOf[DataReferenceList])
  }

  implicit val DataReferenceNameFormat = ValueObjectFormat(DataReferenceName)
  implicit val NamedDataRepoSnapshotFormat = jsonFormat2(NamedDataRepoSnapshot)
}
