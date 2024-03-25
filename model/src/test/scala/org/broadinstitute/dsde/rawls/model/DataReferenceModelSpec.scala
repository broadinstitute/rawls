package org.broadinstitute.dsde.rawls.model

import bio.terra.workspace.model.ResourceType.DATA_REPO_SNAPSHOT
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

import java.util.UUID
import scala.jdk.CollectionConverters._

class DataReferenceModelSpec extends AnyFreeSpec with Matchers {

  "DataReferenceModel" - {
    "stringOrNull() does the right thing" in {
      assertResult(JsNull) {
        stringOrNull(null)
      }
      assertResult(JsString("x")) {
        stringOrNull("x")
      }
      assertResult(JsString(DATA_REPO_SNAPSHOT.toString)) {
        stringOrNull(DATA_REPO_SNAPSHOT)
      }
    }

    "JSON logic" - {

      "DataRepoSnapshotResource, ResourceMetadata, DataRepoSnapshotAttributes" in {
        val resourceId = UUID.randomUUID()
        val workspaceId = UUID.randomUUID()
        val snapshotId = UUID.randomUUID()
        assertResult {
          s"""{
                "attributes": {
                  "instanceName": "test-instance",
                  "snapshot": "$snapshotId"
                },
                "metadata": {
                  "workspaceId": "$workspaceId",
                  "resourceId": "$resourceId",
                  "name": "testReference",
                  "description": "hello",
                  "resourceType": "DATA_REPO_SNAPSHOT",
                  "stewardshipType": "REFERENCED",
                  "cloningInstructions": "COPY_NOTHING",
                  "properties": {
                    "key1": "value1",
                    "key2": "value2"
                  }
                }
             }
          """.parseJson
        } {
          val properties = new Properties()
          properties.addAll(
            java.util.List.of(new Property().key("key1").value("value1"), new Property().key("key2").value("value2"))
          )

          new DataRepoSnapshotResource()
            .metadata(
              new ResourceMetadata()
                .name("testReference")
                .resourceId(resourceId)
                .workspaceId(workspaceId)
                .description("hello")
                .resourceType(ResourceType.DATA_REPO_SNAPSHOT)
                .stewardshipType(StewardshipType.REFERENCED)
                .cloningInstructions(CloningInstructionsEnum.NOTHING)
                .properties(properties)
            )
            .attributes(
              new DataRepoSnapshotAttributes()
                .instanceName("test-instance")
                .snapshot(snapshotId.toString)
            )
            .toJson
        }

      }

      "ResourceList, ResourceDescription, ResourceMetadata, ResourceAttributesUnion, DataRepoSnapshotAttributes" in {
        val snapshotResourceId = UUID.randomUUID()
        val workspaceId = UUID.randomUUID()
        val snapshotId = UUID.randomUUID()
        assertResult {
          s"""
             {
               "resources": [
                 {
                   "metadata": {
                     "cloningInstructions":"COPY_NOTHING",
                     "description":"im a lil snapshot",
                     "name":"snapshot1",
                     "resourceId":"$snapshotResourceId",
                     "resourceType":"DATA_REPO_SNAPSHOT",
                     "stewardshipType":"REFERENCED",
                     "workspaceId":"$workspaceId",
                     "properties": {}
                   },
                   "resourceAttributes": {
                     "gcpDataRepoSnapshot": {
                        "instanceName":"test-instance",
                        "snapshot":"$snapshotId"
                     }
                   }
                 }
               ]
             }
             """.parseJson
        } {
          new ResourceList()
            .resources(
              List(
                new ResourceDescription()
                  .metadata(
                    new ResourceMetadata()
                      .name("snapshot1")
                      .description("im a lil snapshot")
                      .resourceId(snapshotResourceId)
                      .resourceType(ResourceType.DATA_REPO_SNAPSHOT)
                      .stewardshipType(StewardshipType.REFERENCED)
                      .workspaceId(workspaceId)
                      .cloningInstructions(CloningInstructionsEnum.NOTHING)
                  )
                  .resourceAttributes(
                    new ResourceAttributesUnion()
                      .gcpDataRepoSnapshot(
                        new DataRepoSnapshotAttributes()
                          .instanceName("test-instance")
                          .snapshot(snapshotId.toString)
                      )
                  )
              ).asJava
            )
            .toJson
        }
      }

      "Parsing ResourceMetadata should fail if resource id string is not a UUID" in {
        val workspaceId = UUID.randomUUID()
        assertThrows[DeserializationException] {
          s"""
             {
               "cloningInstructions": "COPY_NOTHING",
               "description": "im a lil snapshot",
               "name": "snapshot1",
               "resourceId": "not-an-id",
               "resourceType": "DATA_REPO_SNAPSHOT",
               "stewardshipType": "REFERENCED",
               "workspaceId": "$workspaceId"
              }""".parseJson.convertTo[ResourceMetadata]
        }
      }

      "should fail if workspace id string is not a UUID" in {
        val resourceId = UUID.randomUUID()
        assertThrows[DeserializationException] {
          s"""
             {
               "cloningInstructions":"COPY_NOTHING",
               "description": "im a lil snapshot",
               "name": "snapshot1",
               "resourceId": "$resourceId",
               "resourceType": "DATA_REPO_SNAPSHOT",
               "stewardshipType": "REFERENCED",
               "workspaceId": "not-an-id"
              }""".parseJson.convertTo[ResourceMetadata]
        }
      }

      "DataRepoSnapshotAttributes should fail with nulls" in {
        val snapshotId = UUID.randomUUID()
        assertThrows[DeserializationException] {
          s"""{"instanceName": null, "snapshot": "$snapshotId"}""".parseJson.convertTo[DataRepoSnapshotAttributes]
        }

        assertThrows[DeserializationException] {
          s"""{"instanceName": "fake-instance", "snapshot": null}""".parseJson.convertTo[DataRepoSnapshotAttributes]
        }
      }

      "ResourceMetadata should fail with nulls" in {
        val workspaceId = UUID.randomUUID()
        val resourceId = UUID.randomUUID()
        assertThrows[DeserializationException] {
          s"""{ "workspaceId": "$workspaceId",
                "resourceId": "resourceId",
                "name": null,
                "description": "this is my snapshot",
                "resourceType": "DATA_REPO_SNAPSHOT",
                "resourceId": "$resourceId",
                "stewardshipType": "REFERENCED",
                "cloningInstructions": "COPY_NOTHING"
             }""".parseJson.convertTo[ResourceMetadata]
        }
      }

      "ResourceMetadata should succeed with nulls on fields we are not translating" in {
        val workspaceId = UUID.randomUUID()
        val resourceId = UUID.randomUUID()
        assertResult {
          s"""{ "workspaceId": "$workspaceId",
                             "resourceId": "resourceId",
                             "name": "mysnapshot",
                             "description": "this is my snapshot",
                             "resourceType": "DATA_REPO_SNAPSHOT",
                             "resourceId": "$resourceId",
                             "stewardshipType": "REFERENCED",
                             "cloudPlatform": null,
                             "cloningInstructions": "COPY_NOTHING" } """.parseJson.convertTo[ResourceMetadata]
        } {
          new ResourceMetadata()
            .name("mysnapshot")
            .description("this is my snapshot")
            .resourceId(resourceId)
            .resourceType(ResourceType.DATA_REPO_SNAPSHOT)
            .stewardshipType(StewardshipType.REFERENCED)
            .workspaceId(workspaceId)
            .cloningInstructions(CloningInstructionsEnum.NOTHING)
        }
      }

      "UpdateDataReferenceRequestBody should work when updating name and description" in {
        assertResult(s"""{"name":"foo","description":"bar","instanceName":null,"snapshot":null}""".parseJson) {
          new UpdateDataRepoSnapshotReferenceRequestBody().name("foo").description("bar").toJson
        }
      }

      "UpdateDataReferenceRequestBody should work with only one parameter" in {
        assertResult(s"""{"name":null,"description":"foo","instanceName":null,"snapshot":null}""".parseJson) {
          new UpdateDataRepoSnapshotReferenceRequestBody().description("foo").toJson
        }
      }

      "UpdateDataReferenceRequestBody with no parameters should fail" in {
        assertThrows[DeserializationException] {
          s"""{}""".parseJson.convertTo[UpdateDataRepoSnapshotReferenceRequestBody]
        }
      }
    }
  }
}
