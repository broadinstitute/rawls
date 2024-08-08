package org.broadinstitute.dsde.rawls.model

import akka.http.scaladsl.model.StatusCodes.BadRequest
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.{MethodRepoMethodFormat, WorkspaceSettingFormat}
import org.broadinstitute.dsde.rawls.model.WorkspaceSettingConfig.{GcpBucketLifecycleConfig, GcpBucketLifecycleRule, GcpBucketLifecycleAction, GcpBucketLifecycleCondition}
import org.joda.time.DateTime
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

class WorkspaceModelSpec extends AnyFreeSpec with Matchers {

  val trickyBit = "/+:?&~!@#$^*()[]{}∞€\\"
  val trickyBitEncoded = java.net.URLEncoder.encode(trickyBit, java.nio.charset.StandardCharsets.UTF_8.name)

  "MethodRepoMethod" - {

    "apply() does the right thing" in {
      assertResult(MethodRepoMethod.apply("path", "version")) {
        DockstoreMethod("path", "version")
      }
      assertResult(MethodRepoMethod.apply("namespace", "name", 555)) {
        AgoraMethod("namespace", "name", 555)
      }
    }

    "JSON logic" - {

      "Serialize" - {

        "Agora" in {
          assertResult {
            """{"methodName":"test-name","methodVersion":555,"methodNamespace":"test-namespace","methodUri":"agora://test-namespace/test-name/555","sourceRepo":"agora"}""".parseJson
          } {
            MethodRepoMethodFormat.write(AgoraMethod("test-namespace", "test-name", 555))
          }
        }

        "Dockstore" in {
          assertResult {
            """{"methodUri":"dockstore://test-path/test-version","sourceRepo":"dockstore","methodPath":"test-path","methodVersion":"test-version"}""".parseJson
          } {
            MethodRepoMethodFormat.write(DockstoreMethod("test-path", "test-version"))
          }
        }

        "DockstoreTools" in {
          assertResult {
            """{"methodUri":"dockstoretools://test-path/test-version","sourceRepo":"dockstoretools","methodPath":"test-path","methodVersion":"test-version"}""".parseJson
          } {
            MethodRepoMethodFormat.write(DockstoreToolsMethod("test-path", "test-version"))
          }
        }
      }

      "Deserialize" - {

        "Agora" in {

          // Round-trip
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read(
              """{"methodName":"test-name","methodVersion":555,"methodNamespace":"test-namespace","methodUri":"agora://test-namespace/test-name/555","sourceRepo":"agora"}""".parseJson
            )
          }

          // URI takes precedence
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read(
              """{"methodName":"not-the-name","methodVersion":777,"methodNamespace":"not-the-namespace","methodUri":"agora://test-namespace/test-name/555","sourceRepo":"fake-repo"}""".parseJson
            )
          }

          // URI alone is OK
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read("""{"methodUri":"agora://test-namespace/test-name/555"}""".parseJson)
          }

          // Fall back to fields
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read(
              """{"methodName":"test-name","methodVersion":555,"methodNamespace":"test-namespace","sourceRepo":"agora"}""".parseJson
            )
          }

          // No "sourceRepo" -> default to Agora
          assertResult {
            AgoraMethod("test-namespace", "test-name", 555)
          } {
            MethodRepoMethodFormat.read(
              """{"methodName":"test-name","methodVersion":555,"methodNamespace":"test-namespace"}""".parseJson
            )
          }
        }

        "Dockstore" in {

          // Round-trip
          assertResult {
            DockstoreMethod("test-path", "test-version")
          } {
            MethodRepoMethodFormat.read(
              """{"methodUri":"dockstore://test-path/test-version","sourceRepo":"dockstore","methodPath":"test-path","methodVersion":"test-version"}""".parseJson
            )
          }

          // URI takes precedence
          assertResult {
            DockstoreMethod("test-path", "test-version")
          } {
            MethodRepoMethodFormat.read(
              """{"methodUri":"dockstore://test-path/test-version","sourceRepo":"fake-repo","methodPath":"not-the-path","methodVersion":"not-the-version"}""".parseJson
            )
          }

          // URI alone is OK
          assertResult {
            DockstoreMethod("test-path", "test-version")
          } {
            MethodRepoMethodFormat.read("""{"methodUri":"dockstore://test-path/test-version"}""".parseJson)
          }

          // Fall back to fields
          assertResult {
            DockstoreMethod("test-path", "test-version")
          } {
            MethodRepoMethodFormat.read(
              """{"sourceRepo":"dockstore","methodPath":"test-path","methodVersion":"test-version"}""".parseJson
            )
          }

          // No "sourceRepo" -> Dockstore throws an exception because we default to Agora
          intercept[spray.json.DeserializationException] {
            MethodRepoMethodFormat.read("""{"methodPath":"test-path","methodVersion":"test-version"}""".parseJson)
          }
        }

        "DockstoreTools" in {
          assertResult {
            DockstoreToolsMethod("test-path", "test-version")
          } {
            MethodRepoMethodFormat.read(
              """{"sourceRepo":"dockstoretools","methodPath":"test-path","methodVersion":"test-version"}""".parseJson
            )
          }
        }

        // Bad "sourceRepo"
        intercept[spray.json.DeserializationException] {
          MethodRepoMethodFormat.read(
            """{"sourceRepo":"marks-methods-mart","methodPath":"test-path","methodVersion":"test-version"}""".parseJson
          )
        }

        // Bad "sourceRepo"
        intercept[spray.json.DeserializationException] {
          MethodRepoMethodFormat.read(
            """{"sourceRepo":777,"methodPath":"test-path","methodVersion":"test-version"}""".parseJson
          )
        }

        // Bad repo in URI
        intercept[spray.json.DeserializationException] {
          MethodRepoMethodFormat.read("""{"methodUri":"marks-methods-mart://test-path/test-version"}""".parseJson)
        }
      }
    }

  }

  "AgoraMethod" - {

    val nameNeedsEncoding = s"${trickyBit}test${trickyBit}name$trickyBit"
    val nameEncoded = s"${trickyBitEncoded}test${trickyBitEncoded}name$trickyBitEncoded"

    val namespaceNeedsEncoding = s"${trickyBit}test${trickyBit}namespace$trickyBit"
    val namespaceEncoded = s"${trickyBitEncoded}test${trickyBitEncoded}namespace$trickyBitEncoded"

    val goodMethod = AgoraMethod("test-namespace", "test-name", 555)
    val goodMethodWithCharsToEncode =
      AgoraMethod(namespaceNeedsEncoding, nameNeedsEncoding, 555)
    val badMethod1 = AgoraMethod("a", "", 1)
    val badMethod2 = AgoraMethod("a", "b", 0)

    "Validation works as one would expect" - {
      "for good methods" in {
        assertResult(goodMethod.validate) {
          Some(goodMethod)
        }

        assertResult(goodMethodWithCharsToEncode.validate) {
          Some(goodMethodWithCharsToEncode)
        }
      }

      "for bad methods" in {
        assertResult(badMethod1.validate) {
          None
        }
        assertResult(badMethod2.validate) {
          None
        }
      }
    }

    "Can convert to a method repo URI" - {
      "For valid methods" - {

        "for Agora" in {
          assertResult("agora://test-namespace/test-name/555") {
            goodMethod.methodUri
          }

          assertResult(s"agora://$namespaceEncoded/$nameEncoded/555") {
            goodMethodWithCharsToEncode.methodUri
          }
        }
      }

      "for nasty bad methodses" in {

        intercept[RawlsException] {
          badMethod1.methodUri
        }
        intercept[RawlsException] {
          badMethod2.methodUri
        }
      }

    }

    "Can be created from a method URI" - {
      val methodUri = "agora://test-namespace/test-name/555"
      val methodUriWithEncodedChars = s"agora://$namespaceEncoded/$nameEncoded/555"

      "from good URIs" in {
        assertResult(AgoraMethod("test-namespace", "test-name", 555)) {
          MethodRepoMethod.fromUri(methodUri)
        }

        assertResult(AgoraMethod(namespaceNeedsEncoding, nameNeedsEncoding, 555)) {
          MethodRepoMethod.fromUri(methodUriWithEncodedChars)
        }
      }

      val badUri1 = null
      val badUri2 = ""
      val badUri3 = "agoraphobia"
      val badUri4 = "agora://"
      val badUri5 = "agora://test-namespace/test-name"
      val badUri6 = "agora://test-namespace/1"
      val badUri7 = "agora://test-namespace/test-name/bad/path"
      val badUri8 = "marks-methods-mart://test-namespace/test-name/555"
      val badUri9 = "marks-methods-mart://test-namespace/test-name/0" // proves that validation works in apply()

      "catches bad URIs" in {
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri1)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri2)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri3)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri4)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri5)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri6)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri7)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri8)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri9)
        }
      }
    }
  }

  "DockstoreMethod" - {

    val pathNeedsEncoding = s"${trickyBit}test${trickyBit}path$trickyBit"
    val pathEncoded = s"${trickyBitEncoded}test${trickyBitEncoded}path$trickyBitEncoded"

    val versionNeedsEncoding = s"${trickyBit}test${trickyBit}version$trickyBit"
    val versionEncoded = s"${trickyBitEncoded}test${trickyBitEncoded}version$trickyBitEncoded"

    val goodMethod = DockstoreMethod("test-path", "test-version")
    val goodMethodWithCharsToEncode =
      DockstoreMethod(pathNeedsEncoding, versionNeedsEncoding)
    val badMethod1 = DockstoreMethod("a", "")
    val badMethod2 = DockstoreMethod("", "b")

    "Validation works as one would expect" - {
      "for good methods" in {
        assertResult(goodMethod.validate) {
          Some(goodMethod)
        }

        assertResult(goodMethodWithCharsToEncode.validate) {
          Some(goodMethodWithCharsToEncode)
        }
      }

      "for bad methods" in {
        assertResult(badMethod1.validate) {
          None
        }
        assertResult(badMethod2.validate) {
          None
        }
      }
    }

    "Can convert to a method repo URI" - {
      "For valid methods" - {

        "for Dockstore" in {
          assertResult("dockstore://test-path/test-version") {
            goodMethod.methodUri
          }

          assertResult(s"dockstore://$pathEncoded/$versionEncoded") {
            goodMethodWithCharsToEncode.methodUri
          }
        }
      }

      "for nasty bad methodses" in {

        intercept[RawlsException] {
          badMethod1.methodUri
        }
        intercept[RawlsException] {
          badMethod2.methodUri
        }
      }

    }

    "Can be created from a method URI" - {
      val methodUri = "dockstore://test-path/test-version"
      val methodUriWithEncodedChars = s"dockstore://$pathEncoded/$versionEncoded"

      "from good URIs" in {
        assertResult(DockstoreMethod("test-path", "test-version")) {
          MethodRepoMethod.fromUri(methodUri)
        }

        assertResult(DockstoreMethod(pathNeedsEncoding, versionNeedsEncoding)) {
          MethodRepoMethod.fromUri(methodUriWithEncodedChars)
        }
      }

      val badUri1 = null
      val badUri2 = ""
      val badUri3 = "dockstoreblah"
      val badUri4 = "dockstore://"
      val badUri5 = "dockstore://path-but-no-version"
      val badUri6 = "marks-methods-mart://test-path/test-version"
      val badUri7 = "marks-methods-mart://test-path"

      "catches bad URIs" in {
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri1)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri2)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri3)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri4)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri5)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri6)
        }
        intercept[RawlsException] {
          MethodRepoMethod.fromUri(badUri7)
        }
      }
    }
  }

  "WorkspaceFieldNames" - {
    // if the WorkspaceResponse or WorkspaceDetails case classes change shape, these tests will fail.
    "should introspect WorkspaceResponse correctly" in {
      val expected = List("accessLevel",
                          "canShare",
                          "canCompute",
                          "catalog",
                          "workspace",
                          "workspaceSubmissionStats",
                          "bucketOptions",
                          "owners",
                          "azureContext",
                          "policies"
      )
      WorkspaceFieldNames.workspaceResponseClassNames should contain theSameElementsAs expected
    }
    "should introspect WorkspaceDetails correctly" in {
      val expected = List(
        "namespace",
        "name",
        "workspaceId",
        "bucketName",
        "workflowCollectionName",
        "createdDate",
        "lastModified",
        "createdBy",
        "attributes",
        "isLocked",
        "authorizationDomain",
        "googleProject",
        "googleProjectNumber",
        "workspaceVersion",
        "billingAccount",
        "billingAccountErrorMessage",
        "errorMessage",
        "completedCloneWorkspaceFileTransfer",
        "workspaceType",
        "cloudPlatform",
        "state"
      )
      WorkspaceFieldNames.workspaceDetailClassNames should contain theSameElementsAs expected
    }
    "should collate WorkspaceResponse and WorkspaceDetails correctly" in {
      val expected = List(
        "azureContext",
        "policies",
        "accessLevel",
        "canShare",
        "canCompute",
        "catalog",
        "workspace",
        "workspaceSubmissionStats",
        "bucketOptions",
        "owners",
        "workspace.namespace",
        "workspace.name",
        "workspace.workspaceId",
        "workspace.bucketName",
        "workspace.workflowCollectionName",
        "workspace.createdDate",
        "workspace.lastModified",
        "workspace.createdBy",
        "workspace.attributes",
        "workspace.isLocked",
        "workspace.authorizationDomain",
        "workspace.googleProject",
        "workspace.googleProjectNumber",
        "workspace.workspaceVersion",
        "workspace.billingAccount",
        "workspace.errorMessage",
        "workspace.billingAccountErrorMessage",
        "workspace.completedCloneWorkspaceFileTransfer",
        "workspace.workspaceType",
        "workspace.cloudPlatform",
        "workspace.state"
      )
      WorkspaceFieldNames.workspaceResponseFieldNames should contain theSameElementsAs expected
    }
  }

  "Attribute counter" - {
    "should recursively count attributes" in {
      val map1 = Map(
        AttributeName("namespace1", "name1") -> AttributeString("value1")
      )
      val map2: AttributeMap = Map.empty
      val map3 = Map(
        AttributeName("namespace1", "name1") -> AttributeValueEmptyList
      )
      val map4 = Map(
        AttributeName("namespace1", "name1") -> AttributeValueList(
          Seq(
            AttributeNumber(1),
            AttributeNumber(2),
            AttributeNumber(3)
          )
        )
      )
      val map5 = Map(
        AttributeName("namespace1", "name1") -> AttributeString("value1"),
        AttributeName("namespace2", "name2") -> AttributeString("value2")
      )

      // https://broadworkbench.atlassian.net/browse/BW-689?focusedCommentId=47211
      val map6 = Map(
        AttributeName("default", "read_counts") ->
          AttributeValueList(
            Vector(
              AttributeString("gs://my-workflow/shard-0/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-1/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-2/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-3/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-4/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-5/cacheCopy/some_file.tsv")
            )
          )
      )

      Attributable.attributeCount(map1.values) shouldBe 1
      Attributable.attributeCount(map2.values) shouldBe 0
      Attributable.attributeCount(map3.values) shouldBe 0
      Attributable.attributeCount(map4.values) shouldBe 3
      Attributable.attributeCount(map5.values) shouldBe 2
      Attributable.attributeCount(map6.values) shouldBe 6
    }
  }

  "Safe printer" - {
    "should safely print attributes" in {
      val simpleMap = Map(
        AttributeName("namespace1", "name1") -> AttributeString("value1")
      )
      val emptyMap: AttributeMap = Map.empty
      val emptyListMap = Map(
        AttributeName("namespace1", "name1") -> AttributeValueEmptyList
      )
      val populatedListMap = Map(
        AttributeName("namespace1", "name1") -> AttributeValueList(
          Seq(
            AttributeNumber(1),
            AttributeNumber(2),
            AttributeNumber(3)
          )
        )
      )
      val multiKeyMap = Map(
        AttributeName("namespace1", "name1") -> AttributeString("value1"),
        AttributeName("namespace2", "name2") -> AttributeString("value2")
      )
      val realisticUserMap = Map(
        AttributeName("default", "read_counts") ->
          AttributeValueList(
            Vector(
              AttributeString("gs://my-workflow/shard-0/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-1/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-2/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-3/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-4/cacheCopy/some_file.tsv"),
              AttributeString("gs://my-workflow/shard-5/cacheCopy/some_file.tsv")
            )
          )
      )

      val attributeReferenceMap =
        Map(AttributeName("default", "read_counts") -> AttributeEntityReference("type", "name"))

      val attributeReferenceListMap = Map(
        AttributeName("default", "read_counts") -> AttributeEntityReferenceList(
          Seq(AttributeEntityReference("type", "name1"), AttributeEntityReference("type", "name2"))
        )
      )

      // We forbid nested lists. `AttributeValueList` requires its contents to be an `AttributeValue` and not another `AttributeValueList`
      // For example, this would not compile:
      // val map7 = Map(
      //   AttributeName("default", "read_counts") ->
      //     AttributeValueList(
      //       Vector(
      //         AttributeValueList(
      //           Vector(
      //             AttributeString("gs://my-workflow/shard-0/cacheCopy/some_file.tsv"),
      //             AttributeString("gs://my-workflow/shard-1/cacheCopy/some_file.tsv"),
      //             AttributeString("gs://my-workflow/shard-2/cacheCopy/some_file.tsv"),
      //             AttributeString("gs://my-workflow/shard-3/cacheCopy/some_file.tsv"),
      //             AttributeString("gs://my-workflow/shard-4/cacheCopy/some_file.tsv"),
      //             AttributeString("gs://my-workflow/shard-5/cacheCopy/some_file.tsv")
      //         )
      //       )
      //     )
      // )

      Attributable.safePrint(
        simpleMap
      ) shouldBe "[First 10 items] Map(AttributeName(namespace1,name1) -> AttributeString(value1))"
      Attributable.safePrint(emptyMap) shouldBe "[First 10 items] Map()"
      Attributable.safePrint(emptyListMap) shouldBe "[First 10 items] Map(AttributeName(namespace1,name1) -> List())"
      Attributable.safePrint(
        populatedListMap
      ) shouldBe "[First 10 items] Map(AttributeName(namespace1,name1) -> List(AttributeNumber(1), AttributeNumber(2), AttributeNumber(3)))"
      Attributable.safePrint(
        multiKeyMap
      ) shouldBe "[First 10 items] Map(AttributeName(namespace1,name1) -> AttributeString(value1), AttributeName(namespace2,name2) -> AttributeString(value2))"
      Attributable.safePrint(realisticUserMap,
                             2
      ) shouldBe "[First 2 items] Map(AttributeName(default,read_counts) -> Vector(AttributeString(gs://my-workflow/shard-0/cacheCopy/some_file.tsv), AttributeString(gs://my-workflow/shard-1/cacheCopy/some_file.tsv)))"
      Attributable.safePrint(attributeReferenceMap,
                             2
      ) shouldBe "[First 2 items] Map(AttributeName(default,read_counts) -> AttributeEntityReference(type,name))"
      Attributable.safePrint(attributeReferenceListMap,
                             2
      ) shouldBe "[First 2 items] Map(AttributeName(default,read_counts) -> List(AttributeEntityReference(type,name1), AttributeEntityReference(type,name2)))"
    }
  }

  "Workspace Type" - {
    "should parse workspace type properly" in {
      WorkspaceType.withName("rawls") shouldBe WorkspaceType.RawlsWorkspace
      WorkspaceType.withName("mc") shouldBe WorkspaceType.McWorkspace
    }

    "should fail with invalid workspace type" in {
      val thrown = intercept[RawlsException] {
        WorkspaceType.withName("incorrect")
      }

      thrown.getMessage.contains("Invalid WorkspaceType")
    }

    "should output the string representation" in {
      WorkspaceType.McWorkspace.toString shouldBe "mc"
      WorkspaceType.RawlsWorkspace.toString shouldBe "rawls"
    }
  }

  "errorMessage" - {
    "populates both the errorMessage and billingErrorMessage fields in the details" in {
      val error = "error message"
      val ws = Workspace("ws-namespace",
                         "ws-name",
                         "ws-id",
                         "bucketName",
                         None,
                         DateTime.now(),
                         DateTime.now(),
                         "aUser",
                         Map.empty
      )
        .copy(errorMessage = Some(error))

      val details = WorkspaceDetails.fromWorkspaceAndOptions(ws, None, useAttributes = false)
      details.errorMessage shouldBe Some(error)
      details.billingAccountErrorMessage shouldBe Some(error)
    }

  }

  "toWSMPolicyInput" - {
    "throws an exception for malformed additional fields" in {
      val e = intercept[RawlsExceptionWithErrorReport] {
        WorkspacePolicy("group-constraint", "terra", List(Map("group" -> "myFakeGroup", "otherInvalid" -> "other")))
          .toWsmPolicyInput()
      }
      e.errorReport.statusCode shouldBe Some(BadRequest)
    }
  }

  "WorkspaceSetting" - {
    "throws an exception for invalid workspace setting type" in {
      val fakeSetting =
        """{
          |    "type": "FakeWorkspaceSetting",
          |    "config": {
          |      "rules": []
          |    }
          |  }""".stripMargin.parseJson

      intercept[RawlsException] {
        WorkspaceSettingFormat.read(fakeSetting)
      }
    }

    "GoogleBucketLifecycleSettings" - {
      "parses lifecycle settings with matchesPrefix and age" in {
        val lifecycleSetting =
          """{
            |    "type": "GcpBucketLifecycle",
            |    "config": {
            |      "rules": [
            |        {
            |          "action": {
            |            "type": "Delete"
            |          },
            |          "conditions": {
            |            "age": 30,
            |            "matchesPrefix": [
            |              "prefix1",
            |              "prefix2"
            |            ]
            |          }
            |        }
            |      ]
            |    }
            |  }""".stripMargin.parseJson
        assertResult {
          WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle, GcpBucketLifecycleConfig(List(GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set("prefix1", "prefix2"), Some(30))))))
        } {
          WorkspaceSettingFormat.read(lifecycleSetting)
        }
      }

      "parses lifecycle settings with no prefixes" in {
        val lifecycleSettingNoPrefixes =
          """{
            |    "type": "GcpBucketLifecycle",
            |    "config": {
            |      "rules": [
            |        {
            |          "action": {
            |            "type": "Delete"
            |          },
            |          "conditions": {
            |            "age": 30,
            |            "matchesPrefix": []
            |          }
            |        }
            |      ]
            |    }
            |  }""".stripMargin.parseJson
        assertResult {
          WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle, GcpBucketLifecycleConfig(List(GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set.empty, Some(30))))))
        } {
          WorkspaceSettingFormat.read(lifecycleSettingNoPrefixes)
        }
      }

      "parses lifecycle settings with no age" in {
        val lifecycleSettingNoAge =
          """{
            |    "type": "GcpBucketLifecycle",
            |    "config": {
            |      "rules": [
            |        {
            |          "action": {
            |            "type": "Delete"
            |          },
            |          "conditions": {
            |            "matchesPrefix": [
            |              "prefix1",
            |              "prefix2"
            |            ]
            |          }
            |        }
            |      ]
            |    }
            |  }""".stripMargin.parseJson
        assertResult {
          WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle, GcpBucketLifecycleConfig(List(GcpBucketLifecycleRule(GcpBucketLifecycleAction("Delete"), GcpBucketLifecycleCondition(Set("prefix1", "prefix2"), None)))))
        } {
          WorkspaceSettingFormat.read(lifecycleSettingNoAge)
        }
      }

      "parses lifecycle settings with no rules" in {
        val lifecycleSettingNoRules =
          """{
            |    "type": "GcpBucketLifecycle",
            |    "config": {
            |      "rules": []
            |    }
            |  }""".stripMargin.parseJson
        assertResult {
          WorkspaceSetting(WorkspaceSettingTypes.GcpBucketLifecycle, GcpBucketLifecycleConfig(List.empty))
        } {
          WorkspaceSettingFormat.read(lifecycleSettingNoRules)
        }
      }

      "throws an exception for missing config" in {
        val lifecycleSettingNoConfig =
          """{
            |    "type": "GcpBucketLifecycle"
            |  }""".stripMargin.parseJson
        intercept[NoSuchElementException] {
          WorkspaceSettingFormat.read(lifecycleSettingNoConfig)
        }
      }

      "throws an exception for incorrect format" in {
        val lifecycleSettingBadConfig =
          """{
            |    "type": "GcpBucketLifecycle",
            |    "config": {
            |      "rules": "not a list"
            |    }
            |  }""".stripMargin.parseJson
        intercept[DeserializationException] {
          WorkspaceSettingFormat.read(lifecycleSettingBadConfig)
        }
      }
    }
  }
}
