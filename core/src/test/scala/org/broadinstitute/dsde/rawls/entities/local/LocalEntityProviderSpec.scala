package org.broadinstitute.dsde.rawls.entities.local

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigTestSupport
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString, Entity, EntityTypeMetadata}
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AddUpdateAttribute, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeNumber,
  AttributeString,
  AttributeValueEmptyList,
  AttributeValueList,
  Entity,
  EntityQuery,
  MethodConfiguration,
  RawlsRequestContext,
  SortDirections,
  SubmissionValidationValue,
  WDL,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.scalatest.RecoverMethods.recoverToExceptionIf
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.ExecutionContext

class LocalEntityProviderSpec
    extends AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with TestDriverComponent
    with MethodConfigTestSupport
    with StatsDTestUtils
    with Eventually
    with MockitoTestUtils {
  import driver.api._

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(3000, Millis)))

  implicit private val system: ActorSystem = ActorSystem("LocalEntityProviderSpec")

  val testConf = ConfigFactory.load()

  // Test harness to call resolveInputsForEntities without having to go via the WorkspaceService
  def testResolveInputs(workspaceContext: Workspace,
                        methodConfig: MethodConfiguration,
                        entity: Entity,
                        wdl: WDL,
                        dataAccess: DataAccess
  )(implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {

    val localEntityProvider = new LocalEntityProvider(
      EntityRequestArguments(workspaceContext, testContext),
      slickDataSource,
      testConf.getBoolean("entityStatisticsCache.enabled"),
      testConf.getDuration("entities.queryTimeout"),
      workbenchMetricBaseName
    )

    dataAccess.entityQuery
      .findEntityByName(workspaceContext.workspaceIdAsUUID, entity.entityType, entity.name)
      .result flatMap { entityRecs =>
      methodConfigResolver.gatherInputs(userInfo, methodConfig, wdl) match {
        case scala.util.Failure(exception) =>
          DBIO.failed(exception)
        case scala.util.Success(gatherInputsResult: GatherInputsResult)
            if gatherInputsResult.extraInputs.nonEmpty || gatherInputsResult.missingInputs.nonEmpty =>
          DBIO.failed(new RawlsException(s"gatherInputsResult has missing or extra inputs: $gatherInputsResult"))
        case scala.util.Success(gatherInputsResult: GatherInputsResult) =>
          localEntityProvider.evaluateExpressionsInternal(workspaceContext,
                                                          gatherInputsResult.processableInputs,
                                                          Some(entityRecs),
                                                          dataAccess
          )
      }
    }
  }

  "LocalEntityProvider" should {
    "resolve method config inputs" in withLegacyConfigData {
      val context = workspace

      runAndWait(testResolveInputs(context, configGood, sampleGood, littleWdl, this)) shouldBe
        Map(sampleGood.name -> Seq(SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName)))

      runAndWait(testResolveInputs(context, configEvenBetter, sampleGood, littleWdl, this)) shouldBe
        Map(
          sampleGood.name -> Seq(
            SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName),
            SubmissionValidationValue(Some(AttributeNumber(1)), None, intOptNameWithWfName)
          )
        )

      runAndWait(testResolveInputs(context, configSampleSet, sampleSet, arrayWdl, this)) shouldBe
        Map(
          sampleSet.name -> Seq(
            SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1)))), None, intArrayNameWithWfName)
          )
        )

      runAndWait(testResolveInputs(context, configSampleSet, sampleSet2, arrayWdl, this)) shouldBe
        Map(
          sampleSet2.name -> Seq(
            SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1), AttributeNumber(2)))),
                                      None,
                                      intArrayNameWithWfName
            )
          )
        )

      // attribute reference with 1 element array should resolve as AttributeValueList
      runAndWait(testResolveInputs(context, configSampleSet, sampleSet4, arrayWdl, this)) shouldBe
        Map(
          sampleSet4.name -> Seq(
            SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(101)))), None, intArrayNameWithWfName)
          )
        )

      // failure cases
      assertResult(true, "Missing values should return an error") {
        runAndWait(testResolveInputs(context, configGood, sampleMissingValue, littleWdl, this))
          .get("sampleMissingValue")
          .get match {
          case Seq(SubmissionValidationValue(None, Some(_), intArg)) if intArg == intArgNameWithWfName => true
        }
      }

      // MethodConfiguration config_namespace/configMissingExpr is missing definitions for these inputs: w1.t1.int_arg
      intercept[RawlsException] {
        runAndWait(testResolveInputs(context, configMissingExpr, sampleGood, littleWdl, this))
      }
    }

    "resolve empty lists into AttributeEmptyLists" in withLegacyConfigData {
      val context = workspace

      runAndWait(testResolveInputs(context, configEmptyArray, sampleSet2, arrayWdl, this)) shouldBe
        Map(
          sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueEmptyList), None, intArrayNameWithWfName))
        )
    }

    "resolve empty lists into empty Array in nested WDL Struct" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(
        testResolveInputs(context,
                          configNestedWdlStructWithEmptyList,
                          sampleForWdlStruct,
                          wdlStructInputWdlWithNestedStruct,
                          this
        )
      )
      val methodProps = resolvedInputs(sampleForWdlStruct.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[]},"id":101,"sample":"sample1","samples":[]}}"""
    }

    "unpack AttributeValueRawJson into WDL-arrays" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] =
        runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, doubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack array input expression with attribute reference into WDL-arrays" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] =
        runAndWait(testResolveInputs(context, configArrayWithAttrRef, sampleSet2, doubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[10,11,12],[1,2]]}"""
    }

    "correctly unpack wdl struct expression with attribute references containing 1 element array into WDL Struct input" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] =
        runAndWait(testResolveInputs(context, configWdlStruct, sampleForWdlStruct2, wdlStructInputWdl, this))
      val methodProps = resolvedInputs(sampleForWdlStruct2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"id":123,"sample":"sample1","samples":[101]}}"""
    }

    "correctly unpack nested wdl struct expression with attribute references containing 1 element array into WDL Struct input" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(
        testResolveInputs(context, configNestedWdlStruct, sampleForWdlStruct2, wdlStructInputWdlWithNestedStruct, this)
      )
      val methodProps = resolvedInputs(sampleForWdlStruct2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[101]},"id":123,"sample":"sample1","samples":[101]}}"""
    }

    "unpack wdl struct expression with attribute references into WDL Struct input" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] =
        runAndWait(testResolveInputs(context, configWdlStruct, sampleForWdlStruct, wdlStructInputWdl, this))
      val methodProps = resolvedInputs(sampleForWdlStruct.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"id":101,"sample":"sample1","samples":[1,2]}}"""
    }

    "unpack AttributeValueRawJson into optional WDL-arrays" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] =
        runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, optionalDoubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack nested Array into WDL Struct" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(
        testResolveInputs(context,
                          configNestedArrayWdlStruct,
                          sampleForWdlStruct,
                          wdlStructInputWdlWithNestedArray,
                          this
        )
      )
      val methodProps = resolvedInputs(sampleForWdlStruct.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[[0,1,2],[3,4,5]]},"id":101,"sample":"sample1","samples":[[0,1,2],[3,4,5]]}}"""
    }

    "unpack AttributeValueRawJson into lists-of WDL-arrays" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] =
        runAndWait(testResolveInputs(context, configRawJsonTripleArray, sampleSet2, tripleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aaint_array":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]}"""
    }

    "unpack triple Array into WDL Struct" in withLegacyConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(
        testResolveInputs(context,
                          configTripleArrayWdlStruct,
                          sampleForWdlStruct,
                          wdlStructInputWdlWithTripleArray,
                          this
        )
      )
      val methodProps = resolvedInputs(sampleForWdlStruct.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]},"id":101,"sample":"sample1","samples":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]}}"""
    }

    "cast attribute numbers into strings for string inputs" in withLegacyConfigData {
      val context = workspace
      runAndWait(testResolveInputs(context, configStringArgFromNumberAttribute, sampleGood, stringWdl, this)) shouldBe
        Map(
          sampleGood.name -> Seq(SubmissionValidationValue(Some(AttributeString("1")), None, stringArgNameWithWfName))
        )
    }

    "cast attribute numbers into strings for string inputs via a set" in withLegacyConfigData {
      val context = workspace
      runAndWait(
        testResolveInputs(context, configStringArgFromNumberAttributeViaSampleSet, sampleSet2, arrayStringWdl, this)
      ) shouldBe
        Map(
          sampleSet2.name -> Seq(
            SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeString("1"), AttributeString("2")))),
                                      None,
                                      strArrayNameWithWfName
            )
          )
        )
    }

    "accept multiple update operations for the same entity in batchUpsert" in withLocalEntityProviderTestDatabase {
      dataSource =>
        val workspaceContext = runAndWait(
          dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)
        ).get
        val localEntityProvider = new LocalEntityProvider(
          EntityRequestArguments(workspaceContext, testContext),
          slickDataSource,
          cacheEnabled = true,
          testConf.getDuration("entities.queryTimeout"),
          workbenchMetricBaseName
        )

        val multiUpsert = Seq(
          EntityUpdateDefinition("myname",
                                 "mytype",
                                 Seq(AddUpdateAttribute(AttributeName.withDefaultNS("one"), AttributeString("111")))
          ),
          EntityUpdateDefinition("myname",
                                 "mytype",
                                 Seq(AddUpdateAttribute(AttributeName.withDefaultNS("two"), AttributeString("222")))
          )
        )
        val writes = localEntityProvider.batchUpsertEntities(Source(multiUpsert), testContext).futureValue

        writes shouldBe 2

        val entityQuery = EntityQuery(1, 100, "name", SortDirections.Ascending, None)
        val parentContext = RawlsRequestContext(userInfo)
        val actual = localEntityProvider.queryEntities("mytype", entityQuery, parentContext).futureValue

        actual.resultMetadata.unfilteredCount shouldBe 1
        actual.results.size shouldBe 1
        actual.results.head shouldBe Entity("myname",
                                            "mytype",
                                            Map(AttributeName.withDefaultNS("one") -> AttributeString("111"),
                                                AttributeName.withDefaultNS("two") -> AttributeString("222")
                                            )
        )

        val withAllAttrs = runAndWait(
          dataSource.dataAccess.entityQueryWithInlineAttributes
            .findEntityByName(localEntityProviderTestData.workspace.workspaceIdAsUUID, "mytype", "myname")
            .result
        )

        withAllAttrs.size shouldBe 1
        val allAttrs = withAllAttrs.head.allAttributeValues.getOrElse("")
        allAttrs shouldBe "myname 111 222"
    }

  }

  "LocalEntityProvider case-sensitivity" should {
    "return helpful error message when upserting case-divergent entity names (createEntity method)" in withLocalEntityProviderTestDatabase {
      dataSource =>
        val workspaceContext = runAndWait(
          dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)
        ).get
        val localEntityProvider = new LocalEntityProvider(
          EntityRequestArguments(workspaceContext, testContext),
          slickDataSource,
          cacheEnabled = true,
          testConf.getDuration("entities.queryTimeout"),
          workbenchMetricBaseName
        )

        // create the first entity with name "myname"
        val entity1 = Entity("myname", "casetest", Map())
        val created1 = localEntityProvider.createEntity(entity1, testContext).futureValue
        created1 shouldBe entity1

        // attempt to create the second entity with name "MyName" - differing from entity1's name only in case
        val entity2 = Entity("MyName", "casetest", Map())
        val ex = recoverToExceptionIf[Exception] {
          localEntityProvider.createEntity(entity2, testContext)
        }.futureValue

        ex match {
          case er: RawlsExceptionWithErrorReport =>
            val expectedMessage =
              s"${entity2.entityType} ${entity2.name} already exists in ${workspaceContext.toWorkspaceName}"
            er.errorReport.message shouldBe expectedMessage
          case _ =>
            fail(
              s"expected a RawlsExceptionWithErrorReport, found ${ex.getClass.getName} with message '${ex.getMessage}''"
            )
        }
    }

    "return helpful error message when upserting case-divergent entity names (batchUpsertEntities method)" in withLocalEntityProviderTestDatabase {
      dataSource =>
        val workspaceContext = runAndWait(
          dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)
        ).get
        val localEntityProvider = new LocalEntityProvider(
          EntityRequestArguments(workspaceContext, testContext),
          slickDataSource,
          cacheEnabled = true,
          testConf.getDuration("entities.queryTimeout"),
          workbenchMetricBaseName
        )

        // create the first entity with name "myname"
        val upsert1 = Seq(EntityUpdateDefinition("myname", "casetest", Seq()))
        val created1 = localEntityProvider.batchUpsertEntities(Source(upsert1), testContext).futureValue
        created1 shouldBe 1

        // attempt to create the second entity with name "MyName" - differing from entity1's name only in case
        val upsert2 = Seq(EntityUpdateDefinition("MyName", "casetest", Seq()))
        val ex = recoverToExceptionIf[Exception] {
          localEntityProvider.batchUpsertEntities(Source(upsert2), testContext)
        }.futureValue

        ex match {
          case er: RawlsExceptionWithErrorReport =>
            val expectedMessage =
              "Database error occurred. Check if you are uploading entity names that differ only in case from pre-existing entities."
            er.errorReport.message shouldBe expectedMessage
          case _ =>
            fail(
              s"expected a RawlsExceptionWithErrorReport, found ${ex.getClass.getName} with message '${ex.getMessage}''"
            )
        }
    }
  }

}
