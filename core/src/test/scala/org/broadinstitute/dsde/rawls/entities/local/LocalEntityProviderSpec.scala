package org.broadinstitute.dsde.rawls.entities.local

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction, TestDriverComponent}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigTestSupport
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{AttributeNumber, AttributeValueEmptyList, AttributeValueList, Entity, EntityTypeMetadata, MethodConfiguration, SubmissionValidationValue, WDL, Workspace}
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.scalatest.RecoverMethods.recoverToExceptionIf
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.sql.Timestamp
import java.time.Instant
import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext

class LocalEntityProviderSpec extends AnyWordSpecLike with Matchers with ScalaFutures with TestDriverComponent with MethodConfigTestSupport {
  import driver.api._

  val testConf = ConfigFactory.load()

  //Test harness to call resolveInputsForEntities without having to go via the WorkspaceService
  def testResolveInputs(workspaceContext: Workspace, methodConfig: MethodConfiguration, entity: Entity, wdl: WDL, dataAccess: DataAccess)
                       (implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {

    val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, testConf.getBoolean("entityStatisticsCache.enabled"))

    dataAccess.entityQuery.findEntityByName(workspaceContext.workspaceIdAsUUID, entity.entityType, entity.name).result flatMap { entityRecs =>
      methodConfigResolver.gatherInputs(userInfo, methodConfig, wdl) match {
        case scala.util.Failure(exception) =>
          DBIO.failed(exception)
        case scala.util.Success(gatherInputsResult: GatherInputsResult)
          if gatherInputsResult.extraInputs.nonEmpty || gatherInputsResult.missingInputs.nonEmpty =>
          DBIO.failed(new RawlsException(s"gatherInputsResult has missing or extra inputs: $gatherInputsResult"))
        case scala.util.Success(gatherInputsResult: GatherInputsResult) =>
          localEntityProvider.evaluateExpressionsInternal(workspaceContext, gatherInputsResult.processableInputs, Some(entityRecs), dataAccess)
      }
    }
  }

  "LocalEntityProvider" should {
    "resolve method config inputs" in withConfigData {
      val context = workspace

      runAndWait(testResolveInputs(context, configGood, sampleGood, littleWdl, this)) shouldBe
        Map(sampleGood.name -> Seq(SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName)))

      runAndWait(testResolveInputs(context, configEvenBetter, sampleGood, littleWdl, this)) shouldBe
        Map(sampleGood.name -> Seq(SubmissionValidationValue(Some(AttributeNumber(1)), None, intArgNameWithWfName), SubmissionValidationValue(Some(AttributeNumber(1)), None, intOptNameWithWfName)))

      runAndWait(testResolveInputs(context, configSampleSet, sampleSet, arrayWdl, this)) shouldBe
        Map(sampleSet.name -> Seq(SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1)))), None, intArrayNameWithWfName)))

      runAndWait(testResolveInputs(context, configSampleSet, sampleSet2, arrayWdl, this)) shouldBe
        Map(sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(1), AttributeNumber(2)))), None, intArrayNameWithWfName)))

      // attribute reference with 1 element array should resolve as AttributeValueList
      runAndWait(testResolveInputs(context, configSampleSet, sampleSet4, arrayWdl, this)) shouldBe
        Map(sampleSet4.name -> Seq(SubmissionValidationValue(Some(AttributeValueList(Seq(AttributeNumber(101)))), None, intArrayNameWithWfName)))

      // failure cases
      assertResult(true, "Missing values should return an error") {
        runAndWait(testResolveInputs(context, configGood, sampleMissingValue, littleWdl, this)).get("sampleMissingValue").get match {
          case Seq(SubmissionValidationValue(None, Some(_), intArg)) if intArg == intArgNameWithWfName => true
        }
      }

      //MethodConfiguration config_namespace/configMissingExpr is missing definitions for these inputs: w1.t1.int_arg
      intercept[RawlsException] {
        runAndWait(testResolveInputs(context, configMissingExpr, sampleGood, littleWdl, this))
      }
    }

    "resolve empty lists into AttributeEmptyLists" in withConfigData {
      val context = workspace

      runAndWait(testResolveInputs(context, configEmptyArray, sampleSet2, arrayWdl, this)) shouldBe
        Map(sampleSet2.name -> Seq(SubmissionValidationValue(Some(AttributeValueEmptyList), None, intArrayNameWithWfName)))
    }

    "resolve empty lists into empty Array in nested WDL Struct" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configNestedWdlStructWithEmptyList, sampleForWdlStruct, wdlStructInputWdlWithNestedStruct, this))
      val methodProps = resolvedInputs(sampleForWdlStruct.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[]},"id":101,"sample":"sample1","samples":[]}}"""
    }

    "unpack AttributeValueRawJson into WDL-arrays" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, doubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack array input expression with attribute reference into WDL-arrays" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configArrayWithAttrRef, sampleSet2, doubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[10,11,12],[1,2]]}"""
    }

    "correctly unpack wdl struct expression with attribute references containing 1 element array into WDL Struct input" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configWdlStruct, sampleForWdlStruct2, wdlStructInputWdl, this))
      val methodProps = resolvedInputs(sampleForWdlStruct2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"id":123,"sample":"sample1","samples":[101]}}"""
    }

    "correctly unpack nested wdl struct expression with attribute references containing 1 element array into WDL Struct input" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configNestedWdlStruct, sampleForWdlStruct2, wdlStructInputWdlWithNestedStruct, this))
      val methodProps = resolvedInputs(sampleForWdlStruct2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[101]},"id":123,"sample":"sample1","samples":[101]}}"""
    }

    "unpack wdl struct expression with attribute references into WDL Struct input" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configWdlStruct, sampleForWdlStruct, wdlStructInputWdl, this))
      val methodProps = resolvedInputs(sampleForWdlStruct.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"id":101,"sample":"sample1","samples":[1,2]}}"""
    }

    "unpack AttributeValueRawJson into optional WDL-arrays" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonDoubleArray, sampleSet2, optionalDoubleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aint_array":[[0,1,2],[3,4,5]]}"""
    }

    "unpack nested Array into WDL Struct" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configNestedArrayWdlStruct, sampleForWdlStruct, wdlStructInputWdlWithNestedArray, this))
      val methodProps = resolvedInputs(sampleForWdlStruct.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[[0,1,2],[3,4,5]]},"id":101,"sample":"sample1","samples":[[0,1,2],[3,4,5]]}}"""
    }

    "unpack AttributeValueRawJson into lists-of WDL-arrays" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonTripleArray, sampleSet2, tripleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aaint_array":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]}"""
    }

    "unpack triple Array into WDL Struct" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configTripleArrayWdlStruct, sampleForWdlStruct, wdlStructInputWdlWithTripleArray, this))
      val methodProps = resolvedInputs(sampleForWdlStruct.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"wdlStructWf.obj":{"foo":{"bar":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]},"id":101,"sample":"sample1","samples":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]}}"""
    }

    //The test data for the following entity cache tests are set up so that the cache will return results that are different
    //than would be returned by not using the cache. This will help us determine that we are correctly calling the cache or going
    //in for the full DB query
  }

  "LocalEntityProvider Entity Statistics Cache feature" should {

    val expectedResultWhenUsingCache = localEntityProviderTestData.workspaceEntityTypeCacheEntries.map { case (entityType, entityTypeCount) =>
      entityType -> EntityTypeMetadata(entityTypeCount, s"${entityType}_id", localEntityProviderTestData.workspaceAttrNameCacheEntries(entityType).map(attrName => toDelimitedName(attrName)))
    }

    val expectedResultWhenUsingFullQueries = expectedResultWhenUsingCache - localEntityProviderTestData.sample1.entityType

    "use cache for entityTypeMetadata when useCache=true, cache is up to date, and cache is enabled" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled = true)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(entityCacheQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, new Timestamp(workspaceContext.lastModified.getMillis)))

      val entityTypeMetadataResult = runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(useCache = true)))

      val typeCountCache = runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))
      val attrNamesCache = runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))

      //assert that there is something in the cache for this workspace
      typeCountCache should not be Map.empty
      attrNamesCache should not be Map.empty

      entityTypeMetadataResult should contain theSameElementsAs expectedResultWhenUsingCache
    }

    "use cache for entityTypeMetadata when useCache=true, cache is not up to date, and cache is enabled" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled = true)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(entityCacheQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, new Timestamp(workspaceContext.lastModified.getMillis - 1)))

      val entityTypeMetadataResult = runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(useCache = true)))

      val typeCountCache = runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))
      val attrNamesCache = runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))

      //assert that there is something in the cache for this workspace
      typeCountCache should not be Map.empty
      attrNamesCache should not be Map.empty

      entityTypeMetadataResult should contain theSameElementsAs expectedResultWhenUsingCache
    }

    "not use cache for entityTypeMetadata when useCache=false even if cache is up to date and cache is enabled" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled = true)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(entityCacheQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, new Timestamp(workspaceContext.lastModified.getMillis)))

      val entityTypeMetadataResult = runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(useCache = false)))

      val typeCountCache = runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))
      val attrNamesCache = runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))

      //assert that there is something in the cache for this workspace
      typeCountCache should not be Map.empty
      attrNamesCache should not be Map.empty

      entityTypeMetadataResult should contain theSameElementsAs expectedResultWhenUsingFullQueries
    }

    "not use cache for entityTypeMetadata when it's disabled at the application level, even if cache is up to date and useCache=true" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, false)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(entityCacheQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, new Timestamp(workspaceContext.lastModified.getMillis)))

      val entityTypeMetadataResult = runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(true)))

      val typeCountCache = runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))
      val attrNamesCache = runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))

      //assert that there is something in the cache for this workspace
      typeCountCache should not be Map.empty
      attrNamesCache should not be Map.empty

      entityTypeMetadataResult should contain theSameElementsAs expectedResultWhenUsingFullQueries
    }

    // =========== START isEntityCacheCurrent() tests; these are obsolete
    "consider cache out of date if no cache record" in withLocalEntityProviderTestDatabase { _ =>
      val wsid = localEntityProviderTestData.workspace.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)

      withClue("cache record should not exist before updating") {
        assert(!runAndWait(workspaceFilter.exists.result))
      }

      val isCurrent = runAndWait(entityCacheQuery.isEntityCacheCurrent(wsid))
      withClue("cache should be out of date") {
        assert(!isCurrent)
      }
    }

    "consider cache out of date if cache record exists but is old" in withLocalEntityProviderTestDatabase { _ =>
      val wsid = localEntityProviderTestData.workspace.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)
      val wsLastModifiedTimestamp = Timestamp.from(Instant.ofEpochMilli(localEntityProviderTestData.workspace.lastModified.getMillis - 10000))

      withClue("cache record should not exist before updating") {
        assert(!runAndWait(workspaceFilter.exists.result))
      }

      // update cache timestamp
      runAndWait(entityCacheQuery.updateCacheLastUpdated(wsid, wsLastModifiedTimestamp))

      val isCurrent = runAndWait(entityCacheQuery.isEntityCacheCurrent(wsid))
      withClue("cache should be out of date") {
        assert(!isCurrent)
      }
    }

    "consider cache to be current if cache record exists and is equal to workspace last-modified" in withLocalEntityProviderTestDatabase { da =>
      val wsid = localEntityProviderTestData.workspace.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)

      withClue("cache record should not exist before updating") {
        assert(!runAndWait(workspaceFilter.exists.result))
      }

      val existingWorkspace = runAndWait(workspaceQuery.findById(wsid.toString))
      existingWorkspace should not be empty
      val wsLastModifiedTimestamp = new Timestamp(existingWorkspace.get.lastModified.getMillis)

      // update cache timestamp
      runAndWait(entityCacheQuery.updateCacheLastUpdated(wsid, wsLastModifiedTimestamp))

      val isCurrent = runAndWait(entityCacheQuery.isEntityCacheCurrent(wsid))
      withClue("cache should be current") {
        assert(isCurrent)
      }
    }
    // =========== END isEntityCacheCurrent() tests; these are obsolete

    "return None from entityCacheStaleness if cache is non-existent" in withLocalEntityProviderTestDatabase { _ =>
      val wsid = localEntityProviderTestData.workspace.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)

      withClue("cache record should not exist before updating") {
        assert(!runAndWait(workspaceFilter.exists.result))
      }

      val staleness = runAndWait(entityCacheQuery.entityCacheStaleness(wsid))
      withClue("staleness value should be None for non-existent caches") {
        staleness shouldBe empty
      }
    }
    "return Some(positive integer) from entityCacheStaleness if cache exists but is stale" in withLocalEntityProviderTestDatabase { _ =>
      val wsid = localEntityProviderTestData.workspace.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)
      val wsLastModifiedTimestamp = Timestamp.from(Instant.ofEpochMilli(localEntityProviderTestData.workspace.lastModified.getMillis - 10000))

      withClue("cache record should not exist before updating") {
        assert(!runAndWait(workspaceFilter.exists.result))
      }

      // update cache timestamp
      runAndWait(entityCacheQuery.updateCacheLastUpdated(wsid, wsLastModifiedTimestamp))

      val staleness = runAndWait(entityCacheQuery.entityCacheStaleness(wsid))
      withClue(s"staleness value of [$staleness] should contain a positive integer") {
        staleness match {
          case Some(n) => n should be > 0
          case None => fail("found None")
        }
      }
    }
    "return Some(0) from entityCacheStaleness if cache is up-to-date" in withLocalEntityProviderTestDatabase { da =>
      val wsid = localEntityProviderTestData.workspace.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)

      withClue("cache record should not exist before updating") {
        assert(!runAndWait(workspaceFilter.exists.result))
      }

      val existingWorkspace = runAndWait(workspaceQuery.findById(wsid.toString))
      existingWorkspace should not be empty
      val wsLastModifiedTimestamp = new Timestamp(existingWorkspace.get.lastModified.getMillis)

      // update cache timestamp
      runAndWait(entityCacheQuery.updateCacheLastUpdated(wsid, wsLastModifiedTimestamp))

      val staleness = runAndWait(entityCacheQuery.entityCacheStaleness(wsid))
      withClue("staleness value should be Some(0) for up-to-date caches") {
        staleness should contain (0)
      }
    }

    "insert cache record when updating if non-existent" in withLocalEntityProviderTestDatabase { _ =>
      val wsid = localEntityProviderTestData.workspace.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)
      val expectedTimestamp = Timestamp.from(Instant.now())

      withClue("cache record should not exist before updating") {
        assert(!runAndWait(workspaceFilter.exists.result))
      }

      // update cache timestamp
      runAndWait(entityCacheQuery.updateCacheLastUpdated(wsid, expectedTimestamp))

      withClue("cache record should exist after updating") {
        assert(runAndWait(workspaceFilter.exists.result))
      }

      val actualTimestamp = runAndWait(uniqueResult(workspaceFilter.map(_.entityCacheLastUpdated).result))

      withClue("actual timestamp should match expected timestamp after updating") {
        actualTimestamp should contain(expectedTimestamp)
      }
    }

    "update cache record when updating if existent" in withLocalEntityProviderTestDatabase { _ =>
      val wsid = localEntityProviderTestData.workspace.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)

      val firstTimestamp = Timestamp.valueOf("2000-01-01 12:34:56")
      val secondTimestamp = Timestamp.valueOf("2020-09-09 01:23:45")

      withClue("cache record should not exist before updating") {
        assert(!runAndWait(workspaceFilter.exists.result))
      }

      // update cache timestamp - should cause insert
      runAndWait(entityCacheQuery.updateCacheLastUpdated(wsid, firstTimestamp))

      withClue("cache record should exist after updating") {
        assert(runAndWait(workspaceFilter.exists.result))
      }

      val actualTimestamp = runAndWait(uniqueResult(workspaceFilter.map(_.entityCacheLastUpdated).result))

      withClue("actual timestamp should match expected timestamp after updating") {
        actualTimestamp should contain(firstTimestamp)
      }

      // update cache timestamp - should cause update
      runAndWait(entityCacheQuery.updateCacheLastUpdated(wsid, secondTimestamp))
      val actualUpdatedTimestamp = runAndWait(uniqueResult(workspaceFilter.map(_.entityCacheLastUpdated).result))

      withClue("actual timestamp should match expected timestamp after second update") {
        actualUpdatedTimestamp should contain(secondTimestamp)
      }
    }

    "nullify error message if cache update succeeds after previous failure" in withLocalEntityProviderTestDatabase { _ =>
      val wsid = localEntityProviderTestData.workspace.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)
      val expectedErrorMsg = "intentional error message"

      // save a cache entry that includes a failure
      runAndWait(entityCacheQuery.updateCacheLastUpdated(wsid, EntityStatisticsCacheMonitor.MIN_CACHE_TIME, Some(expectedErrorMsg)))

      val actualMessage = runAndWait(uniqueResult[Option[String]](workspaceFilter.map(_.errorMessage))).flatten
      actualMessage should contain(expectedErrorMsg)

      // save a cache entry that is successful
      runAndWait(entityCacheQuery.updateCacheLastUpdated(wsid, Timestamp.from(Instant.now())))

      val secondMessage = runAndWait(uniqueResult[Option[String]](workspaceFilter.map(_.errorMessage))).flatten
      secondMessage shouldBe empty
    }

    // temporarily disabled until we re-implement opportunistic cache update
    "opportunistically update cache if user requests metadata while cache is out of date" ignore withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled = true)
      val wsid = workspaceContext.workspaceIdAsUUID
      val workspaceFilter = entityCacheQuery.filter(_.workspaceId === wsid)

      withClue("cache record should not exist before requesting metadata") {
        assert(!runAndWait(workspaceFilter.exists.result))
      }

      val isCurrentBefore = runAndWait(entityCacheQuery.isEntityCacheCurrent(wsid))
      withClue("cache should be not-current before requesting metadata") {
        assert(!isCurrentBefore)
      }

      // requesting metadata should update the cache as a side effect
      localEntityProvider.entityTypeMetadata(true).futureValue

      withClue("cache record should exist after requesting metadata") {
        assert(runAndWait(workspaceFilter.exists.result))
      }

      val isCurrentAfter = runAndWait(entityCacheQuery.isEntityCacheCurrent(wsid))
      withClue("cache should be current after requesting metadata") {
        assert(isCurrentAfter)
      }
    }
  }

  "return helpful error message when upserting case-divergent entity names (createEntity method)" in withLocalEntityProviderTestDatabase { dataSource =>
    val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
    val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled = true)

    // create the first entity with name "myname"
    val entity1 = Entity("myname", "casetest", Map())
    val created1 = localEntityProvider.createEntity(entity1).futureValue
    created1 shouldBe entity1

    // attempt to create the second entity with name "MyName" - differing from entity1's name only in case
    val entity2 = Entity("MyName", "casetest", Map())
    val ex = recoverToExceptionIf[Exception] {
      localEntityProvider.createEntity(entity2)
    }.futureValue

    ex match {
      case er:RawlsExceptionWithErrorReport =>
        val expectedMessage = s"${entity2.entityType} ${entity2.name} already exists in ${workspaceContext.toWorkspaceName}"
        er.errorReport.message shouldBe expectedMessage
      case _ => fail(s"expected a RawlsExceptionWithErrorReport, found ${ex.getClass.getName} with message '${ex.getMessage}''")
    }
  }

  "return helpful error message when upserting case-divergent entity names (batchUpsertEntities method)" in withLocalEntityProviderTestDatabase { dataSource =>
    val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
    val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled = true)

    // create the first entity with name "myname"
    val upsert1 = Seq(EntityUpdateDefinition("myname", "casetest", Seq()))
    val created1 = localEntityProvider.batchUpsertEntities(upsert1).futureValue
    created1.size shouldBe 1

    // attempt to create the second entity with name "MyName" - differing from entity1's name only in case
    val upsert2 = Seq(EntityUpdateDefinition("MyName", "casetest", Seq()))
    val ex = recoverToExceptionIf[Exception] {
      localEntityProvider.batchUpsertEntities(upsert2)
    }.futureValue

    ex match {
      case er:RawlsExceptionWithErrorReport =>
        val expectedMessage = "Database error occurred. Check if you are uploading entity names or entity types that differ only in case from pre-existing entities."
        er.errorReport.message shouldBe expectedMessage
      case _ => fail(s"expected a RawlsExceptionWithErrorReport, found ${ex.getClass.getName} with message '${ex.getMessage}''")
    }
  }

  "return helpful error message when upserting case-divergent type names (createEntity method)" in withLocalEntityProviderTestDatabase { dataSource =>
    val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
    val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled = true)

    // create the first entity with type "casetest"
    val entity1 = Entity("myname", "casetest", Map())
    val created1 = localEntityProvider.createEntity(entity1).futureValue
    created1 shouldBe entity1

    // attempt to create the second entity with type "CaseTest" - differing from entity1's type only in case
    val entity2 = Entity("myname", "CaseTest", Map())
    val ex = recoverToExceptionIf[Exception] {
      localEntityProvider.createEntity(entity2)
    }.futureValue

    ex match {
      case er:RawlsExceptionWithErrorReport =>
        val expectedMessage = s"${entity2.entityType} ${entity2.name} already exists in ${workspaceContext.toWorkspaceName}"
        er.errorReport.message shouldBe expectedMessage
      case _ => fail(s"expected a RawlsExceptionWithErrorReport, found ${ex.getClass.getName} with message '${ex.getMessage}''")
    }
  }

  "return helpful error message when upserting case-divergent type names (batchUpsertEntities method)" in withLocalEntityProviderTestDatabase { dataSource =>
    val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
    val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled = true)

    // create the first entity with type "casetest"
    val upsert1 = Seq(EntityUpdateDefinition("myname", "casetest", Seq()))
    val created1 = localEntityProvider.batchUpsertEntities(upsert1).futureValue
    created1.size shouldBe 1

    // attempt to create the second entity with type "CaseTest" - differing from entity1's type only in case
    val upsert2 = Seq(EntityUpdateDefinition("myname", "CaseTest", Seq()))
    val ex = recoverToExceptionIf[Exception] {
      localEntityProvider.batchUpsertEntities(upsert2)
    }.futureValue

    ex match {
      case er:RawlsExceptionWithErrorReport =>
        val expectedMessage = "Database error occurred. Check if you are uploading entity names or entity types that differ only in case from pre-existing entities."
        er.errorReport.message shouldBe expectedMessage
      case _ => fail(s"expected a RawlsExceptionWithErrorReport, found ${ex.getClass.getName} with message '${ex.getMessage}''")
    }
  }

}
