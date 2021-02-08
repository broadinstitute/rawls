package org.broadinstitute.dsde.rawls.entities.local

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction, TestDriverComponent}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigTestSupport
import org.broadinstitute.dsde.rawls.model.AttributeName.toDelimitedName
import org.broadinstitute.dsde.rawls.model.{AttributeNumber, AttributeValueEmptyList, AttributeValueList, Entity, EntityTypeMetadata, MethodConfiguration, SubmissionValidationValue, WDL, Workspace}

import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.sql.Timestamp
import java.util.UUID

class LocalEntityProviderSpec extends AnyWordSpecLike with Matchers with TestDriverComponent with MethodConfigTestSupport {
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

    "unpack AttributeValueRawJson into lists-of WDL-arrays" in withConfigData {
      val context = workspace

      val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configRawJsonTripleArray, sampleSet2, tripleArrayWdl, this))
      val methodProps = resolvedInputs(sampleSet2.name).map { svv: SubmissionValidationValue =>
        svv.inputName -> svv.value.get
      }
      val wdlInputs: String = methodConfigResolver.propertiesToWdlInputs(methodProps.toMap)

      wdlInputs shouldBe """{"w1.aaint_array":[[[0,1,2],[3,4,5]],[[3,4,5],[6,7,8]]]}"""
    }

    //The test data for the following entity cache tests are set up so that the cache will return results that are different
    //than would be returned by not using the cache. This will help us determine that we are correctly calling the cache or going
    //in for the full DB query

    val expectedResultWhenUsingCache = localEntityProviderTestData.workspaceEntityTypeCacheEntries.map { case (entityType, entityTypeCount) =>
      entityType -> EntityTypeMetadata(entityTypeCount, s"${entityType}_id", localEntityProviderTestData.workspaceAttrNameCacheEntries(entityType).map(attrName => toDelimitedName(attrName)))
    }

    val expectedResultWhenUsingFullQueries = expectedResultWhenUsingCache - localEntityProviderTestData.sample1.entityType

    "use cache for entityTypeMetadata when useCache=true, cache exists, and cache is up to date" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled =true)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(workspaceQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, new Timestamp(workspaceContext.lastModified.getMillis)))

      val entityTypeMetadataResult = runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(useCache =true)))

      val typeCountCache = runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))
      val attrNamesCache = runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))

      //assert that there is something in the cache for this workspace
      typeCountCache should not be Map.empty
      attrNamesCache should not be Map.empty

      entityTypeMetadataResult should contain theSameElementsAs expectedResultWhenUsingCache
    }

    "not use cache for entityTypeMetadata when useCache=true, cache exists, and cache is not up to date" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled =true)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(workspaceQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, new Timestamp(workspaceContext.lastModified.getMillis - 1)))

      val entityTypeMetadataResult = runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(useCache =true)))

      val typeCountCache = runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))
      val attrNamesCache = runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))

      //assert that there is something in the cache for this workspace
      typeCountCache should not be Map.empty
      attrNamesCache should not be Map.empty

      entityTypeMetadataResult should contain theSameElementsAs expectedResultWhenUsingFullQueries
    }

    "not use cache for entityTypeMetadata when useCache=true and the cache does not exist for the workspace" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled =true)

      //Update the entityCacheLastUpdated field to null to indicate that the cache has never been populated for this workspace
      runAndWait(workspaceQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, null))

      val entityTypeMetadataResult = runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(useCache = true)))

      //Remove any existing entity cache entries
      runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.deleteAllForWorkspace(workspaceContext.workspaceIdAsUUID))
      runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.deleteAllForWorkspace(workspaceContext.workspaceIdAsUUID))

      val typeCountCache = runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))
      val attrNamesCache = runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))

      //assert that there is nothing in the cache for this workspace
      typeCountCache shouldBe Map.empty
      attrNamesCache shouldBe Map.empty

      //if there's nothing in the cache, we can assert that the results we're getting below are _not_ from the cache
      entityTypeMetadataResult should contain theSameElementsAs expectedResultWhenUsingFullQueries
    }

    "not use cache for entityTypeMetadata when useCache=false even if cache is up to date and cache is enabled" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, cacheEnabled = true)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(workspaceQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, new Timestamp(workspaceContext.lastModified.getMillis)))

      val entityTypeMetadataResult = runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(useCache = false)))

      val typeCountCache = runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))
      val attrNamesCache = runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))

      //assert that there is something in the cache for this workspace
      typeCountCache should not be Map.empty
      attrNamesCache should not be Map.empty

      entityTypeMetadataResult should contain theSameElementsAs expectedResultWhenUsingFullQueries
    }

    "not use the cache when it's disabled even if cache is up to date and useCache=true" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, false)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(workspaceQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, new Timestamp(workspaceContext.lastModified.getMillis)))

      val entityTypeMetadataResult = runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(true)))

      val typeCountCache = runAndWait(dataSource.dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))
      val attrNamesCache = runAndWait(dataSource.dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID))

      //assert that there is something in the cache for this workspace
      typeCountCache should not be Map.empty
      attrNamesCache should not be Map.empty

      entityTypeMetadataResult should contain theSameElementsAs expectedResultWhenUsingFullQueries
    }

    "update the entity cache last updated field if we ask for the cache but the last updated field is null" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, true)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(workspaceQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, null))

      //Ask for the cache which won't exist yet, forcing the cacheLastUpdated timestamp to update
      runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(true)))

      val updatedWorkspaceRecord = runAndWait(workspaceQuery.findByIdQuery(workspaceContext.workspaceIdAsUUID).result)

      assert(updatedWorkspaceRecord.head.entityCacheLastUpdated.isDefined)
    }

    "not update the last updated field if the cache exists but is out of date" in withLocalEntityProviderTestDatabase { dataSource =>
      val workspaceContext = runAndWait(dataSource.dataAccess.workspaceQuery.findById(localEntityProviderTestData.workspace.workspaceId)).get
      val localEntityProvider = new LocalEntityProvider(workspaceContext, slickDataSource, true)

      val outdatedTimestmap = new Timestamp(workspaceContext.lastModified.getMillis - 1)

      //Update the entityCacheLastUpdated field to be identical to lastModified, so we can test our scenario of having a fresh cache
      runAndWait(workspaceQuery.updateCacheLastUpdated(workspaceContext.workspaceIdAsUUID, outdatedTimestmap))

      //Ask for the cache which will be out of date
      runAndWait(DBIO.from(localEntityProvider.entityTypeMetadata(true)))

      val latestWorkspaceRecordTimestamp = runAndWait(workspaceQuery.findByIdQuery(workspaceContext.workspaceIdAsUUID).result).head.entityCacheLastUpdated

      Option(outdatedTimestmap) shouldBe latestWorkspaceRecordTimestamp
    }

  }
}
