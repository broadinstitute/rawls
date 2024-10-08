package org.broadinstitute.dsde.rawls.entities.local

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.time.StopWatch
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  EntityTypeMetadata,
  RawlsRequestContext,
  Workspace,
  WorkspaceFeatureFlag
}
import org.broadinstitute.dsde.rawls.util.TracingUtils._

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * helper methods that deal with entity type metadata and its cache
  */
trait EntityStatisticsCacheSupport extends LazyLogging with RawlsInstrumented {

  implicit protected val executionContext: ExecutionContext
  protected val dataSource: SlickDataSource
  val workspaceContext: Workspace

  import dataSource.dataAccess.driver.api._

  val FEATURE_ALWAYS_CACHE_TYPE_COUNTS = WorkspaceFeatureFlag("alwaysCacheTypeCounts")
  val FEATURE_ALWAYS_CACHE_TYPE_ATTRIBUTES = WorkspaceFeatureFlag("alwaysCacheAttributes")

  case class CacheFeatureFlags(alwaysCacheTypeCounts: Boolean, alwaysCacheAttributes: Boolean)

  /** convenience method for querying and then assembling an entity type metadata response.
    * if this method retrieves metadata directly from entities and attributes (i.e. not from cache),
    * it will update the cache with the results it found.
    * */
  def calculateMetadataResponse(dataAccess: DataAccess,
                                countsFromCache: Boolean,
                                attributesFromCache: Boolean,
                                parentContext: RawlsRequestContext
  ): ReadWriteAction[Map[String, EntityTypeMetadata]] = {
    val typesAndCountsQ = typeCounts(dataAccess, countsFromCache, parentContext)
    val typesAndAttrsQ = typeAttributes(dataAccess, attributesFromCache, parentContext)
    traceDBIOWithParent("generateEntityMetadataMap", parentContext) { innerSpan =>
      dataAccess.entityQuery.generateEntityMetadataMap(typesAndCountsQ, typesAndAttrsQ).flatMap { metadata =>
        // and opportunistically save, if we have bypassed cache for all components
        val saveCacheAction = if (countsFromCache || attributesFromCache) {
          DBIO.successful(())
        } else {
          opportunisticSaveEntityCache(metadata, dataAccess, innerSpan)
        }
        saveCacheAction.map(_ => metadata)
      }
    }
  }

  /** convenience method for writing back to cache, if we have already calculated a response from outside the cache */
  def opportunisticSaveEntityCache(metadata: Map[String, EntityTypeMetadata],
                                   dataAccess: DataAccess,
                                   parentContext: RawlsRequestContext
  ) = {
    val entityTypesWithCounts: Map[String, Int] = metadata.map { case (typeName, typeMetadata) =>
      typeName -> typeMetadata.count
    }
    val entityTypesWithAttrNames: Map[String, Seq[AttributeName]] = metadata.map { case (typeName, typeMetadata) =>
      typeName -> typeMetadata.attributeNames.map(AttributeName.fromDelimitedName)
    }
    val timestamp: Timestamp = new Timestamp(workspaceContext.lastModified.getMillis)

    traceDBIOWithParent("generateEntityMetadataMap", parentContext) { _ =>
      dataAccess.entityCacheManagementQuery
        .saveEntityCache(workspaceContext.workspaceIdAsUUID, entityTypesWithCounts, entityTypesWithAttrNames, timestamp)
        .asTry
        .map {
          case Success(_) => opportunisticEntityCacheSaveCounter.inc()
          case Failure(ex) =>
            logger.warn(
              s"failed to opportunistically update the entity statistics cache: ${ex.getMessage}. " +
                s"The user's request was not impacted."
            )
        }
    }
  }

  /** wrapper for cache staleness lookup, includes performance tracing */
  def cacheStaleness(dataAccess: DataAccess, parentContext: RawlsRequestContext): ReadAction[Option[Int]] =
    traceDBIOWithParent("entityCacheStaleness", parentContext) { _ =>
      dataAccess.entityCacheQuery.entityCacheStaleness(workspaceContext.workspaceIdAsUUID).map { stalenessOpt =>
        // record the cache-staleness for this request
        stalenessOpt match {
          case Some(staleness) =>
            logger
              .info(f"entity statistics cache staleness for workspaceId ${workspaceContext.workspaceId}: $staleness")
            entityCacheStaleness.update(staleness, TimeUnit.SECONDS)
          case None => logger.info(s"entity statistics cache staleness: n/a (cache does not exist)")
        }
        stalenessOpt
      }
    }

  /** wrapper for workspace feature flag lookup, includes performance tracing */
  def cacheFeatureFlags(dataAccess: DataAccess, parentContext: RawlsRequestContext): ReadAction[CacheFeatureFlags] =
    traceDBIOWithParent("getWorkspaceFeatureFlags", parentContext) { _ =>
      dataAccess.workspaceFeatureFlagQuery
        .listFlagsForWorkspace(workspaceContext.workspaceIdAsUUID,
                               List(FEATURE_ALWAYS_CACHE_TYPE_COUNTS, FEATURE_ALWAYS_CACHE_TYPE_ATTRIBUTES)
        )
        .map { flags =>
          val alwaysCacheTypeCounts = flags.contains(FEATURE_ALWAYS_CACHE_TYPE_COUNTS)
          val alwaysCacheAttributes = flags.contains(FEATURE_ALWAYS_CACHE_TYPE_ATTRIBUTES)
          CacheFeatureFlags(alwaysCacheTypeCounts = alwaysCacheTypeCounts,
                            alwaysCacheAttributes = alwaysCacheAttributes
          )
        }
    }

  /** convenience method for retrieving type-counts, either from cache or from raw entities */
  def typeCounts(dataAccess: DataAccess,
                 fromCache: Boolean,
                 parentContext: RawlsRequestContext
  ): ReadAction[Map[String, Int]] =
    if (fromCache)
      cachedTypeCounts(dataAccess, parentContext)
    else
      uncachedTypeCounts(dataAccess, parentContext)

  /** convenience method for retrieving type-attributes, either from cache or from raw attributes */
  def typeAttributes(dataAccess: DataAccess,
                     fromCache: Boolean,
                     parentContext: RawlsRequestContext
  ): ReadAction[Map[String, Seq[AttributeName]]] =
    if (fromCache)
      cachedTypeAttributes(dataAccess, parentContext)
    else
      uncachedTypeAttributes(dataAccess, parentContext)

  /** wrapper for cached type-counts lookup, includes performance tracing */
  def cachedTypeCounts(dataAccess: DataAccess, parentContext: RawlsRequestContext): ReadAction[Map[String, Int]] =
    traceDBIOWithParent("entityTypeStatisticsQuery.getAll", parentContext) { _ =>
      dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID)
    }

  /** wrapper for uncached type-counts lookup, includes performance tracing */
  def uncachedTypeCounts(dataAccess: DataAccess, parentContext: RawlsRequestContext): ReadAction[Map[String, Int]] =
    traceDBIOWithParent("getEntityTypesWithCounts", parentContext) { _ =>
      dataAccess.entityQuery.getEntityTypesWithCounts(workspaceContext.workspaceIdAsUUID)
    }

  /** wrapper for cached type-attributes lookup, includes performance tracing */
  def cachedTypeAttributes(dataAccess: DataAccess,
                           parentContext: RawlsRequestContext
  ): ReadAction[Map[String, Seq[AttributeName]]] =
    traceDBIOWithParent("entityAttributeStatisticsQuery.getAll", parentContext) { _ =>
      dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID)
    }

  /** wrapper for uncached type-attributes lookup, includes performance tracing */
  def uncachedTypeAttributes(dataAccess: DataAccess,
                             parentContext: RawlsRequestContext
  ): ReadAction[Map[String, Seq[AttributeName]]] = {
    val stopwatch = StopWatch.createStarted()
    traceDBIOWithParent("getAttrNamesAndEntityTypes", parentContext) { _ =>
      dataAccess.entityQuery.getAttrNamesAndEntityTypes(workspaceContext.workspaceIdAsUUID) map { result =>
        stopwatch.stop()
        logger.info(s"***** getAttrNamesAndEntityTypes complete in ${stopwatch.getTime}ms")
        result
      }
    }
  }

}
