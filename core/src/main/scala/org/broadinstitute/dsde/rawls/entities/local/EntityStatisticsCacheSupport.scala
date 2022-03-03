package org.broadinstitute.dsde.rawls.entities.local

import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.{AttributeName, EntityTypeMetadata, Workspace, WorkspaceFeatureFlag}
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.{traceDBIOWithParent, traceReadOnlyDBIOWithParent}

import java.sql.Timestamp
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * helper methods that deal with entity type metadata and its cache
  */
trait EntityStatisticsCacheSupport extends LazyLogging {

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
                                        outerSpan: Span = null): ReadWriteAction[Map[String, EntityTypeMetadata]] = {
    val typesAndCountsQ = typeCounts(dataAccess, countsFromCache, outerSpan)
    val typesAndAttrsQ = typeAttributes(dataAccess, attributesFromCache, outerSpan)
    traceDBIOWithParent[Map[String, EntityTypeMetadata]]("generateEntityMetadataMap", outerSpan) { innerSpan =>
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
  def opportunisticSaveEntityCache(metadata: Map[String, EntityTypeMetadata], dataAccess: DataAccess, outerSpan: Span = null) = {
    val entityTypesWithCounts: Map[String, Int] = metadata.map {
      case (typeName, typeMetadata) => typeName -> typeMetadata.count
    }
    val entityTypesWithAttrNames: Map[String, Seq[AttributeName]] = metadata.map {
      case (typeName, typeMetadata) => typeName -> typeMetadata.attributeNames.map(AttributeName.fromDelimitedName)
    }
    val timestamp: Timestamp = new Timestamp(workspaceContext.lastModified.getMillis)

    traceDBIOWithParent("generateEntityMetadataMap", outerSpan) { _ =>
      dataAccess.entityCacheManagementQuery.saveEntityCache(workspaceContext.workspaceIdAsUUID,
        entityTypesWithCounts, entityTypesWithAttrNames, timestamp).asTry.map {
        case Success(_) => // noop
        case Failure(ex) =>
          logger.warn(s"failed to opportunistically update the entity statistics cache: ${ex.getMessage}. " +
            s"The user's request was not impacted.")
      }
    }
  }

  /** wrapper for cache staleness lookup, includes performance tracing */
  def cacheStaleness(dataAccess: DataAccess, outerSpan: Span = null): ReadAction[Option[Int]] = {
    traceReadOnlyDBIOWithParent("entityCacheStaleness", outerSpan) { _ =>
      dataAccess.entityCacheQuery.entityCacheStaleness(workspaceContext.workspaceIdAsUUID).map { stalenessOpt =>
        // record the cache-staleness for this request
        // TODO: send to a metrics service that allows these values to be graphed/analyzed, instead of just logging
        stalenessOpt match {
          case Some(staleness) => logger.info(s"entity statistics cache staleness: $staleness")
          case None => logger.info(s"entity statistics cache staleness: n/a (cache does not exist)")
        }
        stalenessOpt
      }
    }
  }

  /** wrapper for workspace feature flag lookup, includes performance tracing */
  def cacheFeatureFlags(dataAccess: DataAccess, outerSpan: Span = null): ReadAction[CacheFeatureFlags] = {
    traceReadOnlyDBIOWithParent("getWorkspaceFeatureFlags", outerSpan) { _ =>
      dataAccess.workspaceFeatureFlagQuery.listFlagsForWorkspace(workspaceContext.workspaceIdAsUUID,
        List(FEATURE_ALWAYS_CACHE_TYPE_COUNTS, FEATURE_ALWAYS_CACHE_TYPE_ATTRIBUTES)).map { flags =>
        val alwaysCacheTypeCounts = flags.contains(FEATURE_ALWAYS_CACHE_TYPE_COUNTS)
        val alwaysCacheAttributes = flags.contains(FEATURE_ALWAYS_CACHE_TYPE_ATTRIBUTES)
        CacheFeatureFlags(alwaysCacheTypeCounts = alwaysCacheTypeCounts, alwaysCacheAttributes = alwaysCacheAttributes)
      }
    }
  }

  /** convenience method for retrieving type-counts, either from cache or from raw entities */
  def typeCounts(dataAccess: DataAccess, fromCache: Boolean, outerSpan: Span = null): ReadAction[Map[String, Int]] = {
    if (fromCache)
      cachedTypeCounts(dataAccess, outerSpan)
    else
      uncachedTypeCounts(dataAccess, outerSpan)
  }

  /** convenience method for retrieving type-attributes, either from cache or from raw attributes */
  def typeAttributes(dataAccess: DataAccess, fromCache: Boolean, outerSpan: Span = null): ReadAction[Map[String, Seq[AttributeName]]] = {
    if (fromCache)
      cachedTypeAttributes(dataAccess, outerSpan)
    else
      uncachedTypeAttributes(dataAccess, outerSpan)
  }

  /** wrapper for cached type-counts lookup, includes performance tracing */
  def cachedTypeCounts(dataAccess: DataAccess, outerSpan: Span = null): ReadAction[Map[String, Int]] = {
    traceReadOnlyDBIOWithParent("entityTypeStatisticsQuery.getAll", outerSpan) { _ =>
      dataAccess.entityTypeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID)
    }
  }

  /** wrapper for uncached type-counts lookup, includes performance tracing */
  def uncachedTypeCounts(dataAccess: DataAccess, outerSpan: Span = null): ReadAction[Map[String, Int]] = {
    traceReadOnlyDBIOWithParent("getEntityTypesWithCounts", outerSpan) { _ =>
      dataAccess.entityQuery.getEntityTypesWithCounts(workspaceContext.workspaceIdAsUUID)
    }
  }

  /** wrapper for cached type-attributes lookup, includes performance tracing */
  def cachedTypeAttributes(dataAccess: DataAccess, outerSpan: Span = null): ReadAction[Map[String, Seq[AttributeName]]] = {
    traceReadOnlyDBIOWithParent("entityAttributeStatisticsQuery.getAll", outerSpan) { _ =>
      dataAccess.entityAttributeStatisticsQuery.getAll(workspaceContext.workspaceIdAsUUID)
    }
  }

  /** wrapper for uncached type-attributes lookup, includes performance tracing */
  def uncachedTypeAttributes(dataAccess: DataAccess, outerSpan: Span = null): ReadAction[Map[String, Seq[AttributeName]]] = {
    traceReadOnlyDBIOWithParent("getAttrNamesAndEntityTypes", outerSpan) { _ =>
      dataAccess.entityQuery.getAttrNamesAndEntityTypes(workspaceContext.workspaceIdAsUUID)
    }
  }

}
