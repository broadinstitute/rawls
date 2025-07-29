package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.AttributeName

class CompactEntityKeysCacheSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  // shorthand vars for tests below to enhance readability
  private val wsid = minimalTestData.workspace.workspaceIdAsUUID
  private val ws2id = minimalTestData.workspace2.workspaceIdAsUUID
  private val q = compactEntityQuery

  // some attribute names used in tests
  private val attr1 = AttributeName.withDefaultNS("one")
  private val attr2 = AttributeName.withLibraryNS("two")
  private val attr3 = AttributeName.fromDelimitedName("three")
  private val attr4 = AttributeName.fromDelimitedName("namespace:four")
  private val attr5 = AttributeName.fromDelimitedName("other:five")
  private val attr6 = AttributeName.fromDelimitedName("six:six")

  behavior of "entity keys cache"

  it should "save and retrieve entity keys cache" in withMinimalTestDatabase { _ =>
    val cacheEntry1 = EntityTypeAndAttributeKeys("entityType1", Set(attr1, attr2))
    val cacheEntry2 = EntityTypeAndAttributeKeys("entityType2", Set(attr3, attr4))
    val cacheEntry3 = EntityTypeAndAttributeKeys("entityType3", Set(attr5, attr6))

    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntry1, cacheEntry2, cacheEntry3))) shouldBe 3

    // retrieve the cache
    val actual = runAndWait(q.getCachedKeys(wsid))
    actual should contain theSameElementsAs Seq(cacheEntry1, cacheEntry2, cacheEntry3)
  }

  it should "update existing cache entries" in withMinimalTestDatabase { _ =>
    val cacheEntry1 = EntityTypeAndAttributeKeys("entityType1", Set(attr1, attr2))
    val cacheEntry2 = EntityTypeAndAttributeKeys("entityType2", Set(attr3, attr4))
    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntry1, cacheEntry2))) shouldBe 2

    // validate first save
    val actual = runAndWait(q.getCachedKeys(wsid))
    actual should contain theSameElementsAs Seq(cacheEntry1, cacheEntry2)

    // update the cache for entityType1
    val updatedCacheEntry = EntityTypeAndAttributeKeys("entityType1", Set(attr5, attr6))
    // note this returns 2, not 1. When MySQL updates an existing row in
    // an "insert ... on duplicate key update" statement, it counts that as 2 rows affected.
    runAndWait(q.saveCache(wsid, Set(updatedCacheEntry))) shouldBe 2

    // validate the update
    val update = runAndWait(q.getCachedKeys(wsid))
    update should contain theSameElementsAs Seq(updatedCacheEntry, cacheEntry2)
  }

  it should "support invalidation of cache entries" in withMinimalTestDatabase { _ =>
    val cacheEntry1 = EntityTypeAndAttributeKeys("entityType1", Set(attr1, attr2))
    val cacheEntry2 = EntityTypeAndAttributeKeys("entityType2", Set(attr3, attr4))
    val cacheEntry3 = EntityTypeAndAttributeKeys("entityType3", Set(attr5, attr6))

    // save the cache
    runAndWait(q.saveCache(wsid, Set(cacheEntry1, cacheEntry2, cacheEntry3))) shouldBe 3

    // retrieve the cache
    val actual = runAndWait(q.getCachedKeys(wsid))
    actual should contain theSameElementsAs Seq(cacheEntry1, cacheEntry2, cacheEntry3)

    // invalidate the cache for entityType2
    runAndWait(q.invalidateCache(wsid, Set(cacheEntry2.entityType))) shouldBe 1

    // retrieve the cache
    val afterInvalidation = runAndWait(q.getCachedKeys(wsid))
    afterInvalidation should contain theSameElementsAs Seq(cacheEntry1, cacheEntry3)

  }

  it should "respect workspace boundaries for reads" in withMinimalTestDatabase { _ =>
    val cacheEntry1 = EntityTypeAndAttributeKeys("entityType1", Set(attr1, attr2))
    val cacheEntry2 = EntityTypeAndAttributeKeys("entityType2", Set(attr3, attr4))
    val cacheEntry3 = EntityTypeAndAttributeKeys("entityType3", Set(attr5, attr6))

    // save cacheEntry1 and cacheEntry2 to workspace 1
    runAndWait(q.saveCache(wsid, Set(cacheEntry1, cacheEntry2))) shouldBe 2

    // save cacheEntry3 to workspace 2
    runAndWait(q.saveCache(ws2id, Set(cacheEntry3))) shouldBe 1

    // retrieve the cache for workspace 1
    val actual1 = runAndWait(q.getCachedKeys(wsid))
    actual1 should contain theSameElementsAs Seq(cacheEntry1, cacheEntry2)

    // retrieve the cache for workspace 2
    val actual2 = runAndWait(q.getCachedKeys(ws2id))
    actual2 should contain theSameElementsAs Seq(cacheEntry3)
  }

  it should "respect workspace boundaries for invalidations" in withMinimalTestDatabase { _ =>
    val cacheEntry1 = EntityTypeAndAttributeKeys("entityType1", Set(attr1, attr2))
    val cacheEntry2 = EntityTypeAndAttributeKeys("entityType2", Set(attr3, attr4))
    val cacheEntry3 = EntityTypeAndAttributeKeys("entityType3", Set(attr5, attr6))

    // save all cache entries to both workspace 1 and workspace 2
    Seq(wsid, ws2id).foreach { workspaceId =>
      runAndWait(q.saveCache(workspaceId, Set(cacheEntry1, cacheEntry2, cacheEntry3))) shouldBe 3
      runAndWait(q.getCachedKeys(workspaceId)) should contain theSameElementsAs Seq(cacheEntry1,
                                                                                    cacheEntry2,
                                                                                    cacheEntry3
      )
    }
    // invalidate cacheEntry2 in workspace 1
    runAndWait(q.invalidateCache(wsid, cacheEntry2.entityType)) shouldBe 1

    // retrieve the cache for workspace 1
    val actual1 = runAndWait(q.getCachedKeys(wsid))
    actual1 should contain theSameElementsAs Seq(cacheEntry1, cacheEntry3)

    // retrieve the cache for workspace 2
    val actual2 = runAndWait(q.getCachedKeys(ws2id))
    actual2 should contain theSameElementsAs Seq(cacheEntry1, cacheEntry2, cacheEntry3)
  }

}
