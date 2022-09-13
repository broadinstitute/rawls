package org.broadinstitute.dsde.rawls.dataaccess.slick

/**
 * Created by dvoet on 2/3/16.
 */
class PendingBucketDeletionComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  "PendingBucketDeletionComponent" should "create, list and delete" in withEmptyTestDatabase {
    val deletion = PendingBucketDeletionRecord("foo")
    assertResult(Seq()) {
      runAndWait(pendingBucketDeletionQuery.list())
    }

    assertResult(deletion) {
      runAndWait(pendingBucketDeletionQuery.save(deletion))
    }

    assertResult(deletion) {
      runAndWait(pendingBucketDeletionQuery.save(deletion))
    }

    assertResult(Seq(deletion)) {
      runAndWait(pendingBucketDeletionQuery.list())
    }

    assertResult(1) {
      runAndWait(pendingBucketDeletionQuery.delete(deletion))
    }

    assertResult(0) {
      runAndWait(pendingBucketDeletionQuery.delete(deletion))
    }

    assertResult(Seq()) {
      runAndWait(pendingBucketDeletionQuery.list())
    }
  }
}
