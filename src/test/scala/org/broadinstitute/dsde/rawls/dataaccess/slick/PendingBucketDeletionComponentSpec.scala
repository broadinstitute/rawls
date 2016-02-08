package org.broadinstitute.dsde.rawls.dataaccess.slick

/**
 * Created by dvoet on 2/3/16.
 */
class PendingBucketDeletionComponentSpec extends TestDriverComponent {

  "PendingBucketDeletionComponent" should "create, list and delete" in {
    val deletion = PendingBucketDeletion("foo")
    assertResult(Seq()) {
      runAndWait(pendingBucketDeletionQuery.list())
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

    assertResult(Seq()) {
      runAndWait(pendingBucketDeletionQuery.list())
    }
  }
}
