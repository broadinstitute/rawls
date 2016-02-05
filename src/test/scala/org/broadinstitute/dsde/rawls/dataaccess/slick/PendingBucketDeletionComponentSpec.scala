package org.broadinstitute.dsde.rawls.dataaccess.slick

/**
 * Created by dvoet on 2/3/16.
 */
class PendingBucketDeletionComponentSpec extends TestDriverComponent {

  "PendingBucketDeletionComponent" should "create, list and delete" in {
    val deletion = PendingBucketDeletion("foo")
    assertResult(Seq()) {
      runAndWait(listPendingBucketDeletion())
    }

    assertResult(deletion) {
      runAndWait(savePendingBucketDeletion(deletion))
    }

    assertResult(Seq(deletion)) {
      runAndWait(listPendingBucketDeletion())
    }

    assertResult(1) {
      runAndWait(deletePendingBucketDeletion(deletion))
    }

    assertResult(Seq()) {
      runAndWait(listPendingBucketDeletion())
    }
  }
}
