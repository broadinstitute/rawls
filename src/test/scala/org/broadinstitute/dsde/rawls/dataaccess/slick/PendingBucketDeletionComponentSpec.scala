package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

/**
 * Created by dvoet on 2/3/16.
 */
class PendingBucketDeletionComponentSpec extends FlatSpec with Matchers with TestDriverComponent with PendingBucketDeletionComponent with BeforeAndAfterAll {
  import driver.api._

  override def beforeAll: Unit = {
    runAndWait(pendingBucketDeletionQuery.schema.create)
  }

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
