package org.broadinstitute.dsde.rawls.dataaccess.slick

case class PendingBucketDeletionRecord(bucket: String)

trait PendingBucketDeletionComponent {
  this: DriverComponent =>

  import driver.api._

  class PendingBucketDeletionTable(tag: Tag) extends Table[PendingBucketDeletionRecord](tag, "BUCKET_DELETION") {
    def bucket = column[String]("bucket", O.PrimaryKey)

    def * = (bucket) <> (PendingBucketDeletionRecord.apply _, PendingBucketDeletionRecord.unapply)
  }

  object pendingBucketDeletionQuery extends TableQuery(new PendingBucketDeletionTable(_)) {
    def save(pendingBucketDeletion: PendingBucketDeletionRecord): WriteAction[PendingBucketDeletionRecord] = {
      pendingBucketDeletionQuery insertOrUpdate pendingBucketDeletion map(_ => pendingBucketDeletion)
    }

    def list(): ReadAction[Seq[PendingBucketDeletionRecord]] = {
      pendingBucketDeletionQuery.result
    }

    def delete(pendingBucketDeletion: PendingBucketDeletionRecord): WriteAction[Int] = {
      filter(_.bucket === pendingBucketDeletion.bucket).delete
    }
  }
}