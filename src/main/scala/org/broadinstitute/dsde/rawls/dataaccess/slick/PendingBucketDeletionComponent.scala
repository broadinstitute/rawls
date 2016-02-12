package org.broadinstitute.dsde.rawls.dataaccess.slick

case class PendingBucketDeletion(bucket: String)

trait PendingBucketDeletionComponent {
  this: DriverComponent =>

  import driver.api._

  class PendingBucketDeletionTable(tag: Tag) extends Table[PendingBucketDeletion](tag, "BUCKET_DELETION") {
    def bucket = column[String]("bucket", O.PrimaryKey)

    def * = (bucket) <> (PendingBucketDeletion.apply _, PendingBucketDeletion.unapply)
  }

  object pendingBucketDeletionQuery extends TableQuery(new PendingBucketDeletionTable(_)) {
    def save(pendingBucketDeletion: PendingBucketDeletion) = {
      pendingBucketDeletionQuery insertOrUpdate pendingBucketDeletion map(_ => pendingBucketDeletion)
    }

    def list() = {
      pendingBucketDeletionQuery.result
    }

    def delete(pendingBucketDeletion: PendingBucketDeletion) = {
      pendingBucketDeletionQuery.filter(_.bucket === pendingBucketDeletion.bucket).delete
    }
  }
}