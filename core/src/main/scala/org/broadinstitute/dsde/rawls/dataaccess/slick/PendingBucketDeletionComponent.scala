package org.broadinstitute.dsde.rawls.dataaccess.slick

case class PendingBucketDeletionRecord(bucket: String)

trait PendingBucketDeletionComponent {
  this: DriverComponent =>

  import driver.api._

  class PendingBucketDeletionTable(tag: Tag) extends Table[PendingBucketDeletionRecord](tag, "BUCKET_DELETION") {
    def bucket = column[String]("bucket", O.PrimaryKey, O.Length(254))

    def * = bucket <> (PendingBucketDeletionRecord, PendingBucketDeletionRecord.unapply)
  }

  object pendingBucketDeletionQuery extends TableQuery(new PendingBucketDeletionTable(_)) {

    def save(pendingBucketDeletion: PendingBucketDeletionRecord): ReadWriteAction[PendingBucketDeletionRecord] =
      filter(_.bucket === pendingBucketDeletion.bucket).result.flatMap { records =>
        if (records.isEmpty) pendingBucketDeletionQuery += pendingBucketDeletion else DBIO.successful(None)
      } map (_ => pendingBucketDeletion)

    def save(bucketName: String): ReadWriteAction[PendingBucketDeletionRecord] = save(
      PendingBucketDeletionRecord(bucketName)
    )

    def list(): ReadAction[Seq[PendingBucketDeletionRecord]] =
      pendingBucketDeletionQuery.result

    def delete(pendingBucketDeletion: PendingBucketDeletionRecord): WriteAction[Int] =
      filter(_.bucket === pendingBucketDeletion.bucket).delete
  }
}
