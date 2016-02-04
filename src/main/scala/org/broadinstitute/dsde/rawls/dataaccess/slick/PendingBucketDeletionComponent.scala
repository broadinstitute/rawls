package org.broadinstitute.dsde.rawls.dataaccess.slick

case class PendingBucketDeletion(bucket: String)

trait PendingBucketDeletionComponent {
  this: DriverComponent =>

  import driver.api._

  class PendingBucketDeletionTable(tag: Tag) extends Table[PendingBucketDeletion](tag, "BUCKET_DELETION") {
    def bucket = column[String]("bucket", O.PrimaryKey)

    def * = (bucket) <> (PendingBucketDeletion.apply _, PendingBucketDeletion.unapply)
  }

  val pendingBucketDeletionQuery = TableQuery[PendingBucketDeletionTable]


  def savePendingBucketDeletion(pendingBucketDeletion: PendingBucketDeletion) = {
    pendingBucketDeletionQuery insertOrUpdate pendingBucketDeletion map(_ => pendingBucketDeletion)
  }

  def listPendingBucketDeletion() = {
    pendingBucketDeletionQuery.result
  }

  def deletePendingBucketDeletion(pendingBucketDeletion: PendingBucketDeletion) = {
    pendingBucketDeletionQuery.filter(_.bucket === pendingBucketDeletion.bucket).delete
  }
}