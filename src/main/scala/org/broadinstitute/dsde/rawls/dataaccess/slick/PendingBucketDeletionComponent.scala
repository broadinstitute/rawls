package org.broadinstitute.dsde.rawls.dataaccess.slick

case class PendingBucketDeletion(bucket: String)

trait PendingBucketDeletionComponent {
  this: DriverComponent =>

  import driver.api._

  class PendingBucketDeletions(tag: Tag) extends Table[PendingBucketDeletion](tag, "BUCKET_DELETION") {
    def bucket = column[String]("bucket")

    def * = (bucket) <> (PendingBucketDeletion.apply _, PendingBucketDeletion.unapply)
  }

  val pendingBucketDeletions = TableQuery[PendingBucketDeletions]


  def save(pendingBucketDeletion: PendingBucketDeletion) = {
    pendingBucketDeletions += pendingBucketDeletion
  }
}