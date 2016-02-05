package org.broadinstitute.dsde.rawls.dataaccess.slick

case class RawlsBillingProjectRecord(projectName: String, cromwellAuthBucketUrl: String)

trait RawlsBillingProjectComponent {
  this: DriverComponent =>

  import driver.api._

  class RawlsBillingProjectTable(tag: Tag) extends Table[RawlsBillingProjectRecord](tag, "BILLING_PROJECT") {
    def projectName = column[String]("projectName", O.PrimaryKey)
    def cromwellAuthBucketUrl = column[String]("cromwellAuthBucketUrl")

    def * = (projectName, cromwellAuthBucketUrl) <> (RawlsBillingProjectRecord.tupled, RawlsBillingProjectRecord.unapply)
  }

  val rawlsBillingProjectQuery = TableQuery[RawlsBillingProjectTable]

  def rawlsBillingProjectByNameQuery(name: String) = Compiled {
    rawlsBillingProjectQuery.filter(_.projectName === name)
  }

  def saveBillingProject(billingProject: RawlsBillingProjectRecord) = {
    rawlsBillingProjectQuery insertOrUpdate billingProject map { _ => billingProject }
  }

  def loadBillingProject(name: String) = rawlsBillingProjectByNameQuery(name).result

  def deleteBillingProject(name: String) = rawlsBillingProjectByNameQuery(name).delete
}
