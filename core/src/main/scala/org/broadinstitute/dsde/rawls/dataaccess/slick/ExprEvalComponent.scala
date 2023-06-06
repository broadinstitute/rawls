package org.broadinstitute.dsde.rawls.dataaccess.slick

case class ExprEvalRecord(id: Long, name: String, transactionId: String)

/**
  * Created by thibault on 4/20/17.
  */
trait ExprEvalComponent {
  this: DriverComponent =>

  import driver.api._

  class ExprEvalScratch(tag: Tag) extends Table[ExprEvalRecord](tag, "EXPREVAL_SCRATCH") {
    def id = column[Long]("id")
    def name = column[String]("name", O.Length(254))
    def transactionId = column[String]("transaction_id")

    // No foreign key constraint here because MySQL won't allow them on temp tables :(
    // def entityId = foreignKey("FK_EXPREVAL_ENTITY", id, entityQuery)(_.id)

    def * = (id, name, transactionId) <> (ExprEvalRecord.tupled, ExprEvalRecord.unapply)
  }

  val exprEvalQuery = TableQuery[ExprEvalScratch]
}
