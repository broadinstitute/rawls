package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.jdbc.GetResult

/**
 * Created by thibault on 5/19/16.
 */
class DriverComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawSqlQuery {
  import driver.api._

  // TODO tests for GAWB-608:
  //   uniqueResult
  //   validateUserDefinedString
  //   validateAttributeName
  //   createBatches

  implicit val getWorkflowRecord = GetResult { r => WorkflowRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<) }

  val selectAllFromWorkflow = "SELECT ID, EXTERNAL_ID, SUBMISSION_ID, STATUS, STATUS_LAST_CHANGED, ENTITY_ID, record_version, EXEC_SERVICE_KEY FROM WORKFLOW"

  "DriverComponent" should "test concatSqlActions" in withDefaultTestDatabase {

    val select = sql"#$selectAllFromWorkflow "
    val where = sql"WHERE STATUS = 'Submitted' "
    val queryA = sql"#$selectAllFromWorkflow WHERE STATUS = 'Submitted' "

    val queryArecords = runAndWait(queryA.as[WorkflowRecord])

    // first check that we're not just comparing empty seqs
    assertResult(15) { queryArecords.length }

    assertResult(queryArecords) {
      runAndWait(concatSqlActions(Seq(select, where):_*).as[WorkflowRecord])
    }

    val where1 = sql"WHERE STATUS IN ('Submitted' "
    val where2 = sql"'Done') "
    val queryB = sql"#$selectAllFromWorkflow WHERE STATUS IN ('Submitted','Done') "

    val queryBrecords = runAndWait(queryB.as[WorkflowRecord])

    // first check that we're not just comparing empty seqs
    assertResult(15) { queryBrecords.length }

    assertResult(queryBrecords) {
      runAndWait(concatSqlActions(Seq(select, where1, sql",", where2):_*).as[WorkflowRecord])
    }
  }

  it should "test reduceSqlActionsWithDelim" in withDefaultTestDatabase {

    val select = sql"#$selectAllFromWorkflow "
    val where1 = sql"WHERE STATUS IN ("
    val where2 = sql") "
    val statuses = Seq(
      sql"'Queued'",
      sql"'Launching'",
      sql"'Submitted'",
      sql"'Running'",
      sql"'Failed'",
      sql"'Succeeded'",
      sql"'Aborting'",
      sql"'Aborted'",
      sql"'Unknown'"
    )

    val query = sql"#$selectAllFromWorkflow WHERE STATUS IN ('Queued','Launching','Submitted','Running','Failed','Succeeded','Aborting','Aborted','Unknown','Done')"

    val queryRecords = runAndWait(query.as[WorkflowRecord])

    // first check that we're not just comparing empty seqs
    assertResult(22) { queryRecords.length }

    assertResult(queryRecords) {
      runAndWait(concatSqlActions(select, where1, reduceSqlActionsWithDelim(statuses), where2).as[WorkflowRecord])
    }
  }
}
