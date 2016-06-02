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

  "DriverComponent" should "test concatSqlActions" in withDefaultTestDatabase {
    implicit val getWorkflowRecord = GetResult { r => WorkflowRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<) }

    val select = sql"SELECT * FROM WORKFLOW "
    val where = sql"WHERE STATUS = 'Submitted' "
    val queryA = sql"SELECT * FROM WORKFLOW WHERE STATUS = 'Submitted' "

    val queryArecords = runAndWait(queryA.as[WorkflowRecord])

    // first check that we're not just comparing empty seqs
    assertResult(12) { queryArecords.length }

    assertResult(queryArecords) {
      runAndWait(concatSqlActions(Seq(select, where):_*).as[WorkflowRecord])
    }

    val where1 = sql"WHERE STATUS IN ('Submitted' "
    val where2 = sql"'Done') "
    val queryB = sql"SELECT * FROM WORKFLOW WHERE STATUS IN ('Submitted','Done') "

    val queryBrecords = runAndWait(queryB.as[WorkflowRecord])

    // first check that we're not just comparing empty seqs
    assertResult(12) { queryBrecords.length }

    assertResult(queryBrecords) {
      runAndWait(concatSqlActions(Seq(select, where1, sql",", where2):_*).as[WorkflowRecord])
    }
  }

  it should "test reduceSqlActionsWithDelim" in withDefaultTestDatabase {
    implicit val getWorkflowRecord = GetResult { r => WorkflowRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<) }

    val select = sql"SELECT * FROM WORKFLOW "
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

    val query = sql"SELECT * FROM WORKFLOW WHERE STATUS IN ('Queued','Launching','Submitted','Running','Failed','Succeeded','Aborting','Aborted','Unknown','Done')"

    val queryRecords = runAndWait(query.as[WorkflowRecord])

    // first check that we're not just comparing empty seqs
    assertResult(12) { queryRecords.length }

    assertResult(queryRecords) {
      runAndWait(concatSqlActions(select, where1, reduceSqlActionsWithDelim(statuses), where2).as[WorkflowRecord])
    }
  }
}
