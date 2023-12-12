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

  implicit val getWorkflowRecord: GetResult[WorkflowRecord] = GetResult { r =>
    WorkflowRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)
  }

  val selectAllFromWorkflow =
    "SELECT ID, EXTERNAL_ID, SUBMISSION_ID, STATUS, STATUS_LAST_CHANGED, ENTITY_ID, record_version, EXEC_SERVICE_KEY, EXTERNAL_ENTITY_ID FROM WORKFLOW"

  "DriverComponent" should "test concatSqlActions" in withDefaultTestDatabase {

    val select = sql"#$selectAllFromWorkflow "
    val where = sql"WHERE STATUS = 'Submitted' "
    val queryA = sql"#$selectAllFromWorkflow WHERE STATUS = 'Submitted' "

    val queryArecords = runAndWait(queryA.as[WorkflowRecord])

    // first check that we're not just comparing empty seqs
    assertResult(22)(queryArecords.length)

    assertResult(queryArecords) {
      runAndWait(concatSqlActions(Seq(select, where): _*).as[WorkflowRecord])
    }

    val where1 = sql"WHERE STATUS IN ('Submitted' "
    val where2 = sql"'Done') "
    val queryB = sql"#$selectAllFromWorkflow WHERE STATUS IN ('Submitted','Done') "

    val queryBrecords = runAndWait(queryB.as[WorkflowRecord])

    // first check that we're not just comparing empty seqs
    assertResult(22)(queryBrecords.length)

    assertResult(queryBrecords) {
      runAndWait(concatSqlActions(Seq(select, where1, sql",", where2): _*).as[WorkflowRecord])
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

    val query =
      sql"#$selectAllFromWorkflow WHERE STATUS IN ('Queued','Launching','Submitted','Running','Failed','Succeeded','Aborting','Aborted','Unknown','Done')"

    val queryRecords = runAndWait(query.as[WorkflowRecord])

    // first check that we're not just comparing empty seqs
    assertResult(33)(queryRecords.length)

    assertResult(queryRecords) {
      runAndWait(concatSqlActions(select, where1, reduceSqlActionsWithDelim(statuses), where2).as[WorkflowRecord])
    }
  }

  // base64 represents every six bits with one character. but we round up our input number of bits to the nearest 8, so:
  def expectedStringLength(bits: Int): Int =
    Math.ceil(Math.ceil(bits / 8.0) * 8.0 / 6.0).toInt

  it should "get a sufficiently random postfix" in {
    // this corresponds to 16 bits of entropy if my math is right
    assert(getNumberOfBitsForSufficientRandomness(64, 1.0 / 32.0) == 16)

    // one more record should tip us over
    assert(getNumberOfBitsForSufficientRandomness(65, 1.0 / 32.0) == 17)

    // check we don't overflow when we have a ton of records:
    // 2^34 records ~17bn! 17,179,869,184
    // 2^30 is close to 1 in a billion: 1,073,741,824
    assert(getNumberOfBitsForSufficientRandomness(17179869184L, 1.0 / 1073741824) == 97)

    // test that the properties of the random string hold as expected
    assert(getRandomStringWithThisManyBitsOfEntropy(2).length == expectedStringLength(2))
    assert(getRandomStringWithThisManyBitsOfEntropy(8).length == expectedStringLength(8))
    assert(getRandomStringWithThisManyBitsOfEntropy(9).length == expectedStringLength(9))
    assert(getRandomStringWithThisManyBitsOfEntropy(59).length == expectedStringLength(59))
  }
}
