package org.broadinstitute.dsde.rawls.dataaccess.slick

import akka.util.ByteString
import org.apache.commons.codec.binary.Base64
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{RawlsException, StringValidationUtils}
import slick.jdbc._

import java.nio.ByteOrder
import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.ExecutionContext

trait DriverComponent extends StringValidationUtils {
  val driver: JdbcProfile
  val batchSize: Int // used for writes to group inserts/updates; must be explicitly utilized via custom business logic
  val fetchSize: Int // used during Slick streaming to set the size of pages; must be explicitly set via withStatementParameters
  implicit val executionContext: ExecutionContext
  implicit override val errorReportSource = ErrorReportSource("rawls")

  // needed by MySQL but not actually used; we will always overwrite
  val defaultTimeStamp = Timestamp.valueOf("2001-01-01 01:01:01.0")

  import driver.api._

  def uniqueResult[V](readAction: driver.api.Query[_, _, Seq]): ReadAction[Option[V]] =
    readAction.result map {
      case Seq()    => None
      case Seq(one) => Option(one.asInstanceOf[V])
      case tooMany  => throw new RawlsException(s"Expected 0 or 1 result but found all of these: $tooMany")
    }

  def uniqueResult[V](results: ReadAction[Seq[V]]): ReadAction[Option[V]] =
    results map {
      case Seq()    => None
      case Seq(one) => Option(one)
      case tooMany  => throw new RawlsException(s"Expected 0 or 1 result but found all of these: $tooMany")
    }

  def insertInBatches[R, T <: Table[R]](tableQuery: TableQuery[T], records: Seq[R]): WriteAction[Int] =
    DBIO.sequence(records.grouped(batchSize).map(tableQuery ++= _)).map(_.flatten.sum)

  def nowTimestamp: Timestamp =
    new Timestamp(System.currentTimeMillis())

  private[slick] def getNumberOfBitsForSufficientRandomness(recordCount: Long,
                                                            desiredCollisionProbability: Double = 0.000000001
  ): Int = {
    def log2(n: Double): Double = Math.log(n) / Math.log(2)

    /* Uh oh. A huge comment block approaches!

     * What we want here is a string that adds "sufficient randomness" to make it unlikely that this record will collide
     * with another. This is the birthday attack problem!
     *
     * There's some math about how to escape being attacked by a birthday on Wikipedia:
     * https://en.wikipedia.org/wiki/Birthday_attack#Simple_approximation
     * H = n^2 / 2p(n)
     * H is "the number of possible outputs our hash function needs to be able to generate".

     * Below is the naive formula:
     * val H = (recordCount*recordCount)/(2.0*desiredCollisionProbability)

     * However, for large (billions+) counts of records, and very low collision probabilities, H will overflow a double.
     * Thankfully, what we _really_ want is the number of bits of entropy we need to generate.
     * The formula for this is log2(H), which we can push into H to keep the values nice and low.
     */
    Math.ceil(log2(recordCount) * 2.0 - log2(2.0 * desiredCollisionProbability)).toInt
  }

  private[slick] def getRandomStringWithThisManyBitsOfEntropy(bits: Int): String = {
    val uuid = UUID.randomUUID()

    // The goal here is to make this string as short as possible, so base64encode the resulting
    // bits for maximum squishiness
    val byteBuilder = ByteString.newBuilder
    val byteOrder = ByteOrder.nativeOrder()

    if (bits <= 64) {
      byteBuilder.putLongPart(uuid.getLeastSignificantBits, Math.ceil(bits / 8.0).toInt)(byteOrder)
    } else {
      byteBuilder.putLong(uuid.getLeastSignificantBits)(byteOrder)
      byteBuilder.putLongPart(uuid.getMostSignificantBits, ((bits - 64) / 8.0).toInt)(byteOrder)
    }

    Base64.encodeBase64URLSafeString(byteBuilder.result().toArray)
  }

  // By default, calibrated for a one-in-a-billion chance of collision.
  def getSufficientlyRandomSuffix(recordCount: Long, desiredCollisionProbability: Double = 0.000000001): String = {

    // the number of bits of entropy required. if this ever gets above 128 we're in trouble.
    val bits = getNumberOfBitsForSufficientRandomness(recordCount, desiredCollisionProbability)

    getRandomStringWithThisManyBitsOfEntropy(bits)
  }

  def renameForHiding(recordCount: Long, name: String): String =
    name + "_" + getSufficientlyRandomSuffix(recordCount)
}

/**
 * Base trait for objects that encapsulate raw sql. The pattern is to use an object
 * that encloses all the GetResult, SetParameter and raw sql into a nice package.
 */
trait RawSqlQuery {
  val driver: JdbcProfile

  import driver.api._

  implicit val GetUUIDResult = GetResult(r => uuidColumnType.fromBytes(r.nextBytes()))
  implicit val GetUUIDOptionResult = GetResult(r => Option(uuidColumnType.fromBytes(r.nextBytes())))
  implicit object SetUUIDParameter extends SetParameter[UUID] {
    def apply(v: UUID, pp: PositionedParameters) { pp.setBytes(uuidColumnType.toBytes(v)) }
  }
  implicit object SetUUIDOptionParameter extends SetParameter[Option[UUID]] {
    def apply(v: Option[UUID], pp: PositionedParameters) { pp.setBytesOption(v.map(uuidColumnType.toBytes)) }
  }

  def concatSqlActions(builders: SQLActionBuilder*): SQLActionBuilder =
    SQLActionBuilder(builders.flatMap(_.queryParts),
                     new SetParameter[Unit] {
                       def apply(p: Unit, pp: PositionedParameters): Unit =
                         builders.foreach(_.unitPConv.apply(p, pp))
                     }
    )

  // reduce((a, b) => concatSqlActionsWithDelim(a, b, delim)) without recursion
  // e.g.
  //    builders = (sql"1", sql"2", sql"3", sql"4")
  //    delim = sql","
  //    output = sql"1,2,3,4"
  def reduceSqlActionsWithDelim(builders: Seq[SQLActionBuilder], delim: SQLActionBuilder = sql","): SQLActionBuilder = {
    val elementsWithDelimiters = builders.flatMap(Seq(_, delim)).dropRight(1)
    concatSqlActions(elementsWithDelimiters: _*)
  }
}
