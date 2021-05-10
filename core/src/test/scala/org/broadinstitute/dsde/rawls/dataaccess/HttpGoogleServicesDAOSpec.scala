package org.broadinstitute.dsde.rawls.dataaccess

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.rawls.dataaccess.HttpGoogleServicesDAO._

class HttpGoogleServicesDAOSpec extends AnyFlatSpec with Matchers {

  behavior of "handleByOperationIdType"

  def v1Handler(opId: String) = "v1"
  def v2alpha1Handler(opId: String) = "v2alpha1"
  def lifeSciencesHandler(opId: String) = "lifeSciences"
  def defaultHandler(opId: String) = "default"

  val cases: List[(String, String)] = List(
    ("operations/abc", "v1"),
    ("projects/abc/operations/def", "v2alpha1"),
    ("projects/abc/locations/def/operations/ghi", "lifeSciences"),
    ("!!no match!!", "default"),
  )

  cases foreach { case (opId, identification) =>
    it should s"Correctly identify $opId as $identification" in {
      handleByOperationIdType(opId, v1Handler, v2alpha1Handler, lifeSciencesHandler, defaultHandler) should be(identification)
    }
  }
}
