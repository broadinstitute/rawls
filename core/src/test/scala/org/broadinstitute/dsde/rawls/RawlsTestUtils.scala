package org.broadinstitute.dsde.rawls

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model.Workspace
import org.mockserver.model.RegexBody
import org.scalatest.Suite
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers

import scala.util.matching.Regex

trait RawlsTestUtils extends Suite with TestDriverComponent with Matchers {

  def assertWorkspaceModifiedDate(status: StatusCode, workspace: Workspace) = {
    assertResult(StatusCodes.OK) {
      status
    }
    assert {
      workspace.lastModified.isAfter(workspace.createdDate)
    }
  }

  /**
    * The following three methods are needed when comparing a workspace expected vs actual value in the test framework.
    * In the case of a function that results in an update to the last modified date, the original expected value will
    * be different from the actual value only in that field. We need this version of assertion to ignore that field
    * and proceed in checking the equivalence of all other components of the two workspaces.
    */
  def assertWorkspaceResult(expected: Workspace)(actual: Workspace) =
    assertResult(expected) {
      actual.copy(lastModified = expected.lastModified)
    }

  // Option flavor of above
  def assertWorkspaceResult(expected: Option[Workspace])(actual: Option[Workspace]) =
    expected match {
      case None => throw new TestFailedException(Some("Unable to parse expected value: " + expected.toString), None, 4)
      case Some(e) =>
        assertResult(e) {
          actual.get.copy(lastModified = e.lastModified)
        }
    }

  // Seq flavor of above
  def assertWorkspaceResult(expected: Seq[Workspace])(actual: Seq[Workspace]) =
    expected zip actual foreach {
      case (exp, act) =>
        assertResult(exp) {
          act.copy(lastModified = exp.lastModified)
        }
      case _ => throw new TestFailedException(Some("Unable to parse expected value: " + expected.toString), None, 4)
    }

  def sortAndAssertWorkspaceResult(expected: Seq[Workspace])(actual: Seq[Workspace]) = {
    def ordering(w: Workspace) = (w.namespace, w.name)
    assertWorkspaceResult(expected.sortBy(ordering))(actual.sortBy(ordering))
  }

  // prefer this to using theSameElementsAs directly, because its functionality depends on whitespace
  def assertSameElements[T](expected: TraversableOnce[T], actual: TraversableOnce[T]): Unit =
    expected.toTraversable should contain theSameElementsAs actual.toTraversable

  def assertSubsetOf[T](expected: TraversableOnce[T], actual: TraversableOnce[T]): Unit = {
    val as = actual.toSet
    expected foreach { as should contain(_) }
  }

  // MockServer's .withBody doesn't have a built-in string contains feature.  This serves that purpose.
  def mockServerContains(text: String): RegexBody =
    // "(?s)" turns on DOTALL mode, where a "." matches a line break as well as any character
    new RegexBody("(?s).*" + Regex.quote(text) + ".*")
}
