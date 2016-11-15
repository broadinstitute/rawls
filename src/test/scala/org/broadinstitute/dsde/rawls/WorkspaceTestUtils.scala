package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponentWithFlatSpecAndMatchers
import org.broadinstitute.dsde.rawls.model.Workspace
import org.scalatest.exceptions.TestFailedException
import spray.http.{StatusCode, StatusCodes}

trait WorkspaceTestUtils extends TestDriverComponentWithFlatSpecAndMatchers {

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
  def assertWorkspaceResult(expected: Workspace)(actual: Workspace) = {
    assertResult(expected) {
      actual.copy(lastModified = expected.lastModified)
    }
  }

  // Option flavor of above
  def assertWorkspaceResult(expected: Option[Workspace])(actual: Option[Workspace]) = {
    expected match {
      case None => throw new TestFailedException(Some("Unable to parse expected value: " + expected.toString), None, 4)
      case Some(e) =>
        assertResult(e) {
          actual.get.copy(lastModified = e.lastModified)
        }
    }
  }

  // Seq flavor of above
  def assertWorkspaceResult(expected: Seq[Workspace])(actual: Seq[Workspace]) = {
    expected zip actual foreach {
      case (e, a) =>
        assertResult(e) {
          a.copy(lastModified = e.lastModified)
        }
      case _ => throw new TestFailedException(Some("Unable to parse expected value: " + expected.toString), None, 4)
    }
  }

}
