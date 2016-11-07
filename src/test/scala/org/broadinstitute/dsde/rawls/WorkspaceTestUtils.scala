package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponentWithFlatSpecAndMatchers
import org.broadinstitute.dsde.rawls.model.Workspace
import org.scalatest.exceptions.TestFailedException

trait WorkspaceTestUtils extends TestDriverComponentWithFlatSpecAndMatchers {

  def assertWorkspaceResult(expected: Workspace)(actual: Workspace) = {
    assertResult(expected) {
      actual.copy(lastModified = expected.lastModified)
    }
  }

  def assertWorkspaceResult(expected: Option[Workspace])(actual: Option[Workspace]) = {
    expected match {
      case None => throw new TestFailedException(Some("Unable to parse expected value: " + expected.toString), None, 4)
      case Some(e) =>
        assertResult(e) {
          actual.get.copy(lastModified = e.lastModified)
        }
    }
  }

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
