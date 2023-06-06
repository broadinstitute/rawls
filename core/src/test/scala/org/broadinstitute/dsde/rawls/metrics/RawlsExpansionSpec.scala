package org.broadinstitute.dsde.rawls.metrics

import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.model.Subsystems.Subsystem
import org.broadinstitute.dsde.rawls.model.{RawlsEnumeration, Subsystems, WorkspaceName}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Created by rtitle on 7/25/17.
  */
class RawlsExpansionSpec extends AnyFlatSpec with Matchers {

  "the RawlsExpansion typeclass" should "expand WorkspaceNames" in {
    val test = WorkspaceName("test", "workspace")
    assertResult("test.workspace") {
      implicitly[Expansion[WorkspaceName]].makeName(test)
    }
  }

  it should "expand RawlsEnumerations" in {
    val subsystem = Subsystems.Database

    // Verify we can summon an implicit for multiple levels in the object hierarchy
    assertResult("Database") {
      implicitly[Expansion[RawlsEnumeration[_]]].makeName(subsystem)
    }
    assertResult("Database") {
      implicitly[Expansion[Subsystem]].makeName(subsystem)
    }
    assertResult("Database") {
      implicitly[Expansion[Subsystems.Database.type]].makeName(subsystem)
    }
  }

}
