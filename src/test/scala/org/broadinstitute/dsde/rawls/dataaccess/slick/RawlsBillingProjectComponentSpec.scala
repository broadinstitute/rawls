package org.broadinstitute.dsde.rawls.dataaccess.slick

class RawlsBillingProjectComponentSpec extends TestDriverComponent with RawlsBillingProjectComponent with RawlsUserComponent {

  "RawlsBillingProjectComponent" should "create, load and delete" in {
    val projectName = "arbitrary"
    val project = RawlsBillingProjectRecord(projectName, "http://cromwell-auth-url.example.com")

    assertResult(Seq()) {
      runAndWait(loadBillingProject(projectName))
    }

    assertResult(project) {
      runAndWait(saveBillingProject(project))
    }

    assertResult(Seq(project)) {
      runAndWait(loadBillingProject(projectName))
    }

    assertResult(1) {
      runAndWait(deleteBillingProject(projectName))
    }

    assertResult(Seq()) {
      runAndWait(loadBillingProject(projectName))
    }
  }
}
