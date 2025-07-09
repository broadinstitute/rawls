package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

class SpendReportingModelSpec extends AnyFlatSpecLike {
  object TestData {
    val defaultCurrency = "USD"
    val defaultAzureCredits = "0" // always 0 for Azure
  }

  behavior of "TerraSpendCategories mapping"

  it should "convert correct string into specific category" in {
    TerraSpendCategories.withName("compute") shouldBe TerraSpendCategories.Compute
    TerraSpendCategories.withName("Compute") shouldBe TerraSpendCategories.Compute
    TerraSpendCategories.withName("storage") shouldBe TerraSpendCategories.Storage
    TerraSpendCategories.withName("Storage") shouldBe TerraSpendCategories.Storage
    TerraSpendCategories.withName("other") shouldBe TerraSpendCategories.Other
    TerraSpendCategories.withName("Other") shouldBe TerraSpendCategories.Other
    TerraSpendCategories.withName("workspaceinfrastructure") shouldBe TerraSpendCategories.WorkspaceInfrastructure
    TerraSpendCategories.withName("WorkspaceInfrastructure") shouldBe TerraSpendCategories.WorkspaceInfrastructure
  }

  it should "throw exception in case of invalid string representation of a category" in {
    an[RawlsException] should be thrownBy TerraSpendCategories.withName("test")
    an[RawlsException] should be thrownBy TerraSpendCategories.withName("ai")
    an[RawlsException] should be thrownBy TerraSpendCategories.withName("unexpectedCategory")
  }

}
