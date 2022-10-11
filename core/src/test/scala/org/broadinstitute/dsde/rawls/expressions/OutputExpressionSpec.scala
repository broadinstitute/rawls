package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.{AttributeName, AttributeString}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

class OutputExpressionSpec extends AnyFlatSpec with Matchers {
  private val testAttr = AttributeString("this is a test")
  private val testEntityType = Option("wombat")

  "OutputExpressions" should "parse" in {
    OutputExpression.build("this.gvcf", testAttr, testEntityType) shouldEqual Success(
      BoundOutputExpression(ThisEntityTarget, AttributeName("default", "gvcf"), testAttr)
    )
    OutputExpression.build("this.with-dash", testAttr, testEntityType) shouldEqual Success(
      BoundOutputExpression(ThisEntityTarget, AttributeName("default", "with-dash"), testAttr)
    )
    OutputExpression.build("workspace.gvcf", testAttr, testEntityType) shouldEqual Success(
      BoundOutputExpression(WorkspaceTarget, AttributeName("default", "gvcf"), testAttr)
    )
    OutputExpression.build("workspace.with-dash", testAttr, testEntityType) shouldEqual Success(
      BoundOutputExpression(WorkspaceTarget, AttributeName("default", "with-dash"), testAttr)
    )
    OutputExpression.build("workspace.library:cohort", testAttr, testEntityType) shouldEqual Success(
      BoundOutputExpression(WorkspaceTarget, AttributeName("library", "cohort"), testAttr)
    )
    OutputExpression.build("this.library:cohort", testAttr, testEntityType) shouldEqual Success(
      BoundOutputExpression(ThisEntityTarget, AttributeName("library", "cohort"), testAttr)
    )

    // does not enforce Attribute Namespace constraints
    OutputExpression.build("this.arbitrary:whatever", testAttr, testEntityType) shouldEqual Success(
      BoundOutputExpression(ThisEntityTarget, AttributeName("arbitrary", "whatever"), testAttr)
    )
    OutputExpression.build("workspace.arbitrary:whatever", testAttr, testEntityType) shouldEqual Success(
      BoundOutputExpression(WorkspaceTarget, AttributeName("arbitrary", "whatever"), testAttr)
    )

    // empty output expressions are allowed: don't bind the outputs back to the data model
    OutputExpression.build("", testAttr, testEntityType) shouldEqual Success(UnboundOutputExpression)
  }

  it should "reject invalid output expressions" in {
    OutputExpression.build("this.", testAttr, testEntityType) shouldBe a[Failure[_]]
    OutputExpression.build("this.bad|character", testAttr, testEntityType) shouldBe a[Failure[_]]
    OutputExpression.build("this.case_sample.attribute", testAttr, testEntityType) shouldBe a[Failure[_]]
    OutputExpression.build("workspace.", testAttr, testEntityType) shouldBe a[Failure[_]]
    OutputExpression.build("workspace........", testAttr, testEntityType) shouldBe a[Failure[_]]
    OutputExpression.build("workspace.nope.nope.nope", testAttr, testEntityType) shouldBe a[Failure[_]]
    OutputExpression.build("where_does_this_even_go", testAttr, testEntityType) shouldBe a[Failure[_]]
    OutputExpression.build("*", testAttr, testEntityType) shouldBe a[Failure[_]]
  }

  it should "validate" in {
    OutputExpression.validate("workspace.gvcf", None) shouldBe a[Success[_]]
    OutputExpression.validate("workspace.library:cohort", None) shouldBe a[Success[_]]
    OutputExpression.validate("this.gvcf", Option("entity_type")) shouldBe a[Success[_]]
    OutputExpression.validate("workspace.gvcf", Option("entity_type")) shouldBe a[Success[_]]
    OutputExpression.validate("workspace.library:cohort", Option("entity_type")) shouldBe a[Success[_]]
    OutputExpression.validate("this.library:cohort", Option("entity_type")) shouldBe a[Success[_]]

    // does not enforce Attribute Namespace constraints
    OutputExpression.validate("workspace.arbitrary:whatever", None) shouldBe a[Success[_]]
    OutputExpression.validate("this.arbitrary:whatever", Option("entity_type")) shouldBe a[Success[_]]
    OutputExpression.validate("workspace.arbitrary:whatever", Option("entity_type")) shouldBe a[Success[_]]

    // Can overwrite "_id" expressions when the entity type is different
    OutputExpression.validate("this.entity_type_id", testEntityType) shouldBe a[Success[_]]

    // Can overwrite "_id" expressions when using a non-default namespace
    OutputExpression.validate("this.library:entity_type_id", Option("entity_type")) shouldBe a[Success[_]]

    // empty output expressions are allowed: don't bind the outputs back to the data model
    OutputExpression.validate("", None) shouldBe a[Success[_]]
    OutputExpression.validate("", Option("entity_type")) shouldBe a[Success[_]]
  }

  it should "not validate" in {
    OutputExpression.validate("this.", None) shouldBe a[Failure[_]]
    OutputExpression.validate("this.foo", None) shouldBe a[Failure[_]]
    OutputExpression.validate("this.bad|character", None) shouldBe a[Failure[_]]
    OutputExpression.validate("this.case_sample.attribute", None) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.", None) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace........", None) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.nope.nope.nope", None) shouldBe a[Failure[_]]
    OutputExpression.validate("where_does_this_even_go", None) shouldBe a[Failure[_]]
    OutputExpression.validate("*", None) shouldBe a[Failure[_]]

    OutputExpression.validate("this.", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("this.bad|character", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("this.case_sample.attribute", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace........", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.nope.nope.nope", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("where_does_this_even_go", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("*", Option("entity_type")) shouldBe a[Failure[_]]

    // Cannot overwrite the entity type plus "_id" and the default namespace
    OutputExpression.validate("this.entity_type_id", Option("entity_type")) shouldBe a[Failure[_]]

    // Cannot overwrite the workspace_id in most cases
    OutputExpression.validate("workspace.workspace_id", None) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.default:workspace_id", None) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.workspace_id", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.default:workspace_id", Option("entity_type")) shouldBe a[Failure[_]]

    // Cannot overwrite the workspace name
    OutputExpression.validate("workspace.name", None) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.default:name", None) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.name", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.default:name", Option("entity_type")) shouldBe a[Failure[_]]

    // Cannot overwrite the workspace entityType (camel case)
    OutputExpression.validate("workspace.entityType", None) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.default:entityType", None) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.entityType", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("workspace.default:entityType", Option("entity_type")) shouldBe a[Failure[_]]

    // Cannot overwrite this.workspace_id, this.name, this.entityType when an entity type is specified
    OutputExpression.validate("this.workspace_id", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("this.default:workspace_id", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("this.name", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("this.default:name", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("this.entityType", Option("entity_type")) shouldBe a[Failure[_]]
    OutputExpression.validate("this.default:entityType", Option("entity_type")) shouldBe a[Failure[_]]
  }

  it should "not validate with user friendly errors" in {
    intercept[RawlsExceptionWithErrorReport] {
      OutputExpression.validate("workspace.name", None).get
    }.errorReport.message should be("Attribute name name is reserved and cannot be overwritten")

    intercept[RawlsExceptionWithErrorReport] {
      OutputExpression.validate("this.entity_type_id", Option("entity_type")).get
    }.errorReport.message should be("Attribute name entity_type_id is reserved and cannot be overwritten")

    intercept[RawlsExceptionWithErrorReport] {
      OutputExpression.validate("foo.bar", None).get
    }.errorReport.message should be(
      "Error while parsing the expression. Offending symbol is on line 1 at position 0. Error: mismatched input 'foo' expecting {'{', '[', 'workspace.', 'this', 'true', 'false', 'null', STRING, NUMBER}"
    )
  }
}
