package org.broadinstitute.dsde.rawls.submissions

import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.rawls.model.{MethodConfiguration, MethodRepoMethod, SubmissionRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito.MockitoSugar.mock

class SubmissionValidationSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  val defaultValidMethodConfig =
    MethodConfiguration("method-namespace", "method-name", None, None, Map.empty, Map.empty, mock[MethodRepoMethod])

  behavior of "submission request static validation"

  // This test is mostly just to establish a baseline of a request that passes static validation
  it should "not throw on a valid SubmissionRequest" in {
    val submission = SubmissionRequest("name", "namespace", None, None, None, false, false)
    SubmissionRequestValidation.staticValidation(submission, defaultValidMethodConfig) shouldBe ()
  }

  val costCapThresholdValidations = Table(
    ("validation case", "costCapThresholdValue", "expected error text"),
    ("no value", None, List()),
    ("a valid value", Some(BigDecimal("98765432.01")), List()),
    ("a negative value", Some(BigDecimal("-98765432.01")), List("greater than zero")),
    ("a value that is too large",
     Some(BigDecimal("198765432.01")),
     List(s"cannot be greater than ${SubmissionRequestValidation.COST_CAP_THRESHOLD_PRECISION}")
    ),
    ("a value with an invalid scale",
     Some(BigDecimal("8765432.019")),
     List(s"scale is limited to ${SubmissionRequestValidation.COST_CAP_THRESHOLD_SCALE}")
    ),
    ("a value with multiple failed validations",
     Some(BigDecimal("-198765432.012")),
     List(
       "greater than zero",
       s"cannot be greater than ${SubmissionRequestValidation.COST_CAP_THRESHOLD_PRECISION}",
       s"${SubmissionRequestValidation.COST_CAP_THRESHOLD_SCALE}"
     )
    )
  )

  it should "validate costCapThreshold" in {
    forAll(costCapThresholdValidations) { (_, value, expectedErrors) =>
      val submission = SubmissionRequest("name", "namespace", None, None, None, false, false, costCapThreshold = value)
      if (expectedErrors.isEmpty) {
        SubmissionRequestValidation.staticValidation(submission, defaultValidMethodConfig) shouldBe ()
      } else {
        val exception = intercept[RawlsExceptionWithErrorReport] {
          SubmissionRequestValidation.staticValidation(submission, defaultValidMethodConfig)
        }
        val errorReport = exception.errorReport // .causes.head
        val errors = errorReport.causes.toList
        errors should have length expectedErrors.length
        val errorMessages = errors.map(_.message)
        expectedErrors.foreach { expectedErrorText =>
          exactly(1, errorMessages) should include(expectedErrorText)
        }
      }
    }
  }

  it should "validate workflowFailureMode" in {
    forAll(
      Table(
        ("WorkflowFailureMode value", "valid value"),
        (Some("AnInvalidValue"), false),
        (Some("ContinueWhilePossible"), true),
        (Some("NoNewCalls"), true),
        (None, true)
      )
    ) { (workflowFailureMode, validValue) =>
      val submission = SubmissionRequest("name",
                                         "namespace",
                                         None,
                                         None,
                                         None,
                                         false,
                                         false,
                                         workflowFailureMode = workflowFailureMode
      )
      if (validValue) {
        SubmissionRequestValidation.staticValidation(submission, defaultValidMethodConfig) shouldBe ()
      } else {
        val exception = intercept[RawlsExceptionWithErrorReport] {
          SubmissionRequestValidation.staticValidation(submission, defaultValidMethodConfig)
        }
        val errorReport = exception.errorReport
        val errors = errorReport.causes.toList
        errors should have length 1
        errors.head.message should include(workflowFailureMode.getOrElse("None"))
      }
    }
  }

  behavior of "entity name and type validation"

  it should "not return an error if neither the name nor the type are specified" in {
    val submission = SubmissionRequest("name", "namespace", None, None, None, false, false)
    SubmissionRequestValidation.validateEntityNameAndType(submission) shouldBe None
  }

}
