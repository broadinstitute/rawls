package org.broadinstitute.dsde.rawls.util

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationContext
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsTestUtils}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class EntitySupportSpec extends AnyFlatSpec with Matchers with TestDriverComponent with RawlsTestUtils {
  import driver.api._

  "withEntityRecsForExpressionEval" should "pull entity records for a single entity given no expression" in withDefaultTestDatabase {
    val entitySupport = createEntitySupport(slickDataSource)

    withWorkspaceContext(testData.workspace) { ctx =>
      val subRq = ExpressionEvaluationContext(
        entityType = Option("Sample"),
        entityName = Option("sample1"),
        expression = None,
        rootEntityType = Some("Sample")
      )

      // Lookup succeeds
      runAndWait(entitySupport.withEntityRecsForExpressionEval(subRq, ctx, this) { entityRecs =>
        assertResult(1) {
          entityRecs.size
        }
        assertResult("sample1") {
          entityRecs.get.head.name
        }
        DBIO.successful(())
      })

      // Lookup fails because it's not there
      val notFoundExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(entitySupport.withEntityRecsForExpressionEval(subRq.copy(entityName = Some("sampel1")), ctx, this) {
          entityRecs =>
            DBIO.successful(())
        })
      }
      assertResult(StatusCodes.NotFound) {
        notFoundExc.errorReport.statusCode.get
      }

      // Lookup fails because it doesn't match the method type
      val noMatchyMethodTypeExc = intercept[RawlsExceptionWithErrorReport] {
        runAndWait(entitySupport.withEntityRecsForExpressionEval(subRq.copy(rootEntityType = Some("Pair")), ctx, this) {
          entityRecs =>
            DBIO.successful(())
        })
      }
      assertResult(StatusCodes.BadRequest) {
        noMatchyMethodTypeExc.errorReport.statusCode.get
      }
    }
  }

  private def createEntitySupport(ds: SlickDataSource) =
    new EntitySupport {
      implicit override protected val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.global
      override protected val dataSource: SlickDataSource = ds
    }

  it should "pull multiple entity records given an entity expression" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      val entitySupport = createEntitySupport(dataSource)

      withWorkspaceContext(testData.workspace) { ctx =>
        val subRq = ExpressionEvaluationContext(
          entityType = Option("SampleSet"),
          entityName = Option("sset1"),
          expression = Option("this.samples"),
          rootEntityType = Option("Sample")
        )

        // Lookup succeeds
        runAndWait(entitySupport.withEntityRecsForExpressionEval(subRq, ctx, this) { entityRecs =>
          assertResult(Set("sample1", "sample2", "sample3")) {
            entityRecs.get.map(_.name).toSet
          }
          DBIO.successful(())
        })

        // Lookup fails due to parse failure
        val badExpressionExc = intercept[RawlsExceptionWithErrorReport] {
          runAndWait(
            entitySupport.withEntityRecsForExpressionEval(subRq.copy(expression = Some("nonsense!")), ctx, this) {
              entityRecs =>
                DBIO.successful(())
            }
          )
        }
        assertResult(StatusCodes.BadRequest) {
          badExpressionExc.errorReport.statusCode.get
        }

        // Lookup fails due to no results
        val noResultExc = intercept[RawlsExceptionWithErrorReport] {
          runAndWait(
            entitySupport.withEntityRecsForExpressionEval(subRq.copy(expression = Some("this.bonk")), ctx, this) {
              entityRecs =>
                DBIO.successful(())
            }
          )
        }
        assertResult(StatusCodes.BadRequest) {
          noResultExc.errorReport.statusCode.get
        }

        // Lookup fails because it doesn't match the method type
        val noMatchyMethodTypeExc = intercept[RawlsExceptionWithErrorReport] {
          runAndWait(
            entitySupport.withEntityRecsForExpressionEval(subRq.copy(rootEntityType = Some("Pair")), ctx, this) {
              entityRecs =>
                DBIO.successful(())
            }
          )
        }
        assertResult(StatusCodes.BadRequest) {
          noMatchyMethodTypeExc.errorReport.statusCode.get
        }
      }
  }

}
