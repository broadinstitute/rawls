package org.broadinstitute.dsde.rawls

import java.util.concurrent.TimeUnit

import com.codahale.metrics.SharedMetricRegistries
import com.codahale.metrics.health.SharedHealthCheckRegistries
import com.readytalk.metrics.{StatsD, StatsDReporter}
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.{Submission, Workspace}
import org.mockito.Mockito.{atLeastOnce, inOrder => mockitoInOrder}
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * Created by rtitle on 6/29/17.
  */
trait StatsDTestUtils { this: MockitoSugar with Eventually with RawlsTestUtils =>

  protected def withStatsD[T](testCode: => T)(verify: Seq[(String, String)] => Unit = _ => ()): T = {
    val statsD = mock[StatsD]
    val reporter = StatsDReporter.forRegistry(SharedMetricRegistries.getOrCreate("default"))
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(statsD)
    reporter.start(1, TimeUnit.SECONDS)
    try {
      val result = testCode
      eventually(timeout(10 seconds)) {
        val order = mockitoInOrder(statsD)
        order.verify(statsD).connect()
        val metricCaptor = captor[String]
        val valueCaptor = captor[String]
        order.verify(statsD, atLeastOnce).send(metricCaptor.capture, valueCaptor.capture)
        order.verify(statsD).close()
        verify(metricCaptor.getAllValues.asScala.zip(valueCaptor.getAllValues.asScala))
      }
      result
    } finally {
      reporter.stop()
      SharedMetricRegistries.clear()
      SharedHealthCheckRegistries.clear()
    }
  }

  protected def expectedMetric(workspace: Workspace, submission: Submission, workflowStatus: WorkflowStatus, name: Option[String], expectedTimes: Int): (String, String) =
    (s"test.workspace.${workspace.toWorkspaceName.toString.replace('/', '.')}.submission.${submission.submissionId}.workflowStatus.${workflowStatus.toString}${name.map(n => s".$n").getOrElse("")}", expectedTimes.toString)

  protected def expectedMetric(workspace: Workspace, submission: Submission, workflowStatus: WorkflowStatus, name: Option[String] = None): (String, String) =
    expectedMetric(workspace, submission, workflowStatus, name, submission.workflows.size)


}
