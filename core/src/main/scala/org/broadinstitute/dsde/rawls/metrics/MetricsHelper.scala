package org.broadinstitute.dsde.rawls.metrics

import cats.effect.IO
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes, AttributesBuilder}
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes.IamMemberType

object MetricsHelper {
  private val PREFIX = "rawls"
  private val FAST_PASS_FAILURE_METER_NAME = "fastpass_failure"
  private val FAST_PASS_UPDATED_METER_NAME = "fastpass_updated"
  private val FAST_PASS_QUOTA_EXCEEDED_METER_NAME = "fastpass_quota_exceeded"

  private def meter = GlobalOpenTelemetry.get().getMeter("RawlsMetrics")

  private def fastPassFailureCounter = meter
    .counterBuilder(FAST_PASS_FAILURE_METER_NAME)
    .setDescription("Number of fastpass failures")
    .setUnit("failure")
    .build

  private def fastPassUpdatedCounter = meter
    .counterBuilder(FAST_PASS_UPDATED_METER_NAME)
    .setDescription("Number of fastpass updates")
    .setUnit("update")
    .build

  private def fastPassQuotaExceededCounter = meter
    .counterBuilder(FAST_PASS_QUOTA_EXCEEDED_METER_NAME)
    .setDescription("Number of fastpass quota exceeded failures")
    .setUnit("failure")
    .build

  def incrementFastPassFailureCounter(functionName: String): IO[Unit] =
    IO(fastPassFailureCounter.add(1, Attributes.of(AttributeKey.stringKey("function"), functionName)))

  def incrementFastPassGrantedCounter(memberType: IamMemberType): IO[Unit] =
    incrementFastPassUpdatedCounter(memberType, "grant")

  def incrementFastPassRevokedCounter(memberType: IamMemberType): IO[Unit] =
    incrementFastPassUpdatedCounter(memberType, "revoke")

  def incrementCounter(name: String,
                       labels: Map[String, String] = Map.empty,
                       description: Option[String] = None
  ): IO[Unit] = {
    val metrics = meter
      .counterBuilder(s"$PREFIX/$name")
      .setDescription(description.getOrElse("none"))
      .build
    val labelBuilder = Attributes.builder()
    labels.foreach { case (k, v) =>
      labelBuilder.put(k, v)
    }
    IO(metrics.add(1, labelBuilder.build()))
  }

  private def incrementFastPassUpdatedCounter(memberType: IamMemberType, action: String) =
    IO(
      fastPassUpdatedCounter.add(1,
                                 Attributes.of(
                                   AttributeKey.stringKey("member_type"),
                                   memberType.toString,
                                   AttributeKey.stringKey("action"),
                                   action
                                 )
      )
    )

  def incrementFastPassQuotaExceededCounter(): IO[Unit] =
    IO(fastPassQuotaExceededCounter.add(1))
}
