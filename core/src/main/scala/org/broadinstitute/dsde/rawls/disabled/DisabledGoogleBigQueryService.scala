package org.broadinstitute.dsde.rawls.disabled

import cats.effect.{Async, Resource}

import com.google.cloud.bigquery._
import org.broadinstitute.dsde.workbench.google2.{GoogleBigQueryService, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}
import org.typelevel.log4cats.StructuredLogger

class DisabledGoogleBigQueryService[F[_]] extends GoogleBigQueryService[F] {
  def query(queryJobConfiguration: QueryJobConfiguration, options: BigQuery.JobOption*): F[TableResult] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  def query(queryJobConfiguration: QueryJobConfiguration, jobId: JobId, options: BigQuery.JobOption*): F[TableResult] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  def runJob(jobInfo: JobInfo, options: BigQuery.JobOption*): F[Job] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  def createDataset(datasetName: String,
                    labels: Map[String, String],
                    aclBindings: Map[Acl.Role, Seq[(WorkbenchEmail, Acl.Entity.Type)]]
                   ): F[DatasetId] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  def deleteDataset(datasetName: String): F[Boolean] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  @deprecated(message = "Use getTable(BigQueryDatasetName, BigQueryTableName) instead", since = "0.21")
  def getTable(datasetName: String, tableName: String): F[scala.Option[Table]] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  def getTable(datasetName: BigQueryDatasetName, tableName: BigQueryTableName): F[scala.Option[Table]] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  def getTable(googleProjectName: GoogleProject,
               datasetName: BigQueryDatasetName,
               tableName: BigQueryTableName
              ): F[scala.Option[Table]] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  @deprecated(message = "Use getDataset(BigQueryDatasetName) instead", since = "0.21")
  def getDataset(datasetName: String): F[scala.Option[Dataset]] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  def getDataset(datasetName: BigQueryDatasetName): F[scala.Option[Dataset]] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")

  def getDataset(googleProjectName: GoogleProject, datasetName: BigQueryDatasetName): F[scala.Option[Dataset]] =
    throw new NotImplementedError("getServiceForProject is not implemented for Azure.")
}

object DisabledGoogleBigQueryService {
  def resource[F[_]: Async: StructuredLogger](): Resource[F, GoogleBigQueryService[F]] =
    Resource.pure[F, GoogleBigQueryService[F]](new DisabledGoogleBigQueryService[F])
}
