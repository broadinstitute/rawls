package org.broadinstitute.dsde.rawls.fastpass

import akka.actor.{Actor, Props}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.fastpass.FastPassMonitor.DeleteExpiredGrants
import org.broadinstitute.dsde.rawls.model.{FastPassGrant, GoogleProjectId}
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import slick.dbio.DBIO

import scala.concurrent.Future
import scala.language.postfixOps
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

object FastPassMonitor {
  sealed trait FastPassMonitorMessage
  case object DeleteExpiredGrants extends FastPassMonitorMessage
  def props(dataSource: SlickDataSource, googleIamDAO: GoogleIamDAO, googleStorageDao: GoogleStorageDAO): Props = Props(
    new FastPassMonitor(dataSource, googleIamDAO, googleStorageDao)
  )
}

class FastPassMonitor private (dataSource: SlickDataSource,
                               googleIamDAO: GoogleIamDAO,
                               googleStorageDao: GoogleStorageDAO
) extends Actor
    with LazyLogging {
  import context.dispatcher

  override def receive: Receive = { case DeleteExpiredGrants =>
    deleteExpiredGrants()
  }

  /*
   * Remove google roles for expired grants and delete the grants from the database.
   * This is done in a transaction so that if any of the google calls fail, the grants are not deleted.
   * In order to reduce the number of api and db calls, we group the grants by google project id
   */
  private def deleteExpiredGrants(): Future[Unit] =
    for {
      _ <- Future.successful(logger.info("Starting FastPass grant cleanup"))
      grantsGroupedByEmail <- findFastPassGrantsToRemove()
    } yield Future.sequence(grantsGroupedByEmail.map { tuple =>
      val (googleProjectId, groupedFastPassGrants) = tuple
      removeFastPassGrants(googleProjectId, groupedFastPassGrants)
    })

  private def findFastPassGrantsToRemove(): Future[Iterable[(GoogleProjectId, Seq[FastPassGrant])]] =
    dataSource.inTransaction { dataAccess =>
      for {
        expiredGrants <- dataAccess.fastPassGrantQuery.findExpiredFastPassGrants()
        _ = logger.info(s"Found ${expiredGrants.size} FastPass grants to clean up")
        groupedByWorkspaceId = expiredGrants.groupBy(_.workspaceId)
        groupedByGoogleProjectId <- DBIO.sequence(groupedByWorkspaceId.map { case (workspaceId, workspaceGrants) =>
          dataAccess.workspaceQuery
            .findByIdOrFail(workspaceId)
            .map(workspace => workspace.googleProjectId -> workspaceGrants)
        })
        _ = groupedByGoogleProjectId.foreach(t =>
          logger.info(s"Found ${t._2.size} FastPass grants in ${t._1.value} to clean up")
        )
      } yield groupedByGoogleProjectId
    }

  private def removeFastPassGrants(googleProjectId: GoogleProjectId,
                                   groupedFastPassGrants: Seq[FastPassGrant]
  ): Future[Unit] =
    dataSource.inTransaction { dataAccess =>
      FastPassService
        .removeFastPassGrantsInWorkspaceProject(groupedFastPassGrants,
                                                googleProjectId,
                                                dataAccess,
                                                googleIamDAO,
                                                googleStorageDao,
                                                None
        )
        .map { errors =>
          errors.foreach { errorTuple =>
            val (error, grants) = errorTuple
            val users = grants.map(_.accountEmail.value).toSet.mkString("(", ", ", ")")
            logger.warn(
              s"Encountered error while removing FastPass for users: $users in ${googleProjectId.value}. Continuing sweep.",
              error
            )
          }
          val failedGrantIds = errors.flatMap(_._2.map(_.id)).toSet
          val successfulGrants = groupedFastPassGrants.filter(g => !failedGrantIds.contains(g.id))
          successfulGrants.map(_.accountEmail).toSet.foreach { successfulGrantRemovalEmail: WorkbenchEmail =>
            logger.info(
              s"Successfully removed FastPass grants for user ${successfulGrantRemovalEmail.value} in ${googleProjectId.value}"
            )
          }
        }
        .cleanUp {
          case Some(e) =>
            logger.warn(
              s"Encountered an error while removing FastPass grants in ${googleProjectId.value}. Continuing sweep.",
              e
            )
            DBIO.successful()
          case None => DBIO.successful()
        }
    }
}
