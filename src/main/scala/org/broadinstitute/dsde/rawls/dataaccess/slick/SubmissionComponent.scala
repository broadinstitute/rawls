package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import slick.dbio.Effect.Write
import slick.driver.H2Driver.api._
import slick.profile.FixedSqlAction

/**
 * Created by mbemis on 2/18/16.
 */

case class SubmissionRecord(id: UUID,
                            workspaceId: UUID,
                            submissionDate: Timestamp,
                            submitterId: String,
                            methodConfigurationId: Long,
                            submissionEntityId: Option[Long],
                            status: String
                           )

case class SubmissionValidationRecord(workflowId: Option[Long],
                                      workflowFailureId: Option[Long],
                                      valueId: Option[Long],
                                      errorText: Option[String],
                                      inputName: String
                                     )

trait SubmissionComponent {
  this: DriverComponent
    with RawlsUserComponent
    with MethodConfigurationComponent
    with EntityComponent
    with AttributeComponent
    with WorkflowComponent
    with WorkspaceComponent =>

  import driver.api._

  class SubmissionTable(tag: Tag) extends Table[SubmissionRecord](tag, "SUBMISSION") {
    def id = column[UUID]("ID", O.PrimaryKey)
    def workspaceId = column[UUID]("WORKSPACE_ID")
    def submissionDate = column[Timestamp]("DATE_SUBMITTED", O.Default(defaultTimeStamp))
    def submitterId = column[String]("SUBMITTER", O.Length(254))
    def methodConfigurationId = column[Long]("METHOD_CONFIG_ID")
    def submissionEntityId = column[Option[Long]]("ENTITY_ID")
    def status = column[String]("STATUS")

    def * = (id, workspaceId, submissionDate, submitterId, methodConfigurationId, submissionEntityId, status) <> (SubmissionRecord.tupled, SubmissionRecord.unapply)

    def workspace = foreignKey("FK_SUB_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def submitter = foreignKey("FK_SUB_SUBMITTER", submitterId, rawlsUserQuery)(_.userSubjectId)
    def methodConfiguration = foreignKey("FK_SUB_METHOD_CONFIG", methodConfigurationId, methodConfigurationQuery)(_.id)
    def submissionEntity = foreignKey("FK_SUB_ENTITY", submissionEntityId, entityQuery)(_.id.?)
  }

  class SubmissionValidationTable(tag: Tag) extends Table[SubmissionValidationRecord](tag, "SUBMISSION_VALIDATION") {
    /*
      TODO: add a constraint to ensure that one of workflowId / workflowFailureId are present
      See GAWB-288
     */
    def workflowId = column[Option[Long]]("WORKFLOW_ID")
    def workflowFailureId = column[Option[Long]]("WORKFLOW_FAILURE_ID")
    def valueId = column[Option[Long]]("VALUE_ID")
    def errorText = column[Option[String]]("ERROR_TEXT")
    def inputName = column[String]("INPUT_NAME")

    def * = (workflowId, workflowFailureId, valueId, errorText, inputName) <> (SubmissionValidationRecord.tupled, SubmissionValidationRecord.unapply)

    def workflow = foreignKey("FK_SUB_VALIDATION_WF", workflowId, workflowQuery)(_.id.?)
    def workflowFailure = foreignKey("FK_SUB_VALIDATION_FAIL", workflowFailureId, workflowFailureQuery)(_.id.?)
    def value = foreignKey("FK_SUB_VALIDATION_VAL", valueId, submissionAttributeQuery)(_.id.?)
  }

  protected val submissionValidationQuery = TableQuery[SubmissionValidationTable]

  object submissionQuery extends TableQuery(new SubmissionTable(_)) {

    type SubmissionQueryType = Query[SubmissionTable, SubmissionRecord, Seq]

    /*
      the core methods
     */

    /* gets a submission */
    def get(workspaceContext: SlickWorkspaceContext, submissionId: String): ReadAction[Option[Submission]] = {
      loadSubmission(UUID.fromString(submissionId))
    }

    /* lists all submissions in a workspace */
    def list(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[Submission]] = {
      findByWorkspaceId(workspaceContext.workspaceId).result.flatMap(recs => DBIO.sequence(recs.map{ rec =>
        loadSubmission(rec.id) map(sub => sub.get)
      }))
    }

    def listWithSubmitter(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[SubmissionListResponse]] = {
      val query = for {
        submissionRec <- findByWorkspaceId(workspaceContext.workspaceId)
        userRec <- rawlsUserQuery if (submissionRec.submitterId === userRec.userSubjectId)
        methodConfigRec <- methodConfigurationQuery if (submissionRec.methodConfigurationId === methodConfigRec.id)
        entityRec <- entityQuery if (submissionRec.submissionEntityId === entityRec.id)
      } yield (submissionRec, userRec, methodConfigRec, entityRec)

      query.result.map{recs => recs.map {
          case (submissionRec, userRec, methodConfigRec, entityRec) =>
            val user = rawlsUserQuery.unmarshalRawlsUser(userRec)
            val config = methodConfigurationQuery.unmarshalMethodConfig(methodConfigRec, Map.empty, Map.empty, Map.empty)
            val entity = AttributeEntityReference(entityRec.entityType, entityRec.name)
            val sub = unmarshalSubmission(submissionRec, config, Some(entity), Seq.empty, Seq.empty)
            new SubmissionListResponse(sub, user)
        }
      }
    }

    def countByStatus(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, Int]] = {
      filter(_.workspaceId === workspaceContext.workspaceId).groupBy(s => s.status).map { case (status, submissions) =>
        (status, submissions.length)
      }.result map { result =>
        result.toMap
      }
    }

    /* creates a submission and associated workflows in a workspace */
    def create(workspaceContext: SlickWorkspaceContext, submission: Submission): ReadWriteAction[Submission] = {

      def saveSubmissionWorkflows(workflows: Seq[Workflow]) = {
        DBIO.seq(workflows.map { case (wf) =>
          workflowQuery.save(workspaceContext, UUID.fromString(submission.submissionId), wf)
        }.toSeq: _*)
      }

      def saveSubmissionWorkflowFailures(workflowFailures: Seq[WorkflowFailure]) = {
        DBIO.seq(workflowFailures.map { case (wff) =>
          for {
            entityId <- loadSubmissionEntityId(workspaceContext.workspaceId, submission.submissionEntity)
            failureId <- (workflowFailureQuery returning workflowFailureQuery.map(_.id)) += marshalWorkflowFailure(entityId, UUID.fromString(submission.submissionId))
            messageInserts <- DBIO.sequence(wff.errors.map(message => workflowErrorQuery += WorkflowErrorRecord(failureId, message.value)))
          } yield messageInserts
        }: _*)
      }

      uniqueResult[SubmissionRecord](findById(UUID.fromString(submission.submissionId))) flatMap {
        case None =>
          val configIdAction = uniqueResult[Long](methodConfigurationQuery.findByName(
            workspaceContext.workspaceId, submission.methodConfigurationNamespace, submission.methodConfigurationName).map(_.id))

          loadSubmissionEntityId(workspaceContext.workspaceId, submission.submissionEntity) flatMap { entityId =>
            configIdAction flatMap { configId =>
              (submissionQuery += marshalSubmission(workspaceContext.workspaceId, submission, entityId, configId.get))
            } andThen
              saveSubmissionWorkflows(submission.workflows) andThen
              saveSubmissionWorkflowFailures(submission.notstarted)
          }
        case Some(_) =>
          throw new RawlsException(s"A submission already exists by the id [${submission.submissionId}]")
      }
    } map { _ => submission }

    /* updates the status of a submission */
    def updateStatus(submissionId: UUID, newStatus: SubmissionStatus): FixedSqlAction[Int, driver.api.NoStream, Write] = {
      findById(submissionId).map(_.status).update(newStatus.toString)
    }

    /* deletes a submission and all associated records */
    def delete(workspaceContext: SlickWorkspaceContext, submissionId: String): ReadWriteAction[Boolean] = {

      def deleteSubmissionWorkflows(submissionId: UUID) = {
        workflowQuery.findWorkflowsBySubmissionId(submissionId).result.flatMap { recs => DBIO.sequence(recs.map { rec =>
          workflowQuery.delete(rec.id)
        })}
      }

      uniqueResult[SubmissionRecord](findById(UUID.fromString(submissionId))) flatMap {
        case None =>
          DBIO.successful(false)
        case Some(submissionRec) => {
          deleteSubmissionWorkflows(UUID.fromString(submissionId)) andThen
            workflowQuery.findInactiveWorkflows(UUID.fromString(submissionId)).delete andThen
            findById(UUID.fromString(submissionId)).delete
        } map { count =>
          count > 0
        }
      }
    }

    /* admin action: lists all active submissions in the system */
    def listAllActiveSubmissions(): ReadAction[Seq[ActiveSubmission]] = {
      findActiveSubmissions.result.flatMap(recs => DBIO.sequence(recs.map{ rec =>
        loadActiveSubmission(rec.id)
      }))
    }

    def deleteSubmissionAction(submissionId: UUID): ReadWriteAction[Int] = {
      val workflowDeletes = workflowQuery.filter(_.submissionId === submissionId).result flatMap { result =>
        DBIO.seq(result.map(wf => workflowQuery.deleteWorkflowAction(wf.id)).toSeq:_*)
      }
      workflowDeletes andThen
        workflowQuery.deleteWorkflowErrors(submissionId) andThen
        workflowQuery.deleteWorkflowFailures(submissionId) andThen
        submissionQuery.filter(_.id === submissionId).delete
    }

    /*
      the finder methods
     */

    def findByWorkspaceId(workspaceId: UUID): SubmissionQueryType = {
      filter(rec => rec.workspaceId === workspaceId)
    }

    def findById(submissionId: UUID): SubmissionQueryType = {
      filter(rec => rec.id === submissionId)
    }

    def findActiveSubmissions: SubmissionQueryType = {
      filter(rec => rec.status inSetBind(Seq(SubmissionStatuses.Aborting.toString, SubmissionStatuses.Submitted.toString)))
    }

    /*
      the load methods
     */
    def loadSubmission(submissionId: UUID): ReadAction[Option[Submission]] = {
      uniqueResult[SubmissionRecord](findById(submissionId)) flatMap {
        case None => DBIO.successful(None)
        case Some(submissionRec) =>
          for {
            config <- methodConfigurationQuery.loadMethodConfigurationById(submissionRec.methodConfigurationId)
            workflows <- loadSubmissionWorkflows(submissionRec.id)
            entity <- loadSubmissionEntity(submissionRec.submissionEntityId)
            notStarted <- workflowQuery.loadWorkflowFailures(submissionRec.id)
          } yield Option(unmarshalSubmission(submissionRec, config.get, entity, workflows, notStarted))
      }
    }

    def loadActiveSubmission(submissionId: UUID): ReadAction[ActiveSubmission] = {
      uniqueResult[SubmissionRecord](findById(submissionId)) flatMap { rec =>
        for {
          config <- methodConfigurationQuery.loadMethodConfigurationById(rec.get.methodConfigurationId)
          workflows <- loadSubmissionWorkflows(rec.get.id)
          entity <- loadSubmissionEntity(rec.get.submissionEntityId)
          notStarted <- workflowQuery.loadWorkflowFailures(rec.get.id)
          workspace <- workspaceQuery.findById(rec.get.workspaceId.toString)
        } yield unmarshalActiveSubmission(rec.get, workspace.get, config.get, entity, workflows, notStarted)
      }
    }

    def loadSubmissionWorkflows(submissionId: UUID): ReadAction[Seq[Workflow]] = {
      // get all workflows for this submission, with their entity and messages:
      // WORKFLOW left join ENTITY left outer join WORKFLOW_MESSAGE
      val workflows = for {
        ((workflow, entity), message) <-
          workflowQuery.filter(_.submissionId === submissionId) join
            entityQuery on (_.workflowEntityId === _.id) joinLeft
            workflowMessageQuery on (_._1.id === _.workflowId)
      } yield (workflow, entity, message)

      // get all input resolutions for all workflows in this submission
      // we *could* do this in the same query as above, but since workflow:message and workflow:inputResolution
      // are both one-to-many, this would cause a cartesian product
      // WORKFLOW left outer join SUBMISSION_VALIDATION left outer join ATTRIBUTE
      val inputResolutions = for {
        (workflow, validationPair) <-
        workflowQuery.filter(_.submissionId === submissionId) joinLeft {
          submissionValidationQuery  joinLeft
          submissionAttributeQuery on (_.valueId === _.id) } on (_.id === _._1.workflowId)
      } yield (workflow, validationPair)



      inputResolutions.result.flatMap { resolutions =>
        workflows.result.map { recs =>

          // first, prepare all the input resolutions
          val groupedResolutions = resolutions.groupBy { case (workflow, validationPair) => workflow.id }

          // group the workflow/entity/message records by workflowId, which creates a map keyed by workflowId
          val recordsByWorkflowId: Map[Long, Seq[(WorkflowRecord, EntityRecord, Option[WorkflowMessageRecord])]] = recs.groupBy { case (workflow, entity, message) => workflow.id }
          // now that we're grouped, process each workflow's records, translating into a Workflow object
          recordsByWorkflowId.map{ case (workflowId, recordTuple) =>
            // because we are grouped by workflow id, and because workflow:entity is an inner join, we know that the
            // workflow and entity records are the same throughout this sequence. We can safely take the head
            // for each of them.
            val wr: WorkflowRecord = recordTuple.head._1 // first in the recordTuple
            val er: EntityRecord = recordTuple.head._2 // second in the recordTuple
            // but, the workflow messages are all different - and may not exist (i.e. be None) due to the outer join.
            // translate any/all messages that exist into a Seq[AttributeString]
            // messages are third in the recordTuple
            val messages: Seq[AttributeString] = recordTuple.map {_._3}.collect { case Some(wm) => AttributeString(wm.message) }

            // attach the input resolutions to the workflow object
            val workflowResolutions = groupedResolutions.get(wr.id).map {seq =>
              seq.collect {
                case (workflow, Some(validationPair)) =>
                  // unmarshalAttributes returns a highly nested structure; it is meant for bulk translation of
                  // attributes, but we use it here for just one
                  val attr = validationPair._2.map{attrRec =>
                    submissionAttributeQuery.unmarshalAttributes(Seq( ((attrRec.id,attrRec), None) ))
                  }.map{_.values.head}.map{_.values.head}
                SubmissionValidationValue(attr, validationPair._1.errorText, validationPair._1.inputName)
              }
            }.getOrElse(Seq.empty)

            // create the Workflow object, now that we've processed all records for this workflow.
            Workflow(wr.externalId,
              WorkflowStatuses.withName(wr.status),
              new DateTime(wr.statusLastChangedDate.getTime),
              Some(AttributeEntityReference(er.entityType, er.name)),
              workflowResolutions,
              messages
            )
          }.toSeq
        }
      }
    }

    def loadSubmissionEntity(entityId: Option[Long]): ReadAction[Option[AttributeEntityReference]] = {
      entityId match {
        case None => DBIO.successful(None)
        case Some(id) => uniqueResult[EntityRecord](entityQuery.findEntityById(id)).flatMap { rec =>
          DBIO.successful(unmarshalEntity(rec))
        }
      }
    }

    def loadSubmissionEntityId(workspaceId: UUID, entityRef: Option[AttributeEntityReference]): ReadAction[Option[Long]] = {
      entityRef match {
        case None => DBIO.successful(None)
        case Some(ref) =>
          uniqueResult(entityQuery.findEntityByName(workspaceId, ref.entityType, ref.entityName).map(_.id))
      }
    }

    def getMethodConfigOutputExpressions(submissionId: UUID): ReadAction[Map[String, String]] = {
      val query = for {
        submission <- submissionQuery if submission.id === submissionId
        methodConfigOutputs <- methodConfigurationOutputQuery if methodConfigOutputs.methodConfigId === submission.methodConfigurationId
      } yield (methodConfigOutputs.key, methodConfigOutputs.value)

      query.result.map(_.toMap)
    }

    /*
      the marshal/unmarshal methods
     */

    private def marshalSubmission(workspaceId: UUID, submission: Submission, entityId: Option[Long], configId: Long): SubmissionRecord = {
      SubmissionRecord(
        UUID.fromString(submission.submissionId),
        workspaceId,
        new Timestamp(submission.submissionDate.toDate.getTime),
        submission.submitter.userSubjectId.value,
        configId,
        entityId,
        submission.status.toString)
    }

    private def unmarshalSubmission(submissionRec: SubmissionRecord, config: MethodConfiguration, entity: Option[AttributeEntityReference], workflows: Seq[Workflow], notStarted: Seq[WorkflowFailure]): Submission = {
      Submission(
        submissionRec.id.toString,
        new DateTime(submissionRec.submissionDate.getTime),
        RawlsUserRef(RawlsUserSubjectId(submissionRec.submitterId)),
        config.namespace,
        config.name,
        entity,
        workflows.toList.sortBy(wf => wf.workflowEntity.get.entityName),
        notStarted.toList,
        SubmissionStatuses.withName(submissionRec.status))
    }

    private def unmarshalActiveSubmission(submissionRec: SubmissionRecord, workspace: Workspace, config: MethodConfiguration, entity: Option[AttributeEntityReference], workflows: Seq[Workflow], notStarted: Seq[WorkflowFailure]): ActiveSubmission = {
      ActiveSubmission(workspace.namespace, workspace.name,
        unmarshalSubmission(
          submissionRec,
          config,
          entity,
          workflows.toList.sortBy(wf => wf.workflowEntity.get.entityName),
          notStarted)
      )
    }

    private def marshalWorkflowFailure(entityId: Option[Long], submissionId: UUID): WorkflowFailureRecord = {
      WorkflowFailureRecord(0, submissionId, entityId)
    }

    private def unmarshalEntity(entityRec: Option[EntityRecord]): Option[AttributeEntityReference] = {
      entityRec.map(entity =>
        AttributeEntityReference(entity.entityType, entity.name)
      )
    }

  }
}
