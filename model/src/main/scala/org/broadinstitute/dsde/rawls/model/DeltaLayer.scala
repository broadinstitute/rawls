package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.joda.time.DateTime

import java.util.UUID

// TODO: nest under some version tag; we will likely need a way to track different model versions

case class Destination(workspaceId: UUID,
                       referenceId: UUID)
                       // TODO: will also need:
                       // bqDataset: String,
                       // datasetProject: GoogleProjectId,
                       // projectToBill: GoogleProjectId)

case class DeltaInsert(version: String,
                            insertId: UUID,
                            insertTimestamp: DateTime,
                            insertingUser: RawlsUserSubjectId,
                            destination: Destination,
                            inserts: Seq[EntityUpdateDefinition])





//
