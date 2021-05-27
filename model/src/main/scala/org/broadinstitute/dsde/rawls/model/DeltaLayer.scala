package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.joda.time.DateTime

import java.util.UUID

// TODO AS-770: nest under some version tag; we will likely need a way to track different model versions
// TODO AS-770: flesh out with everything these model classes need
// TODO AS-770: use something more generic than Seq[EntityUpdateDefinition]?
// TODO AS-770: JSON de/serializers
// TODO AS-770: class naming?

case class Destination(workspaceId: UUID,
                       referenceId: UUID)
                       // will also need, at least:
                       // bqDataset: String,
                       // datasetProject: GoogleProjectId,
                       // projectToBill: GoogleProjectId)

case class DeltaInsert(version: String,
                       insertId: UUID,
                       insertTimestamp: DateTime,
                       insertingUser: RawlsUserSubjectId,
                       destination: Destination,
                       inserts: Seq[EntityUpdateDefinition])
