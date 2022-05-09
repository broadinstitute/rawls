package org.broadinstitute.dsde.rawls.entities.exceptions

import org.broadinstitute.dsde.rawls.model.AttributeEntityReference

class DeleteEntitiesConflictException(val referringEntities: Set[AttributeEntityReference]) extends DataEntityException

class DeleteEntitiesOfTypeConflictException(val conflictCount: Long) extends DataEntityException
