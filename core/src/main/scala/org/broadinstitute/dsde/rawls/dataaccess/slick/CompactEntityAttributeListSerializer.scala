package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.PlainArrayAttributeListSerializer

/**
  * The serialization/deserialization mode used by compact entities. As of this writing, it is a pointer to
  * PlainArrayAttributeListSerializer. We are giving it its own name to centralize and standardize compact
  * entity code on a consistent serializer.
  *
  * @link org.broadinstitute.dsde.rawls.model.AttributeListSerializer
  */
trait CompactEntityAttributeListSerializer extends PlainArrayAttributeListSerializer {}
