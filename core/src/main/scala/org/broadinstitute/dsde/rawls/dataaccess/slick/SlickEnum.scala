package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.ast.BaseTypedType
import slick.jdbc.H2Profile.MappedColumnType
import slick.jdbc.JdbcType
import slick.jdbc.H2Profile.api.stringColumnType

/**
  * Enums that extend this class can be serialized with slick (mapped to a string in the database)
  */
abstract class SlickEnum extends Enumeration {

  implicit val enumerationMapper: JdbcType[Value] with BaseTypedType[Value] =
    MappedColumnType.base[Value, String](_.toString, this.withName)

}
