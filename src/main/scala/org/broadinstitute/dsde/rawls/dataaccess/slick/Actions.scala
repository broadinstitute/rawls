package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.dbio.Effect.{Write, Read}
import slick.dbio.{NoStream, DBIOAction}

trait Actions {
  type ReadAction[T] = DBIOAction[T, NoStream, Read]
  type WriteAction[T] = DBIOAction[T, NoStream, Write]
  type ReadWriteAction[T] = DBIOAction[T, NoStream, Read with Write]
}
