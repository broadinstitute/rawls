package org.broadinstitute.dsde.rawls.dataaccess

import _root_.slick.dbio.Effect.{Write, Read}
import _root_.slick.dbio.{NoStream, DBIOAction}

/**
 * Created by dvoet on 2/12/16.
 */
package object slick {
  type ReadAction[T] = DBIOAction[T, NoStream, Read]
  type WriteAction[T] = DBIOAction[T, NoStream, Write]
  type ReadWriteAction[T] = DBIOAction[T, NoStream, Read with Write]
}
