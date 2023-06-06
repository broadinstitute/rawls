package org.broadinstitute.dsde.rawls.dataaccess

import _root_.slick.dbio.Effect.{Read, Write}
import _root_.slick.dbio.{DBIOAction, NoStream}

/**
 * Created by dvoet on 2/12/16.
 */
package object slick {
  type ReadAction[T] = DBIOAction[T, NoStream, Read]
  type WriteAction[T] = DBIOAction[T, NoStream, Write]
  type ReadWriteAction[T] = DBIOAction[T, NoStream, Read with Write]

  lazy val hostname: String =
    sys.env.getOrElse("HOSTNAME", "unknown-rawls")
}
