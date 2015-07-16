package org.broadinstitute.dsde.rawls.acl

import org.broadinstitute.dsde.rawls.RawlsException

object AccessLevel extends Enumeration {
  val UnknownUser = Value(0)
  val None = Value(1)
  val Read = Value(2)
  val Write = Value(3)
  val Owner = Value(4)

  def toGoogleString(v: Value): String = {
    v match {
      case Owner => "OWNER"
      case Write => "WRITER"
      case Read => "READER"
      case None => "NONE"
      case UnknownUser => "UNKNOWN USER"
      case _ => throw new RawlsException(s"invalid AccessLevel [${v}]")
    }
  }

  def fromGoogleString(s: String): Value = {
    s match {
      case "OWNER" => Owner
      case "WRITER" => Write
      case "READER" => Read
      case _ => throw new RawlsException(s"invalid AccessLevel [${s}]")
    }
  }
}
