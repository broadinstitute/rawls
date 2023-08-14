package org.broadinstitute.dsde.test.pipeline

import io.circe.Decoder

/**
  * Enum-like sealed trait representing the user type.
  */
sealed trait UserType { def title: String }

/**
  * Enum-like user type for owners.
  */
case object Owner extends UserType { def title = "owner" }

/**
  * Enum-like user type for regular users.
  */
case object Regular extends UserType { def title = "regular" }

/**
  * Companion object containing some useful methods for UserType.
  */
object UserType {
  implicit val userTypeDecoder: Decoder[UserType] = Decoder.decodeString.emap {
    case "owner"   => Right(Owner)
    case "regular" => Right(Regular)
    case other     => Left(s"Unknown user type: $other")
  }
}
