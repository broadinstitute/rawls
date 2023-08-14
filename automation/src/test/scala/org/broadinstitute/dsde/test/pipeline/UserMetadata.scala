package org.broadinstitute.dsde.test.pipeline

import io.circe._
import io.circe.generic.semiauto._

/**
  * Represents metadata associated with a user.
  *
  * @param email  The email address associated with the user.
  * @param type   An instance of UserType (e.g., "Owner or Regular).
  * @param bearer The Bearer token to assert authorization.
  */
case class UserMetadata(email: String, `type`: UserType, bearer: String)

/**
  * Companion object containing some useful methods for UserMetadata.
  */
object UserMetadata {
  implicit val userMetadataDecoder: Decoder[UserMetadata] = deriveDecoder[UserMetadata]
}
