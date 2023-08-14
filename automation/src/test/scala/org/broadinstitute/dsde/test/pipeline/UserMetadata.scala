package org.broadinstitute.dsde.test.pipeline

import io.circe._
import io.circe.generic.semiauto._

/**
  * Represents metadata associated with a user.
  *
  * @param email  The email address associated with the user.
  * @param type   An instance of UserType (e.g., "Owner or Regular).
  * @param bearer The Bearer token to assert authorization.
  *               
  * @example
  * {{{
  * // Sample JSON representation of an array of user metadata injected from the pipeline 
  * [
  *   {
  *     "email": "hermione.owner@quality.firecloud.org",
  *     "type": "owner",
  *     "bearer": "yada yada 1"
  *   },
  *   {
  *     "email": "harry.potter@quality.firecloud.org",
  *     "type": "regular",
  *     "bearer": "yada yada 2"
  *   },
  *   {
  *     "email": "ron.weasley@quality.firecloud.org",
  *     "type": "regular",
  *     "bearer": "yada yada 3"
  *   }
  * ]
  * }}}
  */
case class UserMetadata(email: String, `type`: UserType, bearer: String)

/**
  * Companion object containing some useful methods for UserMetadata.
  */
object UserMetadata {
  implicit val userMetadataDecoder: Decoder[UserMetadata] = deriveDecoder[UserMetadata]
}
