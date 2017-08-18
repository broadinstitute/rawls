package org.broadinstitute.dsde.rawls.metrics

import org.broadinstitute.dsde.rawls.metrics.Expansion.UriExpansion
import org.broadinstitute.dsde.rawls.model.{RawlsEnumeration, WorkspaceName}
import shapeless._
import spray.http.Uri
import spray.routing.PathMatcher.Matched
import spray.routing.PathMatcher1

/**
  * Created by rtitle on 7/25/17.
  */
object RawlsExpansion {

  // Typeclass instances:

  /**
    * Implicit expansion for WorkspaceName.
    * Statsd doesn't allow slashes in metric names, so we override makeName to override
    * the default toString based implementation.
    */
  implicit object WorkspaceNameExpansion extends Expansion[WorkspaceName] {
    override def makeName(n: WorkspaceName): String = n.toString.replace('/', '.')
  }

  /**
    * Implicit expansion for RawlsEnumeration using the default makeName.
    * This takes an upper type bound {{{A <: RawlsEnumeration}}} so it can work with any
    * subtype of RawlsEnumeration.
    */
  implicit def RawlsEnumerationExpansion[A <: RawlsEnumeration[_]] = new Expansion[A] {}

  /**
    * Creates an expansion for Uri which redacts a piece of the Uri matched by the provided PathMatcher.
    * @param pathMatcher a PathMatcher which matches exactly 1 String element. If the PathMatcher matches,
    *                    that String will be redacted from the Uri. Otherwise, the Uri is unchanged.
    * @return Uri Expansion instance
    */
  def redactedUriExpansion(pathMatcher: PathMatcher1[String]): Expansion[Uri] = new Expansion[Uri] {
    override def makeName(uri: Uri): String = {
      val newPath = pathMatcher.apply(uri.path) match {
        case Matched(_, extraction :: HNil) =>
          Uri.Path(uri.path.toString().replace(extraction, "redacted"))
        case _ => uri.path
      }
      UriExpansion.makeName(uri.copy(path = newPath))
    }

  }
}
