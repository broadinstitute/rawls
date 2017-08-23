package org.broadinstitute.dsde.rawls.metrics

import org.apache.commons.lang3.StringUtils
import org.broadinstitute.dsde.rawls.metrics.Expansion.UriExpansion
import org.broadinstitute.dsde.rawls.model.{RawlsEnumeration, WorkspaceName}
import shapeless._
import spray.http.Uri
import spray.routing.PathMatcher.Matched
import spray.routing.{PathMatcher, PathMatcher1}

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
    * Creates an expansion for Uri which redacts pieces of the Uri matched by the provided PathMatcher.
    * @param pathMatcher a PathMatcher which matches 1 or more String elements. If the PathMatcher matches,
    *                    any matched Strings will be redacted from the Uri. Otherwise, the Uri is unchanged.
    * @return Uri Expansion instance
    */
  def redactedUriExpansion[L <: HList](pathMatcher: PathMatcher[String :: L]): Expansion[Uri] = {
    new Expansion[Uri] {
      override def makeName(uri: Uri): String = {

        @annotation.tailrec
        def hloop(path: Uri.Path, extractions: HList): Uri.Path = {
          extractions match {
            case (e: String) :: tail =>
              val newPath = Uri.Path(StringUtils.replaceOnce(path.toString(), e, "redacted"))
              hloop(newPath, tail)
            case _ :: tail =>
              hloop(path, tail)
            case _: HNil => path
          }
        }

        val updatedPath = pathMatcher.apply(uri.path) match {
          case Matched(_, extractions) => hloop(uri.path, extractions)
          case _ => uri.path
        }

        UriExpansion.makeName(uri.copy(path = updatedPath))
      }
    }

  }
}
