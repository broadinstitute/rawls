package org.broadinstitute.dsde.rawls.dataaccess.drs

import io.lemonlabs.uri.Uri
import org.broadinstitute.dsde.rawls.model.UserInfo

import java.net.URI
import scala.concurrent.Future
import scala.util.matching.Regex

trait DrsResolver {
  def drsSignedUrl(drsUrl: String, userInfo: UserInfo): Future[String]
}

object DrsResolver {
  val dosDrsUriPattern: String = "^(dos|drs)://.*"
  /*Source: https://github.com/DataBiosphere/terra-drs-hub/blob/dev/service/src/main/java/bio/terra/drshub/services/DrsProviderService.java#L24
   * <p>Hostname ID: drs://hostname/id
   * <p>https://drs.example.org/ga4gh/drs/v1/objects/314159
   * <p>Compact ID: drs://prefix:accession
   * <p>drs://dg.anv0:f51fc329-b09e-4e16-b1a9-2f60ebc428ab
   */
  val compactIdRegex: Regex = "(?<scheme>dos|drs)://(?<compactIdPrefix>(dg|drs)\\.[0-9a-z-]+):(?<path>.*)".r
  val hostNameRegex: Regex = "(?<scheme>dos|drs)://(?<hostname>[^?/:]+\\.[^?/:]+)/(?<path>.*)".r
  def getProvider(uri: String): Option[String] =
    try
      Option(new URI(uri).getHost) match {
        case Some(host) => Some(host)
        case None       => getProviderFromId(uri)
      }
    catch {
      case _: Throwable => getProviderFromId(uri)
    }

  def getProviderFromId(uri: String): Option[String] =
    hostNameRegex.findFirstMatchIn(uri) match {
      case Some(matchGroup) => Some(matchGroup.group("hostname"))
      case None =>
        compactIdRegex.findFirstMatchIn(uri) match {
          case Some(matchGroup) => Some(matchGroup.group("compactIdPrefix"))
          case None             => None
        }
    }

}
