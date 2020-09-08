package org.broadinstitute.dsde.rawls.dataaccess.martha

import scala.util.Try

// Both Martha v2 and v3 return a JSON that includes `googleServiceAccount` as a top-level key
// The rest of the response in both versions is currently unused
case class MarthaMinimalResponse(googleServiceAccount: Option[ServiceAccountPayload])
case class ServiceAccountPayload(data: Option[ServiceAccountEmail])
case class ServiceAccountEmail(client_email: String)

object MarthaJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val ServiceAccountEmailFormat = jsonFormat1(ServiceAccountEmail)
  implicit val MarthaV2ResponseDataFormat = jsonFormat1(ServiceAccountPayload)
  implicit val MarthaV2ResponseFormat = jsonFormat1(MarthaMinimalResponse)
}

object MarthaUtils {
  // Adapted from https://github.com/broadinstitute/martha/blob/dev/martha/martha_v3.js#L121
  val jdrHostPattern = "jade.*\\.datarepo-.*\\.broadinstitute\\.org"

  def isJDRDomain(dos: String): Boolean = {
    import com.netaporter.uri.Uri.parse

    val maybeMatch = for {
      uri <- Try(parse(dos)).toOption
      host <- uri.host
    } yield host.matches(jdrHostPattern)

    // If for some reason we can't analyze the URI, we assume it's not safe to ignore
    maybeMatch getOrElse false
  }
}

/*

The only thing Rawls cares about from Martha is `googleServiceAccount` so we don't deserialize anything else.

Example v2 response:

{
  "dos": {
    "data_object": {
      "aliases": [],
      "checksums": [
        {
          "checksum": "8bec761c8a626356eb34dbdfe20649b4",
          "type": "md5"
        }
      ],
      "created": "2020-01-15T17:46:25.694142",
      "description": "",
      "id": "dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0",
      "mime_type": "",
      "name": null,
      "size": 1386553,
      "updated": "2020-01-15T17:46:25.694148",
      "urls": [
        {
          "url": "gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_13607/Project_CCDG_13607_B01_GRM_WGS.cram.2019-02-06/Sample_HG01131/analysis/HG01131.final.cram.crai"
        }
      ],
      "version": "d87455aa"
    }
  },
  "googleServiceAccount": {
    "data": {
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "client_email": "asdf",
      "client_id": "asdf",
      "client_x509_cert_url": "asdf",
      "sergeant_key": "nope",
      "sergeant_key_id": "nope",
      "project_id": "stagingdatastage",
      "token_uri": "https://oauth2.googleapis.com/token",
      "type": "asdf asdf please let me commit"
    }
  }
}

Example v3 response:

{
  "contentType": "application/octet-stream",
  "size": 1386553,
  "timeCreated": "2020-01-15T17:46:25.694Z",
  "bucket": "fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e",
  "name": "CCDG_13607/Project_CCDG_13607_B01_GRM_WGS.cram.2019-02-06/Sample_HG01131/analysis/HG01131.final.cram.crai",
  "gsUri": "gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_13607/Project_CCDG_13607_B01_GRM_WGS.cram.2019-02-06/Sample_HG01131/analysis/HG01131.final.cram.crai",
  "googleServiceAccount": {
    "data": {
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "client_email": "asdf",
      "client_id": "asdf",
      "client_x509_cert_url": "asdf",
      "sergeant_key": "nope",
      "sergeant_key_id": "nope",
      "project_id": "stagingdatastage",
      "token_uri": "https://oauth2.googleapis.com/token",
      "type": "asdf asdf please let me commit"
    }
  },
  "hashes": {
    "md5": "8bec761c8a626356eb34dbdfe20649b4"
  },
  "timeUpdated": "2020-01-15T17:46:25.694Z"
}

*/