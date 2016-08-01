package org.broadinstitute.dsde.rawls.integrationtest

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.JndiUserDirectoryDAO
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, RawlsUserSubjectId, RawlsUser}
import org.scalatest.{Matchers, FlatSpec}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by dvoet on 11/5/15.
 */
class JndiUserDirectoryDAOSpec extends FlatSpec with Matchers with IntegrationTestConfig {

  "JndiUserDirectoryDAO" should "create/enable/disable users" in {
    val dao = new JndiUserDirectoryDAO(ldapProviderUrl, ldapUser, ldapPassword, ldapGroupDn, ldapMemberAttribute, ldapUserObjectClasses, ldapUserAttributes, ldapUserDnFormat)
    val user = RawlsUser(RawlsUserSubjectId(UUID.randomUUID().toString), RawlsUserEmail("foo@bar.com"))

    assert(!Await.result(dao.isEnabled(user.userSubjectId), Duration.Inf))
    Await.result(dao.createUser(user.userSubjectId), Duration.Inf)
    assert(!Await.result(dao.isEnabled(user.userSubjectId), Duration.Inf))
    Await.result(dao.enableUser(user.userSubjectId), Duration.Inf)
    assert(Await.result(dao.isEnabled(user.userSubjectId), Duration.Inf))
    Await.result(dao.disableUser(user.userSubjectId), Duration.Inf)
    assert(!Await.result(dao.isEnabled(user.userSubjectId), Duration.Inf))
    Await.result(dao.removeUser(user.userSubjectId), Duration.Inf)
    assert(!Await.result(dao.isEnabled(user.userSubjectId), Duration.Inf))
  }
}
