package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.testkit.TestKit
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  SamFullyQualifiedResourceId,
  SamResourceTypeName,
  SamResourceTypeNames,
  UserInfo
}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class SamExtensionsSpec
    extends TestKit(ActorSystem("SamExtensionsSpec"))
    with AnyFlatSpecLike
    with LazyLogging
    with Matchers
    with MockitoTestUtils {

  val defaultRequestContext: RawlsRequestContext =
    RawlsRequestContext(
      UserInfo(RawlsUserEmail("test"), OAuth2BearerToken("Bearer 123"), 123, RawlsUserSubjectId("abc"))
    )

  "recursiveDeleteResource" should "delete a resource and its children recursively" in {
    // Create a partial mock of the SamDAO with resource children
    // We mock listResourceChildren() and deleteResource(), but use the real implementation
    // for recursiveDeleteResource()
    val resourceTypeName = SamResourceTypeNames.workspace
    val resourceId = UUID.randomUUID().toString

    // Use SamFullyQualifiedResourceId instead of custom case class
    val childResource1 = SamFullyQualifiedResourceId("child-id-1", "child-type-1")
    val childResource2 = SamFullyQualifiedResourceId("child-id-2", "child-type-2")
    val grandchildResource = SamFullyQualifiedResourceId("grandchild-id", "grandchild-type")

    val samDAO = mock[SamDAO]

    // Mock the first level of children
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(resourceTypeName),
        ArgumentMatchers.eq(resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List(childResource1, childResource2)))

    // Mock the second level (grandchildren) - first child has a child, second child doesn't
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeName(childResource1.resourceTypeName)),
        ArgumentMatchers.eq(childResource1.resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List(grandchildResource)))

    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeName(childResource2.resourceTypeName)),
        ArgumentMatchers.eq(childResource2.resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List.empty))

    // Mock the third level (no children)
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeName(grandchildResource.resourceTypeName)),
        ArgumentMatchers.eq(grandchildResource.resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List.empty))

    // Mock all deleteResource calls to return success
    when(
      samDAO.deleteResource(
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(()))

    // Call the method under test
    Await.result(samDAO.recursiveDeleteResource(resourceTypeName, resourceId, defaultRequestContext)(global, logger),
                 Duration.Inf
    )

    // Verify that deleteResource was called for all resources in the correct order (bottom-up)
    // First verify the grandchild was deleted
    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(SamResourceTypeName(grandchildResource.resourceTypeName)),
      ArgumentMatchers.eq(grandchildResource.resourceId),
      ArgumentMatchers.any()
    )

    // Then verify both children were deleted
    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(SamResourceTypeName(childResource1.resourceTypeName)),
      ArgumentMatchers.eq(childResource1.resourceId),
      ArgumentMatchers.any()
    )

    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(SamResourceTypeName(childResource2.resourceTypeName)),
      ArgumentMatchers.eq(childResource2.resourceId),
      ArgumentMatchers.any()
    )

    // Finally verify the parent resource was deleted
    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(resourceTypeName),
      ArgumentMatchers.eq(resourceId),
      ArgumentMatchers.any()
    )
  }

  it should "handle empty child resources" in {
    val resourceTypeName = SamResourceTypeNames.workspace
    val resourceId = UUID.randomUUID().toString

    val samDAO = mock[SamDAO]

    // Mock no children
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(resourceTypeName),
        ArgumentMatchers.eq(resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List.empty))

    // Mock deleteResource to return success
    when(
      samDAO.deleteResource(
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(()))

    // Call the method under test
    Await.result(samDAO.recursiveDeleteResource(resourceTypeName, resourceId, defaultRequestContext)(global, logger),
                 Duration.Inf
    )

    // Verify that deleteResource was called only for the parent resource
    verify(samDAO, times(1)).deleteResource(
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()
    )

    verify(samDAO).deleteResource(
      ArgumentMatchers.eq(resourceTypeName),
      ArgumentMatchers.eq(resourceId),
      ArgumentMatchers.any()
    )
  }

  it should "handle 403 Forbidden errors when listing resource children" in {
    val resourceTypeName = SamResourceTypeNames.workspace
    val resourceId = UUID.randomUUID().toString
    val childResource = SamFullyQualifiedResourceId("child-id", "child-type")

    val samDAO = mock[SamDAO]

    // Mock a 403 Forbidden error for the parent resource's children
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(resourceTypeName),
        ArgumentMatchers.eq(resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(
      Future.failed(
        new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.Forbidden, "Forbidden"))
      )
    )

    // Mock successful listing for the child resource (this should never be called)
    when(
      samDAO.listResourceChildren(
        ArgumentMatchers.eq(SamResourceTypeName(childResource.resourceTypeName)),
        ArgumentMatchers.eq(childResource.resourceId),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(List.empty))

    // Mock deleteResource to return success
    when(
      samDAO.deleteResource(
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )
    ).thenReturn(Future.successful(()))

    // Call the method under test - should not throw an exception
    Await.result(samDAO.recursiveDeleteResource(resourceTypeName, resourceId, defaultRequestContext)(global, logger),
                 Duration.Inf
    )

    // Verify that listResourceChildren was called for the parent resource
    verify(samDAO).listResourceChildren(
      ArgumentMatchers.eq(resourceTypeName),
      ArgumentMatchers.eq(resourceId),
      ArgumentMatchers.any()
    )

    // Verify that no other listResourceChildren calls were made
    verify(samDAO, times(1)).listResourceChildren(
      ArgumentMatchers.any(),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()
    )

    // Verify that deleteResource was called only for the parent resource
    verify(samDAO, times(1)).deleteResource(
      ArgumentMatchers.eq(resourceTypeName),
      ArgumentMatchers.eq(resourceId),
      ArgumentMatchers.any()
    )
  }

}
