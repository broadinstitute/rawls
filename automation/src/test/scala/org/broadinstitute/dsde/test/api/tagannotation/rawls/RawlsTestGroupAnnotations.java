package org.broadinstitute.dsde.test.api.tagannotation.rawls;

import org.scalatest.TagAnnotation;

public class RawlsTestGroupAnnotations {
    @TagAnnotation
    @RawlsTestAnnotationBase
    public @interface AuthDomainsTest {}

    @TagAnnotation
    @RawlsTestAnnotationBase
    public @interface BillingsTest {}

    @TagAnnotation
    @RawlsTestAnnotationBase
    public @interface DataRepoSnapshotsTest {}

    @TagAnnotation
    @RawlsTestAnnotationBase
    public @interface MethodsTest {}

    @TagAnnotation
    @RawlsTestAnnotationBase
    public @interface RawlsTest {}

    @TagAnnotation
    @RawlsTestAnnotationBase
    public @interface WorkspacesTest {}
}
