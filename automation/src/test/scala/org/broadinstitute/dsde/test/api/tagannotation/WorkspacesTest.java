package org.broadinstitute.dsde.test.api.tagannotation;

import java.lang.annotation.*;
import org.scalatest.TagAnnotation;

//taggedAs
@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface WorkspacesTest {}
