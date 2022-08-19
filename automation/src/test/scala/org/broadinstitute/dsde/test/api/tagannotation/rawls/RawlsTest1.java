package org.broadinstitute.dsde.test.api.tagannotation.rawls;

import org.broadinstitute.dsde.test.api.tagannotation.rawls.RawlsTestAnnotationBase;
import org.scalatest.TagAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface RawlsTest1 {}
