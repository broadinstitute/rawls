package org.broadinstitute.dsde.rawls.util

import com.typesafe.scalalogging.LazyLogging
import spray.json.{JsArray, JsObject, JsValue}

/** Mix in this trait to gain utility methods for filtering/masking JSON objects.
  */
trait JsonFilterUtils extends LazyLogging {

  /** Filters a JSON object to only those top-level keys specified in the `filters`
    * argument. Does not recurse into nested objects.
    *
    * @param in the JSON object to be filtered
    * @param filters the JSON keys to include in the return object
    * @return a new JSON object filtered to the specified keys, or a copy of the original JSON object if no filters specified.
    */
  def shallowFilterJsObject(in: JsObject, filters: Set[String]): JsObject =
    if (filters.isEmpty) {
      in.copy()
    } else {
      JsObject(in.fields.view.filterKeys(filters.contains).toMap)
    }

  /** Filters a JSON object to only those keys specified in the `filters`
    * argument. Recurse into nested objects. To specify nested keys, use dot-notation.
    *
    * @param in the JSON object to be filtered
    * @param filters the JSON keys to include in the return object
    * @return a new JSON object filtered to the specified keys, or the original JSON object if no filters specified.
    */
  def deepFilterJsObject(in: JsObject, filters: Set[String]): JsObject = {
    val SEP = '.'

    // if the user specified any top-level key PLUS a nested key underneath that top-level, remove
    // the nested keys up front for efficiency. We'll just include the top-level value in its entirety,
    // with no need to consider the nested keys.
    val cleanedFilters = filters.collect {
      case topLevel if !topLevel.contains(SEP)                                         => topLevel
      case nested if nested.contains(SEP) && !filters.contains(nested.split(SEP).head) => nested
    }

    // do we have any nested keys?
    if (!cleanedFilters.exists(_.contains(SEP))) {
      // no nested keys; perform a shallow filter.
      shallowFilterJsObject(in, cleanedFilters)
    } else {
      // the input, filtered to only top-level keys
      val baseResponse = shallowFilterJsObject(in, cleanedFilters)

      // collect the top-level keys that specified nesting
      val nestedKeys: Set[String] = cleanedFilters.filter(_.contains(SEP)).map(_.split(SEP).head)

      val partialFields: Map[String, JsValue] = nestedKeys.flatMap { key =>
        in.fields.get(key) match {
          case Some(inner: JsObject) if inner.fields.isEmpty =>
            // it's an empty JsObject; return it as-is. We need this to allow
            // responses that contain empty objects that may be semantically important.
            Option(key -> inner)
          case Some(inner: JsObject) =>
            // it's a JsObject; recursively filter it
            val filtersForThisKey = filters.filter(_.startsWith(s"$key$SEP")).map(_.replaceFirst(s"$key$SEP", ""))
            val filteredValue = deepFilterJsObject(inner, filtersForThisKey)
            if (filteredValue.fields.nonEmpty) {
              Option(key -> filteredValue)
            } else {
              // if we've filtered out all of the nested object, don't include it in the parent.
              None
            }
          case Some(arr: JsArray)       => Option(key -> deepFilterJsValue(arr, filters))
          case Some(otherJson: JsValue) =>
            // it's some other json, like a scalar; return as-is.
            logger.debug(s"nested filter specified for '$key', but '$key' does not contain an object.")
            Option(key -> otherJson)
          case _ =>
            // key doesn't exist in the original, or we somehow found something that's not a JsValue. Skip it.
            None
        }
      }.toMap

      JsObject(baseResponse.fields ++ partialFields)
    }
  }

  def deepFilterJsValue(json: JsValue, filters: Set[String]): JsValue =
    json match {
      case jso: JsObject => deepFilterJsObject(jso, filters)
      case jsa: JsArray =>
        val filteredElements = jsa.elements.map(el => deepFilterJsValue(el, filters))
        JsArray(filteredElements)
      case js => js // some other JsValue, like a scalar; pass as-is
    }

}
