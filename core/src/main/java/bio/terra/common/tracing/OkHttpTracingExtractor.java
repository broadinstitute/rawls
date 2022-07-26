package bio.terra.common.tracing;

import io.opencensus.contrib.http.HttpExtractor;
import okhttp3.Request;
import okhttp3.Response;

import javax.annotation.Nullable;

/**
 * Extractor to populate span fields from a Request object
 *
 * <p>OpenCensus spans have a number of fields defined, this class extracts values from OkHttp
 * request/response objects to populate those fields. Per the OpenCensus spec, "All attributes are
 * optional, but collector should make the best effort to collect those."
 */
public class OkHttpTracingExtractor extends HttpExtractor<Request, Response> {

  @Nullable
  @Override
  public String getRoute(Request request) {
    // OpenCensus spec wants this to be something like the literal string
    // "/api/workspace/{workspaceId}", whereas path would be "/api/workspace/12345".
    // We don't have the route available and this is an optional method, so return null.
    return null;
  }

  @Nullable
  @Override
  public String getUrl(Request request) {
    return request.url().toString();
  }

  @Nullable
  @Override
  public String getHost(Request request) {
    return request.url().host();
  }

  @Nullable
  @Override
  public String getMethod(Request request) {
    return request.method();
  }

  @Nullable
  @Override
  public String getPath(Request request) {
    return request.url().encodedPath();
  }

  @Nullable
  @Override
  public String getUserAgent(Request request) {
    return request.header("user-agent");
  }

  @Override
  public int getStatusCode(@Nullable Response response) {
    // Per base class, "If the response is null, this method should return 0".
    return response != null ? response.code() : 0;
  }
}
