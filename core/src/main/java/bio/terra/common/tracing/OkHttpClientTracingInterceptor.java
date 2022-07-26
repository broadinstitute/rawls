package bio.terra.common.tracing;

import io.opencensus.contrib.http.HttpClientHandler;
import io.opencensus.contrib.http.HttpRequestContext;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.propagation.TextFormat.Setter;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

/**
 * An interceptor to add tracing headers to outgoing requests.
 *
 * <p>OkHttp interceptors are called before and after network requests, and can modify the request
 * and response objects. In this case, we're adding headers for the current traceId and span, which
 * the remote service can read to link traces across services.
 */
public class OkHttpClientTracingInterceptor implements Interceptor {
  private final Tracer tracer;
  // This looks a little odd, but OkHttp requests are immutable so the carrier type must be a
  // request builder, not a Request.
  private final HttpClientHandler<Request, Response, Request.Builder> handler;
  private static final Setter<Request.Builder> SETTER =
      new Setter<Request.Builder>() {
        @Override
        public void put(Request.Builder carrier, String key, String value) {
          carrier.header(key, value);
        }
      };

  public OkHttpClientTracingInterceptor(Tracer tracer) {
    this.tracer = tracer;
    this.handler =
        new HttpClientHandler<>(
            tracer,
            new OkHttpTracingExtractor(),
            Tracing.getPropagationComponent().getB3Format(),
            SETTER);
  }

  @Override
  public Response intercept(Chain chain) throws IOException {
    Request originalRequest = chain.request();
    Request.Builder newRequestBuilder = originalRequest.newBuilder();

    HttpRequestContext requestContext =
        handler.handleStart(tracer.getCurrentSpan(), newRequestBuilder, originalRequest);
    Request newRequest = newRequestBuilder.build();
    Response response = null;
    try {
      response = chain.proceed(newRequest);
    } catch (IOException e) {
      // Error during call
      handler.handleEnd(requestContext, newRequest, null, e);
      throw e;
    }
    // Call succeeded
    handler.handleEnd(requestContext, newRequest, response, null);
    return response;
  }
}
