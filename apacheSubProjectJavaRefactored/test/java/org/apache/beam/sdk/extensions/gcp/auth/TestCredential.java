package org.apache.beam.sdk.extensions.gcp.auth;
import com.google.auth.Credentials;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/** 
 * TestCredential is a subclass of Credentials that provides a test implementation 
 * for authentication. It overrides methods to return test-specific behavior.
 */
public class TestCredential extends Credentials {
  @Override
  public String getAuthenticationType() {
    return "Test";
  }

  // Optimized by LLM: Suggestion 3 applied
  private static final Map<String, List<String>> EMPTY_MAP = Collections.emptyMap();

  @Override
  public Map<String, List<String>> getRequestMetadata() throws IOException {
    return EMPTY_MAP;
  }

  @Override
  public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
    return EMPTY_MAP;
  }

  @Override
  public boolean hasRequestMetadata() {
    return false;
  }

  @Override
  public boolean hasRequestMetadataOnly() {
    return true;
  }

  // Optimized by LLM: Suggestion 2 applied
  @Override
  public void refresh() throws IOException {}
}