package org.apache.beam.sdk.extensions.gcp.auth;

import com.google.auth.Credentials;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class TestCredential extends Credentials {
  @Override
  public String getAuthenticationType() {
    return "Test";
  }

  @Override
  public Map<String, List<String>> getRequestMetadata() throws IOException {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
    return Collections.emptyMap();
  }

  @Override
  public boolean hasRequestMetadata() {
    return false;
  }

  @Override
  public boolean hasRequestMetadataOnly() {
    return true;
  }

  @Override
  public void refresh() throws IOException {}
}
