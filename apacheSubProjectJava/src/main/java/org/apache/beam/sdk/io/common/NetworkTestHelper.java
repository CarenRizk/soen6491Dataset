package org.apache.beam.sdk.io.common;

import java.io.IOException;
import java.net.ServerSocket;

public class NetworkTestHelper {
  public static synchronized int getAvailableLocalPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
