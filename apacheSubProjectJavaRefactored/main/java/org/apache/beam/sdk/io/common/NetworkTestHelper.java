package org.apache.beam.sdk.io.common;

import java.io.IOException;
import java.net.ServerSocket;

// Optimized by LLM: Removed unnecessary synchronized keyword for thread safety
public class NetworkTestHelper {
  // Optimized by LLM: Added a comment to explain the purpose of the method
  // This method creates a ServerSocket bound to an available local port and returns that port number.
  public static int getAvailableLocalPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      // Optimized by LLM: Handle the case where the ServerSocket might fail to bind to a port
      throw new IOException("Failed to bind to an available local port", e);
    }
  }
  
  // Optimized by LLM: Added a private constructor to prevent instantiation of the NetworkTestHelper class
  private NetworkTestHelper() {
    throw new UnsupportedOperationException("Utility class should not be instantiated");
  }
}