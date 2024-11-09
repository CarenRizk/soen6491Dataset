package org.apache.beam.sdk.io.common;

import java.io.IOException;
import java.net.ServerSocket;

public class NetworkTestHelper {
// Optimized by LLM: Removed unnecessary synchronized keyword for performance improvement
  public static int getAvailableLocalPort() throws IOException {
    // Optimized by LLM: Added comment to explain the purpose of the method
    // This method attempts to find an available local port by creating a ServerSocket bound to port 0.
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
// Optimized by LLM: Added error handling to log the IOException
    } catch (IOException e) {
      // Log the error or handle it gracefully
      System.err.println("Failed to get available local port: " + e.getMessage());
      throw new IOException("Unable to find an available local port", e); // Optimized by LLM: Using a more descriptive exception
    }
  }
}