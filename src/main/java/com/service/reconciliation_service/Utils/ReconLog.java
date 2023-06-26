package com.service.reconciliation_service.Utils;

import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

@Component
public class ReconLog {

  private ReconLog() {
    throw new IllegalStateException("Utility class");
  }


  public static void writeLog(String message, boolean append) throws IOException {
    String outputPath = "reconLog.txt";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath, append))) {
      // Write the column headers
      writer.write(message);
      writer.newLine();
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeLog(String message) throws IOException {
    writeLog(message, true); // Set default value to true
  }
}
