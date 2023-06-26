package com.service.reconciliation_service.Utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
public class WriteToText{

  private WriteToText() {
    throw new IllegalStateException("Utility class");
  }
  // Define the column widths for each column
  static int[] columnWidths = {30, 20, 20, 25, 30, 12, 20, 16, 14, 14, 14, 27, 20, 25, 50, 40, 40, 25, 50, 40, 40, 20, 20, 45, 20, 20, 15, 30, 50, 50};

  public static void writeDF(String outputPath, Dataset<Row> dataframe) {
    // Collect the rows of the DataFrame as a list
    List<Row> rows = dataframe.collectAsList();
    // Write each row to a separate line in the text file with fixed-width columns
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
      // Write the column headers
      writer.write(getFixedWidthString(dataframe.columns(), columnWidths));
      writer.newLine();
      // Write the separator line
      writer.write(getSeparatorLine(columnWidths));
      writer.newLine();

      // Write each row with fixed-width columns
      for (Row row : rows) {
        String[] values = new String[row.size()];
        for (int i = 0; i < row.size(); i++) {
          values[i] = row.get(i).toString();
        }
        writer.write(getFixedWidthString(values, columnWidths));
        writer.newLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  // Helper method to generate a fixed-width string with padding for each value
  private static String getFixedWidthString(String[] values, int[] columnWidths) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < values.length; i++) {
    String value = values[i];
    int width = columnWidths[i];
    sb.append(String.format("%-" + width + "s", value));
    }
    return sb.toString();
    }

  // Helper method to generate a separator line for the column headers
  private static String getSeparatorLine(int[] columnWidths) {
    StringBuilder sb = new StringBuilder();
    for (int width : columnWidths) {
    sb.append("-".repeat(width));
    }
    return sb.toString();
    }
}