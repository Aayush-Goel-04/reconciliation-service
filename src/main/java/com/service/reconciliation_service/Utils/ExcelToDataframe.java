package com.service.reconciliation_service.Utils;

import lombok.extern.log4j.Log4j2;
import org.apache.poi.ss.usermodel.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;



@Log4j2
public class ExcelToDataframe {
  private ExcelToDataframe() {
    throw new IllegalStateException("Utility class");
  }

  public static Dataset<Row> createDataframe(SparkSession spark, String filePath, String sheetName) throws IOException {

    Dataset<Row> dataframe = readExcel(spark, filePath, sheetName);
    assert dataframe != null;
    return dataframe;
  }

  private static Dataset<Row> readExcel(SparkSession spark, String filePath, String sheetName) throws IOException {
    try (FileInputStream fis = new FileInputStream(filePath);
         Workbook workbook = WorkbookFactory.create(fis)) {

      Sheet sheet = workbook.getSheetAt(0);
      if(sheetName != null){
        sheet = workbook.getSheet(sheetName);
      }
      if(sheet == null){
        throw new NullPointerException("Specified sheet not found in workbook");
      }

      org.apache.poi.ss.usermodel.Row headerRow = sheet.getRow(0);

      List<String> columnNames = new ArrayList<>();
      for (int i = 0; i < headerRow.getLastCellNum(); i++) {
        Cell cell = headerRow.getCell(i);
        columnNames.add(cell.getStringCellValue());
      }
      List<Row> rows = new ArrayList<>();
      Iterator<org.apache.poi.ss.usermodel.Row> rowIterator = sheet.iterator();
      rowIterator.next(); // Skip the header row
      while (rowIterator.hasNext()) {
        Row row = convertRow(rowIterator.next());
        rows.add(row);
      }

      StructType schema = createSchema(columnNames);
      return spark.createDataFrame(rows, schema);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    } catch (NullPointerException e){
      e.printStackTrace();
      ReconLog.writeLog(String.valueOf(e));
      return null;
    }
  }

  private static Row convertRow(org.apache.poi.ss.usermodel.Row poiRow) {
    List<String> cellValues = new ArrayList<>();
    for (int i = 0; i < poiRow.getLastCellNum(); i++) {
      Cell cell = poiRow.getCell(i, org.apache.poi.ss.usermodel.Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
      cellValues.add(cell.toString());
    }
    return RowFactory.create(cellValues.toArray());
  }

  private static StructType createSchema(List<String> columnNames) {
    List<StructField> fields = new ArrayList<>();
    for (String columnName : columnNames) {
      StructField field = DataTypes.createStructField(columnName, DataTypes.StringType, true);
      fields.add(field);
    }
    return DataTypes.createStructType(fields.toArray(StructField[]::new));
  }
}
