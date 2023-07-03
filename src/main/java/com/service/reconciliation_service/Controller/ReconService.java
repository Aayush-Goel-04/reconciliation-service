package com.service.reconciliation_service.Controller;

import com.service.reconciliation_service.Utils.*;
import com.service.reconciliation_service.Configuration.Config;
import com.service.reconciliation_service.Configuration.ConfigLoader;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.time.LocalTime;
@Log4j2
@Component
public class ReconService {
@SneakyThrows
  public static void main(String[] args) throws IOException {

//    String[] paths = {"./test_files/Piramal-Recon-File 21062023.xlsx",
//                      "./test_files/Piramal-Bank-File 21062023.xlsx"};
//    String configPath = "./zestRecon_1.yml";

    String[] paths = {"./test_files/Piramal-Recon-File 21062023.xlsx",
                      "./test_files/Portfolio Report.csv"};
    String configPath = "./zestRecon_2.yml";

    ReconLog.writeLog("- Recon Started", false);
    ReconLog.writeLog(String.valueOf(LocalTime.now()));
    try {
      // loading config file for job
      Config config = ConfigLoader.load(configPath);

      ReconLog.writeLog("- Config Loaded");
//      ReconLog.writeLog(config.getFileFormat().toString());
//      ReconLog.writeLog(config.getValidationRules().toString());
//      ReconLog.writeLog(config.getTransformationRules().toString());
//      ReconLog.writeLog(config.getMergeRows().toString());
//      ReconLog.writeLog(config.getMatchingRules().toString());

      // Check File Names
      List<String []> files = CheckFileName.checkFileNameFormat(config.getFileFormat(),paths);

      // Creating Spark Session
      SparkSession spark = createSparkSession();

      @SuppressWarnings("unchecked")
      Dataset<Row>[] dfList = new Dataset[2];
      Dataset<Row>[] unmatchedDfs;
      dfList[0] = ExcelToDataframe.createDataframe(spark, files.get(0)[0], files.get(0)[1]);
      dfList[1] = ExcelToDataframe.createDataframe(spark, files.get(1)[0], files.get(1)[1]);

      ReconLog.writeLog("- Dataframe Created.");
      ReconLog.writeLog(String.valueOf(LocalTime.now()));
      ReconLog.writeLog(Arrays.toString(dfList[0].columns()));
      ReconLog.writeLog(Arrays.toString(dfList[1].columns()));

      FileValidation.startValidation(config.getValidationRules(), dfList);
      ReconLog.writeLog(String.valueOf(LocalTime.now()));
      ReconLog.writeLog(Arrays.toString(dfList[0].columns()));
      ReconLog.writeLog(Arrays.toString(dfList[1].columns()));

      DataTransformation.performTransformations(config.getTransformationRules(), dfList);
      ReconLog.writeLog(String.valueOf(LocalTime.now()));

      MergeRows.startMergingRows(config.getMergeRows(), dfList);
      ReconLog.writeLog(String.valueOf(LocalTime.now()));

      unmatchedDfs = RemoveRepeatedRows.removeRepeated(dfList);
      ReconLog.writeLog(String.valueOf(LocalTime.now()));

      Dataset<Row> matchedDF = DataMatching.startMatching(config.getMatchingRules(), dfList, unmatchedDfs);
      ReconLog.writeLog(String.valueOf(LocalTime.now()));

      try {
        ReconLog.writeLog("- Printing Recon Results");
        writeDF(matchedDF, "./test_files/matchedDF");
        writeDF(unmatchedDfs[0], "./test_files/unmatchedDF_1");
        writeDF(unmatchedDfs[1], "./test_files/unmatchedDF_2");
        ReconLog.writeLog("- Dataframe written in test-files folder");
        ReconLog.writeLog(String.valueOf(LocalTime.now()));
      } catch (Exception e){
        ReconLog.writeLog("Errors while writing the dataframe \n" + e);
      }

      spark.stop();
    }catch (ClassCastException e) {
      log.error(e);
    }

  }

  private static SparkSession createSparkSession() throws IOException {
    String currentPath = new File(".").getCanonicalPath();
    ReconLog.writeLog("Current dir:" + currentPath);

    Logger.getLogger("org").setLevel(Level.OFF);

    return SparkSession.builder()
            .appName("ExcelToDataframe")
            .master("local[*]")
            .getOrCreate();
  }

  public static void writeDF(Dataset<Row> dataframe, String name) throws IOException {
    try {
      dataframe.coalesce(1)
              .write()
              .format("csv")
              .option("header", "true")  // Include header in the output file
              .mode("overwrite")  // Overwrite the file if it already exists
              .save(name);
    } catch (NullPointerException ignored){
      ReconLog.writeLog("Dataframe is null : " + name);
    }
  }
}