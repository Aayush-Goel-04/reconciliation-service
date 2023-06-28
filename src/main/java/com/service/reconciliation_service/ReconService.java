package com.service.reconciliation_service;

import com.service.reconciliation_service.Utils.*;
import com.service.reconciliation_service.Configuration.Config;
import com.service.reconciliation_service.Configuration.ConfigLoader;
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

@Log4j2
@Component
public class ReconService {

  public static void main(String[] args) throws IOException {

    String[] paths = {"./test_files/Piramal-Recon-File 21062023.xlsx",
                      "./test_files/Piramal-Bank-File 21062023.xlsx"};

    String configPath = "./zestRecon.yml";

    try {
      // loading config file for job
      Config config = ConfigLoader.load(configPath);

      ReconLog.writeLog("- Config Loaded", false);
//      ReconLog.writeLog(config.getFileFormat().toString());
//      ReconLog.writeLog(config.getValidationRules().toString());
//      ReconLog.writeLog(config.getTransformationRules().toString());
//      ReconLog.writeLog(config.getGenerateMap().toString());
//      ReconLog.writeLog(config.getMatchingRules().toString());

      // Check File Names
      List<String []> files = CheckFileName.checkFileNameFormat(config.getFileFormat(),paths);

      // Creating Spark Session
      SparkSession spark = createSparkSession();

      @SuppressWarnings("unchecked")
      Dataset<Row>[] dfList = new Dataset[2];
      dfList[0] = ExcelToDataframe.createDataframe(spark, files.get(0)[0], files.get(0)[1]);
      dfList[1] = ExcelToDataframe.createDataframe(spark, files.get(1)[0], files.get(1)[1]);

      ReconLog.writeLog("- Dataframe Created.");

      FileValidation.startValidation(config.getValidationRules(), dfList);

      ReconLog.writeLog(Arrays.toString(dfList[0].columns()));
      ReconLog.writeLog(Arrays.toString(dfList[1].columns()));

      DataTransformation.performTransformations(config.getTransformationRules(), dfList);

      GenerateMap.createMaps(config.getGenerateMap(), dfList);

      try {
        dfList[0].coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")  // Include header in the output file
                .mode("overwrite")  // Overwrite the file if it already exists
                .save("./test_files/df_1");
        dfList[1].coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")  // Include header in the output file
                .mode("overwrite")  // Overwrite the file if it already exists
                .save("./test_files/df_2");

        // stopping current active spark session
      } catch (Exception e){
        ReconLog.writeLog("Errors while writing the dataframe \n" + e);
      }
      spark.stop();
    }catch (ClassCastException e) {
      log.error(e);
    }

  }

  public static SparkSession createSparkSession() throws IOException {
    String currentPath = new File(".").getCanonicalPath();
    ReconLog.writeLog("Current dir:" + currentPath);

    Logger.getLogger("org").setLevel(Level.OFF);

    return SparkSession.builder()
            .appName("ExcelToDataframe")
            .master("local[*]")
            .getOrCreate();
  }
}