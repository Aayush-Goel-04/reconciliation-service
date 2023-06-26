package com.service.reconciliation_service.Utils;

import com.service.reconciliation_service.Exception.InvalidRuleException;
import com.service.reconciliation_service.Exception.ValidationException;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import static com.service.reconciliation_service.Configuration.Config.castMap;

@Log4j2
@Component
public class FileValidation {

  private FileValidation() {
    throw new IllegalStateException("Utility class");
  }

  public static void startValidation(Map<String, Object> validationRules, Dataset<Row>[] dfList) throws IOException {
    ReconLog.writeLog("- Starting Validation");

    String[] files = validationRules.keySet().toArray(new String[0]);
    for(int i=0; i< files.length ; i++){
      Map<String, Object> file = castMap(validationRules.get(files[i]));
      ReconLog.writeLog("Loading Validation rules for file "+(i+1)+" : " + file);
      for(Map.Entry<String, Object> rule : file.entrySet()){
        checkRule(rule.getKey(), rule.getValue(), dfList[i]);
      }
    }
    ReconLog.writeLog("Validation Done");
  }

  public static void checkRule(String ruleName, Object ruleValue, Dataset<Row> df) throws IOException {

    switch (ruleName){

      case "checkColumns":
        if(!checkColumns(df, (ArrayList<String>) ruleValue)){
          throw new ValidationException("Column Names Not Found in Dataframe");
        }
        break;
      // To add new rules add new cases and checks.
      default:
        throw new InvalidRuleException("Rule name isn't defined in service");
    }
  }

  private static boolean checkColumns(Dataset<Row> df, ArrayList<String> columns) {
    return new HashSet<>(Arrays.asList(df.columns())).containsAll(columns);
  }
}
