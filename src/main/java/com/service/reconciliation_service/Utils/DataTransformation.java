package com.service.reconciliation_service.Utils;

import com.service.reconciliation_service.Exception.InvalidRuleException;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static com.service.reconciliation_service.Configuration.Config.castMap;
import static org.apache.spark.sql.functions.*;

@Component
@Log4j2
public class DataTransformation {
  Dataset<Row>[] dataframes;
  private DataTransformation() {

  }

  public static void performTransformations(Map<String, Object> transformationRules,
                                            Dataset<Row>[] dfList) throws IOException {
    ReconLog.writeLog("- Starting Transformation");

    String[] files = transformationRules.keySet().toArray(new String[0]);
    for(int i=0; i< files.length ; i++){
      Map<String, Object> file = castMap(transformationRules.get(files[i]));
      ReconLog.writeLog("Loading Transformation rules for file "+(i+1)+" : " + file);
      if(file == null) continue;
      for(Map.Entry<String, Object> rule : file.entrySet()){
        dfList[i] = checkRule(rule.getKey(), rule.getValue(), dfList[i]);
      }
    }
    ReconLog.writeLog("Transformation Done");

  }

  public static Dataset<Row> checkRule(String ruleName, Object ruleValue, Dataset<Row> df) throws IOException {

    if(Objects.equals(ruleName, "createNewColumns")) {
      if (ruleValue == null) return df;
      String[] replaceColumns = df.columns();
      for (String column : replaceColumns) {
        df = df.withColumn(column, when(col(column).equalTo("NA"), "").otherwise(col(column)));
      }
      df = createNewColumns(df, castMap(ruleValue));
    } else {
        throw new InvalidRuleException("Rule name isn't defined in service");
    }
    return df;
  }

  private static Dataset<Row> createNewColumns(Dataset<Row> df,
                                               Map<String, String> rules) throws IOException {
    for(Map.Entry<String, String> newEntry : rules.entrySet()){
      String newColumnName = newEntry.getKey();
      // String[] columnsToOperate = newEntry.getValue().toArray(new String[0]);
      try {
        String expression = String.format(newEntry.getValue());
        df = df.withColumn(newColumnName, expr(expression));
      } catch (Exception e) {
        ReconLog.writeLog("Might be an Illegal expression exception \n" + e.toString());
      }
    }
    return df;
  }
}
