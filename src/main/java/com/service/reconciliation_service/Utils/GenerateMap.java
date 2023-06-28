package com.service.reconciliation_service.Utils;

import com.service.reconciliation_service.Exception.IllegalOperationException;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.service.reconciliation_service.Configuration.Config.castMap;
import static org.apache.spark.sql.functions.*;

@Log4j2
public class GenerateMap {

  private GenerateMap() {

  }

  public static void createMaps(Map<String, Object> mapRules, Dataset<Row>[] dfList) throws IOException {
    ReconLog.writeLog("- Merging Rows");
    String[] files = mapRules.keySet().toArray(new String[0]);
    for(int i=0; i< files.length ; i++){
      Map<String, Object> file = castMap(mapRules.get(files[i]));
      ReconLog.writeLog("Loading rules to generate map for file "+(i+1)+" : " + file);
      String uid = (String) file.get("uniqueColumn_UID");
      Map<String, String> values = castMap(file.get("value"));
      dfList[i] = mergeRows(dfList[i], uid , (ArrayList<String>) file.get("requiredColumns"), values);
    }
    ReconLog.writeLog("Dataframes Updated");
  }
  public static Dataset<Row> mergeRows(Dataset<Row> df, String uid,
                                       List<String> allColumns, Map<String, String> columns) throws IOException {
    String validInitial = "valid_initial";
    String distinctCounts = "distinct_count";
    df = df.withColumn(validInitial, lit(true));
    for(Map.Entry<String, String> column : columns.entrySet()){
      Column[] groupByColumns = new Column[allColumns.size() - 1];
      int index = 0;
      for (String currentColumn : allColumns) {
        if (currentColumn.equals(column.getKey())) {
          continue; // Skip the column mentioned in the condition
        }
        groupByColumns[index] = col(currentColumn);
        index++;
      }
      if(Objects.equals(column.getValue(), "SUM")) {
        df = df.groupBy(groupByColumns).agg(sum(column.getKey()).as(column.getKey()));
      } else if (Objects.equals(column.getValue(), "UNIQUE")) {

        int groupUIDs = (int) df.groupBy(uid).count().count();
        int groupColumns = (int) df.groupBy(uid, column.getKey()).count().count();

        if(groupColumns == groupUIDs) {

          df = df.groupBy(groupByColumns)
                  .agg(first(column.getKey()).as(column.getKey()));

        }else{

          // Add a new column 'distinct_count to the DataFrame using approx_count_distinct() function
          df = df.withColumn(distinctCounts, approx_count_distinct(column.getKey()).over(Window.partitionBy(uid)));

          df = df.withColumn("valid", when(col(distinctCounts).gt(lit(1)), lit(false))
                  .otherwise(col(validInitial)));

          df = df.withColumn(validInitial, col("valid"));
          ReconLog.writeLog("Same Groups with different values found in column : " + column.getKey());
        }
      }else
        throw new IllegalOperationException(column.getValue() + " is not a defined operation");
    }
    df = df.drop(validInitial).drop(distinctCounts);
    return df;
  }
}
