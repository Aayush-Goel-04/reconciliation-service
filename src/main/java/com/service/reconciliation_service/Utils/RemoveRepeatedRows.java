package com.service.reconciliation_service.Utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.util.Map;

import static com.service.reconciliation_service.Configuration.Config.castMap;
@Component
public class RemoveRepeatedRows {
  private RemoveRepeatedRows(){
  }
  public static Dataset<Row>[] removeRepeated(Map<String, Object> mapRules, Dataset<Row>[] dfList) throws IOException {
    Dataset<Row>[] unmatchedDFs = new Dataset[dfList.length];
    String[] files = mapRules.keySet().toArray(new String[0]);
    ReconLog.writeLog("- Merging Repeated Rows if any");
    for (int i = 0; i < dfList.length; i++) {
      Map<String, Object> file = castMap(mapRules.get(files[i]));
      String uid = (String) file.get("uniqueColumn_UID");

      // Group by the column containing the values you want to check for repetitions
      Dataset<Row> groupedDF = dfList[i].groupBy(uid)
              .agg(count("*").as("count"))
              .filter(col("count").gt(1));

      // Extract the repeated rows into a separate DataFrame
      unmatchedDFs[i] = dfList[i].join(groupedDF, uid);

      // Filter out the repeated rows from the initial DataFrame
      dfList[i] = dfList[i].join(groupedDF, dfList[i].col(uid).equalTo(groupedDF.col(uid)), "left_anti");

    }
    ReconLog.writeLog("Removed Repeated Rows from dfs if any.");
    return unmatchedDFs;
  }
}