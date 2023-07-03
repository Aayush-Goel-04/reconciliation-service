package com.service.reconciliation_service.Utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.util.Objects;

@Component
public class RemoveRepeatedRows {
  private RemoveRepeatedRows(){
  }
  public static Dataset<Row>[] removeRepeated(Dataset<Row>[] dfList) throws IOException {
    Dataset<Row>[] unmatchedDFs = new Dataset[dfList.length];
    ReconLog.writeLog("- Merging Repeated Rows if any");
    String distinctCount = "distinct_count";
    for (int i = 0; i < dfList.length; i++) {
     boolean noRepeatedRows = true;
     for(String column : dfList[i].columns()){
       if (Objects.equals(column, distinctCount)) {
         noRepeatedRows = false;
         break;
       }
     }
     if (noRepeatedRows) continue;

     // Extract the repeated rows into a separate DataFrame
     unmatchedDFs[i] = dfList[i].filter(col(distinctCount).gt(1));
     unmatchedDFs[i] = unmatchedDFs[i].withColumn("reason", lit("Unique UID couldn't be established"))
                                      .drop("valid").drop("distinct_count");
     // Filter out the repeated rows from the initial DataFrame
     dfList[i] = dfList[i].filter(col(distinctCount).equalTo(1)).dropDuplicates()
                                                        .drop("valid").drop(distinctCount);
    }
    ReconLog.writeLog("Removed Repeated Rows from dfs if any.");
    return unmatchedDFs;
  }
}