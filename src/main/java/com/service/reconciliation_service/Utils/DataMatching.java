package com.service.reconciliation_service.Utils;

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
public class DataMatching {
  private DataMatching() {}

  public static Dataset<Row> startMatching(Map<String, Object> matchingRules, Dataset<Row>[] dfList,
                                           Dataset<Row>[] unmatchedDFs) throws IOException {
    ReconLog.writeLog("- Starting Matching");

    String joinCondition = (String) matchingRules.get("matchCondition");
    Dataset<Row> df1 = dfList[0];
    Dataset<Row> df2 = dfList[1];

    // Matched Entries from both dataframes
    Dataset<Row> similarEntries = df1.alias("df1").join(df2.alias("df2"), expr(joinCondition), "left_semi");

    Map<String, String> fileMatchTypes = castMap(matchingRules.get("fileMatchType"));
    String[] files = fileMatchTypes.keySet().toArray(new String[0]);


    // Unmatched entries from 1st dataframe
    if(Objects.equals(fileMatchTypes.get(files[0]), "ALL")){
      Dataset<Row> unmatchedInDF1 = df1.except(similarEntries).withColumn("reason", lit("Match not Found"));
      if(unmatchedDFs[0] == null) unmatchedDFs[0] = unmatchedInDF1;
      else unmatchedDFs[0] = unmatchedDFs[0].union(unmatchedInDF1);
    }

    // Unmatched entries for 2nd dataframe
    if(Objects.equals(fileMatchTypes.get(files[1]), "ALL")) {
      Dataset<Row> unmatchedInDF2 = df2.alias("df2").join(df1.alias("df1"), expr(joinCondition), "left_anti")
              .withColumn("reason", lit("Match not Found"));
      if (unmatchedDFs[1] == null) unmatchedDFs[1] = unmatchedInDF2;
      else unmatchedDFs[1] = unmatchedDFs[1].union(unmatchedInDF2);
    }
    ReconLog.writeLog("Matching Done");
    return similarEntries;
  }
}
