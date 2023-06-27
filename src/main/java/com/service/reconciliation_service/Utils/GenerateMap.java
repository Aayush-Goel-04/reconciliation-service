package com.service.reconciliation_service.Utils;

import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Map;

import static com.service.reconciliation_service.Configuration.Config.castMap;
@Log4j2
public class GenerateMap {

  private GenerateMap() {
    throw new IllegalStateException("Utility class");
  }

  public static Map<String, Object>[] createMaps(Map<String, Object> mapRules, Dataset<Row>[] dfList) throws IOException {
    ReconLog.writeLog("- Creating Maps");
    Map<String, Object>[] maps = new Map[dfList.length];
    String[] files = mapRules.keySet().toArray(new String[0]);
    for(int i=0; i< files.length ; i++){
      Map<String, Object> file = castMap(mapRules.get(files[i]));
      ReconLog.writeLog("Loading rules to generate map for file "+(i+1));
      log.info("Map Rules");
      String uid = (String) file.get("uniqueColumn_UID");
      log.info(dfList[i].col(uid));
      log.info("values class is");
      log.info(file.get("value").getClass());
    }
    ReconLog.writeLog("Maps Created");
    return maps;
  }
}
