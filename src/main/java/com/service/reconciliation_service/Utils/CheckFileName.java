package com.service.reconciliation_service.Utils;

import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.service.reconciliation_service.Configuration.Config.castMap;

@Log4j2
@Component
public class CheckFileName {
  private CheckFileName() {

  }

  public static List<String []> checkFileNameFormat(Map<String, Object> fileFormat , String[] filePaths) throws IOException {

    List<String []> fileInfo = new ArrayList<>();
    for(Map.Entry<String, Object> files : fileFormat.entrySet()){
      Map<String, Object> fileRules = castMap(files.getValue());

      for(String filePath : filePaths){
        File file = new File(filePath);
        String fileName = file.getName();
        if (fileName.startsWith((String) fileRules.get("nameFormat"))) {
          fileInfo.add(new String[]{filePath, (String) fileRules.get("sheetName")});
        }
      }
    }

    if(fileInfo.size() != 2){
      ReconLog.writeLog("File name starting with specified name not found");
      throw new FileNotFoundException("File name starting with specified name not found");
    }

    return fileInfo;
  }
}
