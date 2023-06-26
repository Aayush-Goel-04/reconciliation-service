package com.service.reconciliation_service.Configuration;

import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Log4j2
@Component
public class ConfigLoader {

  private ConfigLoader() {
    throw new IllegalStateException("Utility class");
  }
  public static Config load(String path) throws IOException{
    InputStream inputStream = new FileInputStream(new File(path));

    Yaml yaml = new Yaml();
    Map<String, Object> data;
    data = yaml.load(inputStream);
    Config config = new Config();
    config.loadRules(data);

    return config;
  }
}
