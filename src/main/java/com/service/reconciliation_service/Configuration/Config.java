package com.service.reconciliation_service.Configuration;


import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Objects;


@Configuration
@Log4j2
public class Config {

  @SuppressWarnings("unchecked cast")
  public static <K, V> Map<K, V> castMap(Object obj) {
    return (Map<K, V>) obj;
  }

  private Map<String, Object> fileFormat;
  private Map<String, Object> validationRules;
  private Map<String, Object> transformationRules;
  private Map<String, Object> generateMap;
  private Map<String, Object> matchingRules;

  public Map<String, Object> getFileFormat() {
    return fileFormat;
  }

  private void setFileFormat(Map<String, Object> fileFormat) {
    this.fileFormat = fileFormat;
  }

  public Map<String, Object> getValidationRules() {
    return validationRules;
  }

  private void setValidationRules(Map<String, Object> validationRules) {
    this.validationRules = validationRules;
  }

  public Map<String, Object> getTransformationRules() {
    return transformationRules;
  }

  private void setTransformationRules(Map<String, Object> transformationRules) {
    this.transformationRules = transformationRules;
  }

  public Map<String, Object> getGenerateMap() {
    return generateMap;
  }

  private void setGenerateMap(Map<String, Object> generateMap) {
    this.generateMap = generateMap;
  }

  public Map<String, Object> getMatchingRules() {
    return matchingRules;
  }

  private void setMatchingRules(Map<String, Object> matchingRules) {
    this.matchingRules = matchingRules;
  }

  public void loadRules(Map<String, Object> data){

    String configName = (String) data.keySet().toArray()[0];
    Map<String, Object> ruleSet = castMap(data.get(configName));
    Map<String, Object> rules= castMap(ruleSet.get("rules"));

    for(String rule : rules.keySet()){
      if(Objects.equals(rule, "fileFormat")){
        setFileFormat(castMap(rules.get("fileFormat")));
      }else if(Objects.equals(rule, "validationRules")){
        setValidationRules(castMap(rules.get("validationRules")));
      }else if(Objects.equals(rule, "transformationRules")){
        setTransformationRules(castMap(rules.get("transformationRules")));
      }else if(Objects.equals(rule, "generateMap")){
        setGenerateMap(castMap(rules.get("generateMap")));
      }else if(Objects.equals(rule, "matchingRules")){
        setMatchingRules(castMap(rules.get("matchingRules")));
      }
    }

  }
}
