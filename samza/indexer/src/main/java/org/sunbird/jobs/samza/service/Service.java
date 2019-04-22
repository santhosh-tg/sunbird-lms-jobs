package org.sunbird.jobs.samza.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.models.Constants;
import org.sunbird.models.Message;
import org.sunbird.models.MessageCreator;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

/**
 * @author iostream04
 *
 */
public class Service {
  private static final String INDEX = ProjectUtil.EsIndex.sunbird.getIndexName();
 
  private static MessageCreator messageCreator = null;
  private final static String confFile = "indexMapping.conf";
  private static com.typesafe.config.Config config = null;
  private static Map<String, String> properties = null;

  public Service() {
    messageCreator = new MessageCreator();
    config = ConfigFactory.parseResources(confFile);
    initProps();
    ProjectLogger.log("Service : intialized", LoggerEnum.INFO);
  }

  private void initProps() {
    properties = new HashMap<>();
    Set<Entry<String, ConfigValue>> confs = config.entrySet();
    for (Entry<String, ConfigValue> conf : confs) {
      properties.put(conf.getKey(), conf.getValue().unwrapped().toString());
    }

  }

  public void process(Map<String, Object> messageMap) {
    Message message = messageCreator.getMessage(messageMap);
    Map<String, String> esTypesToUpdate = getEsTypeToUpdate(message);
    updateToEs(esTypesToUpdate, message);

  }

  private Map<String, String> getEsTypeToUpdate(Message message) {
    Map<String, String> esTypesToUpdate = new HashMap<>();
    String objectType = message.getObjectType();
    esTypesToUpdate.put(getIndex(objectType), getKey(objectType));
    return esTypesToUpdate;
  }

  private void updateToEs(Map<String, String> esTypesToUpdate, Message message) {
    Set<String> keys = esTypesToUpdate.keySet();
    for (String key : keys) {
      String type = key;
      Map<String, Object> properties = prepareData(message, type);
      if (!message.getOperationType().equals(Constants.DELETE)
          || (message.getOperationType().equals(Constants.DELETE) && !isHardDelete(message.getObjectType()))) {
        String identifier = (String) properties.get(esTypesToUpdate.get(key));
        ProjectLogger.log("Service: updateToEs : upserting data with identifier = " + identifier, LoggerEnum.INFO);
        ElasticSearchUtil.upsertData(INDEX, type, identifier, properties);
      } else {
        String identifier = (String) properties.get(esTypesToUpdate.get(key));
        ProjectLogger.log("Service: updateToEs : removing data with identifier = " + identifier, LoggerEnum.INFO);
        ElasticSearchUtil.removeData(INDEX, type, identifier);
      }
    }

  }

  private Map<String, Object> prepareData(Message message, String type) {
    Map<String, Object> data = null;
    data = message.getProperties();
    return data;
  }

  private String getIndex(String objectType) {
    String key = objectType + Constants.DOT + Constants.INDEX;
    if (properties.containsKey(key)) {
      return properties.get(key);
    }
    return null;

  }

  private String getKey(String objectType) {
    String key = objectType + Constants.DOT + Constants.KEY;
    if (properties.containsKey(key)) {
      return properties.get(key);
    }
    return null;
  }

  private boolean isHardDelete(String objectType) {
    String key = objectType + Constants.DOT + Constants.IS_HARD_DELETE;
    if (properties.containsKey(key)) {
      return Boolean.parseBoolean(properties.get(key));
    }
    return false;
  }

}