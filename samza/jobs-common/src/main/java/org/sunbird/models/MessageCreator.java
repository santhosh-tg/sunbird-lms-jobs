package org.sunbird.models;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.sunbird.common.models.util.CassandraPropertyReader;
import org.sunbird.common.models.util.JsonKey;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageCreator {
  private static final CassandraPropertyReader propertiesCache =
      CassandraPropertyReader.getInstance();
  private static ObjectMapper mapper = new ObjectMapper();

  @SuppressWarnings("unchecked")
  public Message getMessage(String messageString) {

    Message message = new Message();
    JsonNode res = null;
    try {
      res = mapper.readTree(messageString);
    } catch (IOException e) {
      System.out.println("error while reading message :" + messageString);
      e.printStackTrace();
    }
    message.setEts(res.get(Constants.ETS).asLong());
    message.setEventType(res.get(Constants.EVENT_TYPE).asText());
    message.setIdentifier(res.get(Constants.IDENTIFIER));
    message.setOperationType(res.get(Constants.OPERATION_TYPE).asText());
    message.setProperties(getSimpleProperties((Map<String, Object>) mapper.convertValue(res.get(Constants.EVENT), Map.class).get(Constants.PROPERTIES)));
    message.setObjectType(res.get(Constants.OBJECT_TYPE).asText());
    message.setUserId(res.get(JsonKey.USER_ID).asText());
    message.setCreatedOn(res.get(Constants.CREATED_ON).asText());
    return message;
  }
  
  public Map<String,Object> getSimpleProperties(Map<String,Object> props){
    Map<String,Object> properties = new HashMap<>();
    Set<String> keys = props.keySet();
    for(String key :keys) {
      Object obj= ((Map<String,Object>)props.get(key)).get(Constants.NV);
      String elasticKey = propertiesCache.readProperty(key);
      properties.put(elasticKey, obj);
    }
    return properties;
  }

}
