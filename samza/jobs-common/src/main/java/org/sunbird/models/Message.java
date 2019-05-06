package org.sunbird.models;

import java.util.Map;

public class Message {
  
  public static final String TRANSACTIONAL = "transactional";

  private String objectType;
  private String eventType;
  private String operationType;
  private long ets;
  private Object identifier;
  private Map<String, Object> properties;
  private String userId;
  private String createdOn;

  public String getObjectType() {
    return objectType;
  }

  public void setObjectType(String objectType) {
    this.objectType = objectType;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getCreatedOn() {
    return createdOn;
  }

  public void setCreatedOn(String createdOn) {
    this.createdOn = createdOn;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public String getOperationType() {
    return operationType;
  }

  public void setOperationType(String operationType) {
    this.operationType = operationType;
  }

  public long getEts() {
    return ets;
  }

  public void setEts(long ets) {
    this.ets = ets;
  }

  public Object getIdentifier() {
    return identifier;
  }

  public void setIdentifier(Object identifier) {
    this.identifier = identifier;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    return "Message [objectType=" + objectType + ", eventType=" + eventType + ", operationType=" + operationType
        + ", ets=" + ets + ", identifier=" + identifier + ", properties=" + properties + ", userId=" + userId
        + ", createdOn=" + createdOn + "]";
  }

}
