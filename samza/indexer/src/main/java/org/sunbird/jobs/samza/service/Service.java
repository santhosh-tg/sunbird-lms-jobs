package org.sunbird.jobs.samza.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.elasticsearch.util.ElasticSearchIndexerUtil;
import org.sunbird.models.Constants;
import org.sunbird.models.Message;
import org.sunbird.models.MessageCreator;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author iostream04
 *
 */
public class Service {
  private static ObjectMapper mapper;
  private static final String INDEX = ProjectUtil.EsIndex.sunbird.getIndexName();

  private static Map<String, String> keyMap;
  private static List<String> hardDeleteList;
  private static DecryptionService decService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
      .getDecryptionServiceInstance(null);
  private static List<String> userRelatedTables = null;
  private static List<String> orgRelatedTables = null;
  private static MessageCreator messageCreator = null;

  public Service() {
    hardDeleteList = new ArrayList<>();
    messageCreator = new MessageCreator();
    mapper = new ObjectMapper();
    keyMap = new HashMap<>();
    userRelatedTables = Arrays.asList("user", "address", "user_education", "user_job_profile", "user_org",
        "user_badge_assertion", "user_skills", "user_courses");
    orgRelatedTables = Arrays.asList("organisation", "org_external_identity");

    keyMap.put("org_external_identity", "orgid");
    keyMap.put(JsonKey.ORGANISATION, JsonKey.ORGANISATION_ID);
    keyMap.put(JsonKey.LOCATION, JsonKey.ID);
    keyMap.put(JsonKey.USER, JsonKey.ID);
    keyMap.put("address", "userid");
    keyMap.put("user_education", "userid");
    keyMap.put("user_job_profile", "userid");
    keyMap.put("user_org", "userid");
    keyMap.put("user_badge_assertion", "userid");
    keyMap.put("user_skills", "userid");
    keyMap.put("user_courses", "userid");
    keyMap.put("course_batch", JsonKey.ID);

    hardDeleteList.add(JsonKey.LOCATION);

  }

  public void process(String messageString) {
    Message message = messageCreator.getMessage(messageString);
    Map<String, String> esTypesToUpdate = getEsTypeToUpdate(message);
    updateToEs(esTypesToUpdate, message);
  }

  private Map<String, String> getEsTypeToUpdate(Message message) {
    Map<String, String> esTypesToUpdate = new HashMap<>();
    String table = message.getObjectType();
    if (orgRelatedTables.contains(table)) {
      esTypesToUpdate.put(Constants.ES_ORG, getPrimaryKey(message));
    } else if (table.equalsIgnoreCase(Constants.LOCATION)) {
      esTypesToUpdate.put(Constants.LOCATION, getPrimaryKey(message));
    } else if (userRelatedTables.contains(table)) {
      esTypesToUpdate.put(Constants.USER, getPrimaryKey(message));
    } else if (table.equalsIgnoreCase(Constants.COURSE_BATCH)) {
      esTypesToUpdate.put(Constants.ES_C_BATCH, getPrimaryKey(message));
    }
    if (table.equalsIgnoreCase(Constants.USER_COURSES)) {
      esTypesToUpdate.put(Constants.ES_USER_COURSES, Constants.ID);
    }
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
        ElasticSearchIndexerUtil.upsertData(INDEX, type, identifier, properties);
      } else {
        String identifier = (String) properties.get(esTypesToUpdate.get(key));
        ElasticSearchIndexerUtil.removeData(INDEX, type, identifier);
      }
    }

  }

  private boolean isHardDelete(String objectType) {
    if (hardDeleteList.contains(objectType)) {
      return true;
    }
    return false;
  }

  private Map<String, Object> prepareData(Message message, String type) {
    Map<String, Object> data = null;
    if (type.equalsIgnoreCase(JsonKey.ORGANISATION)) {
      data = prepareOrgData(message.getProperties());
    } else if (type.equalsIgnoreCase(JsonKey.USER)) {
      data = prepareUserData(message);
    } else {
      data = message.getProperties();
    }
    return data;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> prepareUserData(Message message) {
    Map<String, Object> data = null;
    data = message.getProperties();
    if (message.getObjectType().equals(JsonKey.USER)) {
      return data;
    } else {
      String table = message.getObjectType();
      if (table.equalsIgnoreCase(Constants.USR_EXTERNAL_IDENTITY)) {
        String userId = (String) data.get(Constants.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        esMap.put(JsonKey.PROVIDER, data.get(JsonKey.PROVIDER));
        esMap.put(JsonKey.EXTERNAL_ID, data.get(JsonKey.EXTERNAL_ID));
      } else if (table.equalsIgnoreCase(Constants.ADDRESS)) {
        String userId = (String) data.get(Constants.USER_ID);
        userId = decryptData(userId);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.ADDRESS, Constants.ID, message.getOperationType());
      } else if (table.equalsIgnoreCase(Constants.USER_EDUCATION)) {
        String userId = (String) data.get(Constants.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.EDUCATION, Constants.ID, message.getOperationType());

      } else if (table.equalsIgnoreCase(Constants.USER_JOB_PROFILE)) {
        String userId = (String) data.get(Constants.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.JOB_PROFILE, Constants.ID, message.getOperationType());

      } else if (table.equalsIgnoreCase(Constants.USER_ORG)) {
        String userId = (String) data.get(Constants.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.ORGANISATIONS, Constants.ID, message.getOperationType());
      } else if (table.equalsIgnoreCase(Constants.USER_BADGE_ASSERTION)) {
        String userId = (String) data.get(Constants.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        esMap.put(JsonKey.BADGE_ASSERTIONS, data);
        data = esMap;
      } else if (table.equalsIgnoreCase(Constants.USER_SKILLS)) {
        String userId = (String) data.get(Constants.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = updateNestedData(data, esMap, JsonKey.SKILLS, Constants.ID, message.getOperationType());
      } else if (table.equalsIgnoreCase(Constants.USER_COURSES)) {
        String userId = (String) data.get(Constants.USER_ID);
        Map<String, Object> esMap = ElasticSearchIndexerUtil.getDataByIdentifier(INDEX, JsonKey.USER, userId);
        data = addUserCourses(data);
        data = updateNestedData(data, esMap, JsonKey.BATCHES, Constants.BATCH_ID, message.getOperationType());
      }
    }
    return data;
  }

  private Map<String, Object> addUserCourses(Map<String, Object> data) {
    Map<String, Object> tempMap = new HashMap<>();
    tempMap.put(JsonKey.ENROLLED_ON, data.get(JsonKey.COURSE_ENROLL_DATE));
    tempMap.put(JsonKey.COURSE_ID, data.get(JsonKey.COURSE_ID));
    tempMap.put(JsonKey.BATCH_ID, data.get(JsonKey.BATCH_ID));
    tempMap.put(JsonKey.PROGRESS, data.get(JsonKey.PROGRESS));
    tempMap.put(JsonKey.LAST_ACCESSED_ON, data.get(JsonKey.DATE_TIME));
    return tempMap;
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> updateNestedData(Map<String, Object> data, Map<String, Object> esMap, String attribute,
      String key, String operationType) {
    if (esMap.get(attribute) != null) {
      boolean isAlreadyPresent = updateIfAlreadyExist((List) esMap.get(attribute), data, attribute, key, operationType);
      if (!isAlreadyPresent) {
        ((List) esMap.get(attribute)).add(data);
      }

    } else {
      List<Map<String, Object>> list = new ArrayList<>();
      list.add(data);
      esMap.put(attribute, list);
      data = esMap;
    }
    return data;
  }

  private boolean updateIfAlreadyExist(List<Map<String, Object>> esDataList, Map<String, Object> data, String attribute,
      String id, String operationType) {
    for (Map<String, Object> esData : esDataList) {
      if (esData.get(id).equals(data.get(id))) {
        if (operationType.equalsIgnoreCase(Constants.DELETE)
            || (data.get(JsonKey.IS_DELETED) != null && (boolean) data.get(JsonKey.IS_DELETED))) {
          esDataList.remove(esData);
        } else {
          esData = data;
        }
        return true;
      }
    }
    return false;
  }

  private String decryptData(String value) {
    return decService.decryptData(value);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> prepareOrgData(Map<String, Object> map) {
    String contactDetails = (String) map.get(JsonKey.CONTACT_DETAILS);
    if (StringUtils.isNotBlank(contactDetails)) {
      Object[] arr;
      try {
        arr = mapper.readValue(contactDetails, Object[].class);
        map.put(JsonKey.CONTACT_DETAILS, arr);
      } catch (IOException e) {
        map.put(JsonKey.CONTACT_DETAILS, new Object[] {});
      }
    } else {
      map.put(JsonKey.CONTACT_DETAILS, new Object[] {});
    }

    if (MapUtils.isNotEmpty((Map<String, Object>) map.get(JsonKey.ADDRESS))) {
      map.put(JsonKey.ADDRESS, map.get(JsonKey.ADDRESS));
    } else {
      map.put(JsonKey.ADDRESS, new HashMap<>());
    }

    return map;
  }

  private String getPrimaryKey(Message message) {
    String key = keyMap.get(message.getObjectType());
    if (key != null) {
      return key;
    }
    return null;
  }

}