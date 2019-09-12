package org.sunbird.jobs.samza.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.config.Config;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.jobs.samza.util.*;
import org.sunbird.telemetry.dto.Target;
import org.sunbird.telemetry.dto.Telemetry;

import java.util.Map;
import java.util.HashMap;

public class NotificationService {

    private JobLogger Logger = new JobLogger(NotificationService.class);
    private Config appConfig = null;
    private String[] mandatoryParams = null;
    private ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);

    public void initialize(Config config) throws Exception {
        JSONUtils.loadProperties(config);
        appConfig = config;
        Logger.info("NotificationService:initialize: Service config initialized");
    }

    public void processMessage(Map<String, Object> message) throws Exception {
       
        Map<String, Object> mergeCertMap = null;
        String mergeeId = null; 
        String mergerId = null;
        mergeCertMap = new HashMap<>();
        String hashValue = OneWayHashing.encryptVal(mergeeId+"_"+mergerId);
        if(!hashValue.equals("")) {
            Logger.info("UserAccountMergerService:processMessage: hashValue is not matching - " + mergeeId);
        } else {
            Logger.info("UserAccountMergerService:processMessage: merging user-cert data for mergeeId:: " + mergeeId + " mergerId::" +mergerId);
            boolean isMerged = userCertMerge(mergeCertMap);
            if(isMerged) {
                Logger.info("UserAccountMergerService:processMessage: User cert data merged in the system with userId - " + mergerId);
            } else {
                Logger.info("UserAccountMergerService:processMessage: User cert data not merged, no certs available for  userId - " + mergeeId);
            }
        }

    }

    private boolean userCertMerge(Map<String, Object> updateMap) throws Exception {
    	return false;

    }
}
