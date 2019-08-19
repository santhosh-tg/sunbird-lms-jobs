package org.sunbird.jobs.samza.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.config.Config;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.jobs.samza.util.*;
import org.sunbird.telemetry.dto.Target;
import org.sunbird.telemetry.dto.Telemetry;

import java.util.Map;
import java.util.HashMap;

public class UserAccountMergerService {

    private JobLogger Logger = new JobLogger(UserAccountMergerService.class);
    private Config appConfig = null;
    private UserAccountMergerMessageValidator validator = null;
    private String[] mandatoryParams = null;
    private ObjectMapper objectMapper = new ObjectMapper();

    public void initialize(Config config) throws Exception {
        JSONUtils.loadProperties(config);
        appConfig = config;
        validator = new UserAccountMergerMessageValidator();
        mandatoryParams = new String[]{
                UserAccountMergerParams.fromAccountId.name(),
                UserAccountMergerParams.toAccountId.name()};
        Logger.info("UserAccountMergerService:initialize: Service config initialized");
    }

    public void processMessage(Map<String, Object> message) throws Exception {

        Map<String, Object> userMergeEvent = null;
        Telemetry telemetryMessage = objectMapper.convertValue(message, Telemetry.class);
        Target target = telemetryMessage.getObject();
        String id = target.getId();
        String type = target.getType();
        Map<String, Object> edataMap= telemetryMessage.getEdata();
        String mergeeId = (String) edataMap.get("fromAccountId");
        String mergerId = (String) edataMap.get("toAccountId");
        String action = (String) edataMap.get("action");
        userMergeEvent = new HashMap<>();
        userMergeEvent.put(UserAccountMergerParams.fromAccountId.name(),mergeeId);
        userMergeEvent.put(UserAccountMergerParams.toAccountId.name(),mergeeId);

        validator.validateMessage(userMergeEvent, mandatoryParams);
        String hashValue = OneWayHashing.encryptVal(mergeeId+"_"+mergerId);
        if(!hashValue.equals(id)) {
            Logger.info("UserAccountMergerService:processMessage: hashValue is not matching - " + mergeeId);
        } else {
            Logger.info("UserAccountMergerService:processMessage: merging user-cert data for mergeeId:: " + mergeeId + " mergerId::" +mergerId);

            Map<String, Object> mergeCertMap = new HashMap<>();
            mergeCertMap.put(UserAccountMergerParams.fromAccountId.name(),mergeeId);
            mergeCertMap.put(UserAccountMergerParams.toAccountId.name(),mergerId);
            boolean isMerged = userCertMerge(mergeCertMap);
            if(isMerged) {
                Logger.info("UserAccountMergerService:processMessage: User cert data merged in the system with userId - " + mergerId);
            } else {
                Logger.info("UserAccountMergerService:processMessage: User cert data not merged, no certs available for  userId - " + mergeeId);
            }
        }

    }

    private boolean userCertMerge(Map<String, Object> updateMap) throws Exception {

        Map<String, Object> mergeUserCert = new HashMap<>();
        mergeUserCert.put(UserAccountMergerParams.request.name(), updateMap);
        String url = appConfig.get(UserAccountMergerParams.learner_host.name()) + appConfig.get(UserAccountMergerParams.user_cert_merge_private_api.name());
        return UserCertUtil.mergeUserCert(mergeUserCert, url);

    }
}
