package org.sunbird.jobs.samza.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.config.Config;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.jobs.samza.util.JSONUtils;
import org.sunbird.jobs.samza.util.JobLogger;
import org.sunbird.notification.fcm.provider.IFCMNotificationService;
import org.sunbird.notification.fcm.provider.NotificationFactory;
import org.sunbird.notification.utils.FCMResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.sunbird.jobs.samza.util.Constant;

/**
 * 
 * @author manzarul
 *
 */
public class NotificationService {
    ObjectMapper mapper = new ObjectMapper();
    private JobLogger Logger = new JobLogger(NotificationService.class);
    private Config appConfig = null;
    public void initialize(Config config) throws Exception {
        JSONUtils.loadProperties(config);
        appConfig = config;
        Logger.info("NotificationService:initialize: Service config initialized");
    }

    public void processMessage(Map<String, Object> message) throws Exception {
    	 Map<String, String> notificationMap = new HashMap<String, String>();
         Map<String,Object> edataMap =  (Map<String,Object>) message.get(Constant.EDATA);
         String requestHash = "";
         List<String> deviceIds = new ArrayList<String>();
         String topic = null;
         if(edataMap != null && edataMap.size()>0) {
        	String actionValue = (String)edataMap.get(Constant.ACTION);
        	if(Constant.ACTION_NAME.equalsIgnoreCase(actionValue)) {
        		Map<String,Object> requestMap = (Map<String,Object>) edataMap.get(Constant.REQUEST);
        		requestHash = OneWayHashing.encryptVal(mapper.writeValueAsString(requestMap));
        		Map<String,Object> tmp = (Map<String,Object>) requestMap.get(Constant.NOTIFICATION);
        		if (tmp.get(Constant.IDS) != null) {
        			deviceIds = (List<String>) tmp.get(Constant.IDS);
        		}else {
        			Map<String,Object> configMap  = (Map<String,Object>) tmp.get(Constant.CONFIG);
        			topic = (String)configMap.getOrDefault(Constant.TOPIC,"");
        		}
        		notificationMap.put(Constant.RAW_DATA, mapper.writeValueAsString(tmp.get(Constant.RAW_DATA)));
        	}else {
        	Logger.info("NotificationService:processMessage procide actioname is incorrect: " + actionValue);	
        	}
         } else {
        	 Logger.info("NotificationService:processMessage event data map is either null or empty");
         }
         Map<String,Object> objectMap   = (Map<String,Object>) message.get(Constant.OBJECT);
        if(!requestHash.equals((String)objectMap.get(Constant.ID))) {
            Logger.info("NotificationService:processMessage: hashValue is not matching - " + requestHash);
        } else {
            Logger.info("NotificationService:processMessage: calling send notification ");
            boolean isSuccess = notify(notificationMap,deviceIds,topic);
            if(isSuccess) {
            	
            } else {
            }
        }

    }
    private IFCMNotificationService service =
    	      NotificationFactory.getInstance(NotificationFactory.instanceType.httpClinet.name());
    private boolean notify(Map<String, String> notificationMap,List<String> deviceIds , String topic) throws Exception {
    	if(deviceIds != null && deviceIds.size()>0) {
         FCMResponse response = service.sendMultiDeviceNotification(deviceIds, notificationMap, false) ;   		
    		
    	}
    	return false;

    }
    
}
