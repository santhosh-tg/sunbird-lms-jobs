package org.sunbird.jobs.samza.util;


/**
 * {
  "actor": {
    "id": "Broadcast topic notification",    //id of the actor
    "type": "System"
  },
  "eid": "BE_JOB_REQUEST",
  "edata": {
    "action": "broadcast-topic-notification-all",  //action name to check (Mandatory field)
    "request": {
        "notification": {
            "mode": "device",
            "deliveryType": "message",
            "config": {
                "topic": "publicTopic"
            },
            "ids": [],
            "template": {
                "data: "You have successfully enrolled for $courseName.",
                "params": {
                    "courseName": "Sunbird training"
                 }
             }
        }
    }         
    "iteration": 1
  },
  "ets": 1564144562948,             //system time-stamp
  "context": {
    "pdata": {
      "ver": "1.0",
      "id": "org.sunbird.platform"
    }
  },
  "mid": "LP.1564144562948.0deae013-378e-4d3b-ac16-237e3fc6149a",           //producer.system-time-stamp.uuid
  "object": {
    "id": "hash(request)",             
    "type": "TopicNotifyAll"
  }
}
 * @author manzarul
 *
 */

public class Constant {
	

}
