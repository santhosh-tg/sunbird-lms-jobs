package org.sunbird.jobs.samza.common;

/**
 * This interface will hold all the response key and message related to samza jobs
 */
public interface ResponseMessage {

    interface Message {
        String UNAUTHORIZED_USER = "You are not authorized.";
        String INVALID_ORG_DATA = "Given Organization Data doesn't exist in our records. Please provide a valid one";
    }

    interface Key {
        String UNAUTHORIZED_USER = "UNAUTHORIZED_USER";
        String INVALID_ORG_DATA = "INVALID_ORGANIZATION_DATA";
    }
}
