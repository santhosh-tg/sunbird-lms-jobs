package org.sunbird.jobs.samza.util;

import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.jobs.samza.common.ResponseCode;

import java.text.MessageFormat;
import java.util.Map;

public class SSOAccountUpdaterMessageValidator {

    public void validateEvent(Map<String, Object> event) {
        if (StringUtils.isBlank((String) event.get(SSOAccountUpdaterParams.userExternalId.name()))) {
            throwMandatoryParamsMissingException(SSOAccountUpdaterParams.userExternalId.name());
        } else if (StringUtils.isBlank((String) event.get(SSOAccountUpdaterParams.nameFromPayload.name()))) {
            throwMandatoryParamsMissingException(SSOAccountUpdaterParams.nameFromPayload.name());
        } else if (StringUtils.isBlank((String) event.get(SSOAccountUpdaterParams.channel.name()))) {
            throwMandatoryParamsMissingException(SSOAccountUpdaterParams.channel.name());
        } else if (StringUtils.isBlank((String) event.get(SSOAccountUpdaterParams.orgExternalId.name()))) {
            throwMandatoryParamsMissingException(SSOAccountUpdaterParams.orgExternalId.name());
        } else if (StringUtils.isBlank((String) event.get(SSOAccountUpdaterParams.userId.name()))) {
            throwMandatoryParamsMissingException(SSOAccountUpdaterParams.userId.name());
        } else if (StringUtils.isBlank((String) event.get(SSOAccountUpdaterParams.firstName.name()))) {
            throwMandatoryParamsMissingException(SSOAccountUpdaterParams.firstName.name());
        }
    }

    private void throwMandatoryParamsMissingException(String fieldName) {
        throw new ProjectCommonException(
            ResponseCode.mandatoryParamsMissing.getErrorCode(),
            MessageFormat.format(ResponseCode.mandatoryParamsMissing.getErrorMessage(), fieldName),
            ResponseCode.CLIENT_ERROR.getResponseCode());
    }
}
