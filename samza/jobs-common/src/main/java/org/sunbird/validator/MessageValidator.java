package org.sunbird.validator;

import java.text.MessageFormat;
import java.util.Map;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.models.Constants;

public class MessageValidator {

  public void validateMessage(Map<String, Object> message) {

    validate(message, Constants.IDENTIFIER, ResponseCode.mandatoryParamsMissing);
    validate(message, Constants.OBJECT_TYPE, ResponseCode.mandatoryParamsMissing);
    validate(message, Constants.OPERATION_TYPE, ResponseCode.mandatoryParamsMissing);
    validate(message, Constants.EVENT, ResponseCode.mandatoryParamsMissing);
    validate(message, Constants.EVENT_TYPE, ResponseCode.mandatoryParamsMissing);
  }

  public void validate(Map<String, Object> message, String attribute, ResponseCode responseCode) {
    if (!message.containsKey(attribute)) {
      throw new ProjectCommonException(ResponseCode.mandatoryParamsMissing.getErrorCode(),
          MessageFormat.format(ResponseCode.mandatoryParamsMissing.getErrorMessage(), attribute),
          ResponseCode.mandatoryParamsMissing.getResponseCode());
    }
  }

}
