package org.sunbird.validator;

import java.text.MessageFormat;
import java.util.Map;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.common.responsecode.ResponseMessage;
import org.sunbird.models.Constants;

public class MessageValidator {

  public void validateMessage(Map<String, Object> message) {
    if (!message.containsKey(Constants.OBJECT_TYPE)) {
      ProjectCommonException.throwClientErrorException(ResponseCode.mandatoryParamsMissing, Constants.OBJECT_TYPE);
    } else if (!message.containsKey(Constants.OPERATION_TYPE)) {
      ProjectCommonException.throwClientErrorException(ResponseCode.mandatoryParamsMissing, Constants.OPERATION_TYPE);
    } else if (!message.containsKey(Constants.EVENT)) {
      ProjectCommonException.throwClientErrorException(ResponseCode.mandatoryParamsMissing, Constants.EVENT);
    } else if (!message.containsKey(Constants.IDENTIFIER)) {
      throw new ProjectCommonException(ResponseCode.mandatoryParamsMissing.getErrorCode(),
          MessageFormat.format(ResponseCode.mandatoryParamsMissing.getErrorMessage(), Constants.IDENTIFIER),
          ResponseCode.mandatoryParamsMissing.getResponseCode());
    } else if (!message.containsKey(Constants.EVENT_TYPE)) {
      ProjectCommonException.throwClientErrorException(ResponseCode.mandatoryParamsMissing, Constants.EVENT_TYPE);
    }
  }

}
