package org.sunbird.validator;

import static org.hamcrest.CoreMatchers.instanceOf;

import java.text.MessageFormat;
import org.sunbird.models.Message;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.StringFormatter;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.models.Constants;

public class MessageValidator {

  public void validateMessage(Map<String, Object> message) {

    validate(message, Constants.IDENTIFIER, ResponseCode.mandatoryParamsMissing);
    validate(message, Constants.OBJECT_TYPE, ResponseCode.mandatoryParamsMissing);
    validate(message, Constants.OPERATION_TYPE, ResponseCode.mandatoryParamsMissing);
    validate(message, Constants.EVENT_TYPE, ResponseCode.mandatoryParamsMissing);
    validateEvent(message, Constants.EVENT, ResponseCode.mandatoryParamsMissing);
    validateEventType(message);
    validateOperationType(message);
  }

  @SuppressWarnings("unchecked")
  private void validateEvent(Map<String, Object> message, String attribute, ResponseCode mandatoryparamsmissing) {
    if (!message.containsKey(attribute) || (!(message.get(attribute) instanceof Map))
        || (!MapUtils.isNotEmpty((Map<String, Object>) message.get(attribute)))) {
      throw new ProjectCommonException(ResponseCode.mandatoryParamsMissing.getErrorCode(),
          MessageFormat.format(ResponseCode.mandatoryParamsMissing.getErrorMessage(), attribute),
          ResponseCode.mandatoryParamsMissing.getResponseCode());
    }

  }

  private void validateEventType(Map<String, Object> message) {
    if (!Message.TRANSACTIONAL.equals(message.get(Constants.EVENT_TYPE))) {
      throw new ProjectCommonException(ResponseCode.dataTypeError.getErrorCode(),
          MessageFormat.format(ResponseCode.dataTypeError.getErrorMessage(), Message.TRANSACTIONAL),
          ResponseCode.dataTypeError.getResponseCode());
    }

  }

  private void validateOperationType(Map<String, Object> message) {
    if ((!Constants.UPSERT.equals(message.get(Constants.OPERATION_TYPE)))
        && (!Constants.DELETE.equals(message.get(Constants.OPERATION_TYPE)))) {
      throw new ProjectCommonException(ResponseCode.dataTypeError.getErrorCode(),
          MessageFormat.format(ResponseCode.dataTypeError.getErrorMessage(), Constants.OPERATION_TYPE,
              StringFormatter.joinByComma(Constants.UPSERT, Constants.DELETE)),
          ResponseCode.dataTypeError.getResponseCode());
    }

  }

  public void validate(Map<String, Object> message, String attribute, ResponseCode responseCode) {
    if (!message.containsKey(attribute) || StringUtils.isBlank((String) message.get(attribute))) {
      throw new ProjectCommonException(ResponseCode.mandatoryParamsMissing.getErrorCode(),
          MessageFormat.format(ResponseCode.mandatoryParamsMissing.getErrorMessage(), attribute),
          ResponseCode.mandatoryParamsMissing.getResponseCode());
    }
  }

}
