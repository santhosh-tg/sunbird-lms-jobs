package org.sunbird.jobs.samza.service;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.samza.config.Config;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Map;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.jobs.samza.common.ResponseCode;
import org.sunbird.jobs.samza.util.SearchUtil;
import org.sunbird.jobs.samza.util.UserUtil;
import org.sunbird.jobs.samza.util.JSONUtils;
import org.sunbird.jobs.samza.util.JobLogger;
import org.sunbird.jobs.samza.util.SSOAccountUpdaterParams;
import org.sunbird.jobs.samza.util.TestUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({OkHttpClient.class, Response.class, Call.class, JSONUtils.class, JobLogger.class, ResponseBody.class, SearchUtil.class, UserUtil.class})
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*", "javax.security.*"})
public class SSOAccountUpdaterServiceTest {

    private static final String JSON_EXTN = ".json";
    private static final String RESOURCE_PATH = "SSOAccountUpdaterServiceTest/";
    private Response response;
    private ResponseBody responseBody;
    private Config config;

    @Before
    public void setup() throws Exception {
        //mock logging
        JobLogger logger = PowerMockito.mock(JobLogger.class);
        PowerMockito.whenNew(JobLogger.class).withAnyArguments().thenReturn(logger);
        PowerMockito.doNothing().when(logger).info(Mockito.anyString());

        //mock samza config
        config = PowerMockito.mock(Config.class);
        PowerMockito.mockStatic(JSONUtils.class);
        PowerMockito.doNothing().when(JSONUtils.class, "loadProperties", config);

        //mock network call
        OkHttpClient client = PowerMockito.mock(OkHttpClient.class);
        Call call = PowerMockito.mock(Call.class);
        response = PowerMockito.mock(Response.class);
        responseBody = PowerMockito.mock(ResponseBody.class);
        PowerMockito.whenNew(OkHttpClient.class).withAnyArguments().thenReturn(client);
        PowerMockito.when(client.newCall(Mockito.any())).thenReturn(call);
        PowerMockito.when(call.execute()).thenReturn(response);
    }

    @Test
    @PrepareForTest({OkHttpClient.class, Response.class, Call.class, JSONUtils.class, JobLogger.class, ResponseBody.class, SearchUtil.class, UserUtil.class})
    public void testSSOAccountUpdateSuccessWithMandatoryFields() throws Exception {
        setValidConfigurations();
        PowerMockito.when(response.body()).thenReturn(responseBody);
        PowerMockito.when(response.code()).thenReturn(200);
        PowerMockito.when(responseBody.string()).thenReturn(getJSONFileAsString("org-search-org-found-response"));

        SSOAccountUpdaterService service = new SSOAccountUpdaterService();
        service.initialize(config);
        service.processMessage(getJSONFileAsMap("message-with-mandatory-fields"));
    }

    @Test
    @PrepareForTest({OkHttpClient.class, Response.class, Call.class, JSONUtils.class, JobLogger.class, ResponseBody.class, SearchUtil.class, UserUtil.class})
    public void testSSOAccountUpdateSuccessWithAllFields() throws Exception {
        setValidConfigurations();
        PowerMockito.when(response.body()).thenReturn(responseBody);
        PowerMockito.when(response.code()).thenReturn(200);
        PowerMockito.when(responseBody.string()).thenReturn(getJSONFileAsString("org-search-org-found-response"));

        SSOAccountUpdaterService service = new SSOAccountUpdaterService();
        service.initialize(config);
        service.processMessage(getJSONFileAsMap("message-with-all-fields"));
    }

    @Test
    @PrepareForTest({OkHttpClient.class, Response.class, Call.class, JSONUtils.class, JobLogger.class, ResponseBody.class, SearchUtil.class, UserUtil.class})
    public void testSSOAccountUpdateSuccessWithOnlyNameChange() throws Exception {
        setValidConfigurations();
        PowerMockito.when(response.body()).thenReturn(responseBody);
        PowerMockito.when(response.code()).thenReturn(200);
        PowerMockito.when(responseBody.string()).thenReturn(getJSONFileAsString("org-search-org-found-response"));

        SSOAccountUpdaterService service = new SSOAccountUpdaterService();
        service.initialize(config);
        service.processMessage(getJSONFileAsMap("message-with-name-update"));
    }

    @Test
    @PrepareForTest({OkHttpClient.class, Response.class, Call.class, JSONUtils.class, JobLogger.class, ResponseBody.class, SearchUtil.class, UserUtil.class})
    public void testSSOAccountUpdateSkipScenario() throws Exception {
        setValidConfigurations();
        PowerMockito.when(response.body()).thenReturn(responseBody);
        PowerMockito.when(response.code()).thenReturn(200);
        PowerMockito.when(responseBody.string()).thenReturn(getJSONFileAsString("org-search-org-found-response"));

        SSOAccountUpdaterService service = new SSOAccountUpdaterService();
        service.initialize(config);
        service.processMessage(getJSONFileAsMap("message-skip-update"));
    }

    @Test
    @PrepareForTest({OkHttpClient.class, Response.class, Call.class, JSONUtils.class, JobLogger.class, ResponseBody.class, SearchUtil.class, UserUtil.class})
    public void testSSOAccountUpdateSuccessWithMissingRoleField() throws Exception {
        setValidConfigurations();
        PowerMockito.when(response.body()).thenReturn(responseBody);
        PowerMockito.when(response.code()).thenReturn(200);
        PowerMockito.when(responseBody.string()).thenReturn(getJSONFileAsString("org-search-org-found-response"));

        SSOAccountUpdaterService service = new SSOAccountUpdaterService();
        service.initialize(config);
        service.processMessage(getJSONFileAsMap("message-with-missing-role-field"));
    }

    @Test(expected = ProjectCommonException.class)
    @PrepareForTest({OkHttpClient.class, Response.class, Call.class, JSONUtils.class, JobLogger.class, ResponseBody.class, SearchUtil.class, UserUtil.class})
    public void testSSOAccountUpdateFailureDueToOrgNotFound() throws Exception {
        setValidConfigurations();
        PowerMockito.when(response.body()).thenReturn(responseBody);
        PowerMockito.when(response.code()).thenReturn(200);
        PowerMockito.when(responseBody.string()).thenReturn(getJSONFileAsString("org-search-org-not-found-response"));

        SSOAccountUpdaterService service = new SSOAccountUpdaterService();
        service.initialize(config);
        try {
            service.processMessage(getJSONFileAsMap("message-with-all-fields"));
        } catch (ProjectCommonException e) {
            assertEquals(e.getCode(), ResponseCode.invalidOrgData.getErrorCode());
            throw e;
        }
    }

    @Test(expected = ProjectCommonException.class)
    @PrepareForTest({OkHttpClient.class, Response.class, Call.class, JSONUtils.class, JobLogger.class, ResponseBody.class, SearchUtil.class, UserUtil.class})
    public void testSSOAccountUpdateFailureDueToOrgSearchErrorResponse() throws Exception {
        setValidConfigurations();
        PowerMockito.when(response.body()).thenReturn(responseBody);
        PowerMockito.when(response.code()).thenReturn(200);
        PowerMockito.when(responseBody.string()).thenReturn(getJSONFileAsString("org-search-error-response"));

        SSOAccountUpdaterService service = new SSOAccountUpdaterService();
        service.initialize(config);
        try {
            service.processMessage(getJSONFileAsMap("message-with-all-fields"));
        } catch (ProjectCommonException e) {
            assertEquals(e.getCode(), ResponseCode.invalidOrgData.getErrorCode());
            throw e;
        }
    }

    @Test(expected = ProjectCommonException.class)
    @PrepareForTest({OkHttpClient.class, Response.class, Call.class, JSONUtils.class, JobLogger.class, ResponseBody.class, SearchUtil.class, UserUtil.class})
    public void testMandatoryValidationFailure() throws Exception {
        setValidConfigurations();
        SSOAccountUpdaterService service = new SSOAccountUpdaterService();
        service.initialize(config);
        try {
            service.processMessage(getJSONFileAsMap("message-without-userid"));
        } catch (ProjectCommonException e) {
            assertEquals(e.getCode(), ResponseCode.mandatoryParamsMissing.getErrorCode());
            throw e;
        }
    }

    private Map getJSONFileAsMap(String fileName) {
        Map map = null;
        try {
            map = TestUtil.getJSONFileAsMap(RESOURCE_PATH + fileName + JSON_EXTN);
        } catch (IOException e) {
            fail();
        }
        return map;
    }

    private String getJSONFileAsString(String fileName) {
        return TestUtil.getJSONFileAsString(RESOURCE_PATH + fileName + JSON_EXTN);
    }

    private void setValidConfigurations() {
        PowerMockito.when(config.get(SSOAccountUpdaterParams.lms_host.name())).thenReturn("http://localhost:9000");
        PowerMockito.when(config.get(SSOAccountUpdaterParams.user_update_private_api.name())).thenReturn("/private/user/v1/update");
        PowerMockito.when(config.get(SSOAccountUpdaterParams.org_search_api.name())).thenReturn("/v1/org/search");
    }

}
