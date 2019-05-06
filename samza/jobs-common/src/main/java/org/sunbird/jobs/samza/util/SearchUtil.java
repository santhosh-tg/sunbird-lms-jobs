package org.sunbird.jobs.samza.util;

import javax.ws.rs.core.HttpHeaders;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.Request;
import okhttp3.Response;

public class SearchUtil {

    private static MediaType jsonMediaType = MediaType.parse(javax.ws.rs.core.MediaType.APPLICATION_JSON);
    private static OkHttpClient client = new OkHttpClient();
    private static ObjectMapper mapper = new ObjectMapper();

    public static Map<String, Object> search(Map<String, Object> searchRequest, String url) throws Exception {

        Map<String, Object> responseMap = null;
        RequestBody body = RequestBody.create(jsonMediaType, mapper.writeValueAsString(searchRequest));
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader(HttpHeaders.ACCEPT, javax.ws.rs.core.MediaType.APPLICATION_JSON)
                .addHeader(HttpHeaders.CONTENT_TYPE, javax.ws.rs.core.MediaType.APPLICATION_JSON)
                .build();

        Response response = client.newCall(request).execute();

        int responseCode = response.code();
        String responseJson = response.body().string();

        if (200 == responseCode) {
            Map<String, Object> searchResponse = mapper.readValue(responseJson, Map.class);
            Map<String, Object> resultMap = (Map<String, Object>) searchResponse.get(CommonParams.result.name());
            responseMap = (Map<String, Object>) resultMap.get(CommonParams.response.name());
        }
        return responseMap;
    }
}
