package org.sunbird.jobs.samza.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.Request;
import okhttp3.Response;

import javax.ws.rs.core.HttpHeaders;
import java.util.Map;

public class UserUtil {

    private static MediaType jsonMediaType = MediaType.parse(javax.ws.rs.core.MediaType.APPLICATION_JSON);
    private static OkHttpClient client = new OkHttpClient();
    private static ObjectMapper mapper = new ObjectMapper();

    public static boolean updateUser(Map<String, Object> updateUserRequest, String url) throws Exception {

        RequestBody body = RequestBody.create(jsonMediaType, mapper.writeValueAsString(updateUserRequest));
        Request request = new Request.Builder()
                .url(url)
                .patch(body)
                .addHeader(HttpHeaders.ACCEPT, javax.ws.rs.core.MediaType.APPLICATION_JSON)
                .addHeader(HttpHeaders.CONTENT_TYPE, javax.ws.rs.core.MediaType.APPLICATION_JSON)
                .build();

        Response response = client.newCall(request).execute();

        int responseCode = response.code();
        response.close();

        if (200 == responseCode) {
            return true;
        } else {
            return false;
        }
    }
}
