package org.sunbird.jobs.samza.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.util.Map;

public class UserCertUtil {

  private static MediaType jsonMedia = MediaType.parse(javax.ws.rs.core.MediaType.APPLICATION_JSON);
  private static ObjectMapper mapper = new ObjectMapper();
  private static OkHttpClient httpClient = new OkHttpClient();

  public static boolean mergeUserCert(Map<String, Object> mergeCertRequest, String url) throws IOException {
    RequestBody body = RequestBody.create(jsonMedia,mapper.writeValueAsString(mergeCertRequest));
    Request request = new Request.Builder()
            .url(url)
            .patch(body)
            .addHeader(HttpHeaders.ACCEPT, javax.ws.rs.core.MediaType.APPLICATION_JSON)
            .addHeader(HttpHeaders.CONTENT_TYPE, javax.ws.rs.core.MediaType.APPLICATION_JSON)
            .build();
    Response response = httpClient.newCall(request).execute();
    int responsecode = response.code();
    response.close();

    if(responsecode == 200)
      return true;
    else
      return false;
  }
}
