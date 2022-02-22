package fedprototype.tools;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Http {
    private static ObjectMapper mapper = new ObjectMapper();

    private static PostMethod pure_post(String url, Map<String, ?> params) throws IOException {
        String paramsJson = mapper.writeValueAsString(params);
        HttpClient client = new HttpClient();
        PostMethod postMethod = new PostMethod(url);
        postMethod.addRequestHeader("Content-type", "application/json; charset=utf-8");
        byte[] requestBytes = paramsJson.getBytes("utf-8");
        InputStream inputStream = new ByteArrayInputStream(requestBytes, 0, requestBytes.length);
        RequestEntity requestEntity = new InputStreamRequestEntity(inputStream);
        postMethod.setRequestEntity(requestEntity);
        client.executeMethod(postMethod);
        return postMethod;
    }

    public static JsonNode post_pro(String url, Map<String, ?> params) throws IOException {
        return post_pro(url, params, true, "raise");
    }

    public static JsonNode post_pro(
            String url,
            Map<String, ?> params,
            boolean checkStatusCode,
            String error) throws IOException {
        try {
            PostMethod res = pure_post(url, params);
            byte[] responseBody = res.getResponseBody();
            String responseText = new String(responseBody);
            if (res.getStatusCode() == 200) {
                return mapper.readTree(responseText);
            } else if (checkStatusCode) {
                throw new IOException("post status code:" + res.getStatusCode() + " != 200, res text: " + responseText);
            } else {
                return null;
            }
        } catch (IOException e) {
            if ("None".equals(error)) {
                return null;
            } else {
                throw e;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        HashMap<String, String> p = new HashMap<String, String>();
        p.put("job_id", "abc");
        p.put("root_role_name", "def");
        JsonNode jn = post_pro("http://127.0.0.1:6609/test", p);
        System.out.println(jn);
    }
}
