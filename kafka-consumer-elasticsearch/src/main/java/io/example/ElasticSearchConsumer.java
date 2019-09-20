package io.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumer {
    private Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) {
        new ElasticSearchConsumer().run();
    }

    private void run() {
        RestHighLevelClient client = createClient();

        String doc = "{ \"foo\": \"bar\" }";

        IndexRequest indexRequest = new IndexRequest("twitter");
        indexRequest.source(doc, XContentType.JSON);

        try {
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            String id = indexResponse.getId();
            logger.info("Document index id {}", id);
        } catch (IOException ex) {
            logger.error("Document index failed", ex);
        }
    }

    private RestHighLevelClient createClient() {
        String host = System.getenv("EL_HOST");
        String user = System.getenv("EL_USER");
        String pass = System.getenv("EL_PASS");

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, pass));

        RestClientBuilder restClientBuilder = RestClient
                .builder(new HttpHost(host, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(restClientBuilder);
    }
}
