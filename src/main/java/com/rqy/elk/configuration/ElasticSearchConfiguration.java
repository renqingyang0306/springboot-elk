package com.rqy.elk.configuration;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * @author renqingyang
 * @since v1.0.0
 * ES配置
 *  -- 注意，也不要使用spring-data-elasticsearch
 */
@Configuration
public class ElasticSearchConfiguration {

    @Value("${es.url}")
    private String url;

    @Value("${es.user}")
    private String user;

    @Value("${es.password}")
    private String password;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        RestClientBuilder builder = RestClient.builder(HttpHost.create(url))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder.setMaxConnTotal(20);
                    httpAsyncClientBuilder.setMaxConnPerRoute(10);
                    if (!StringUtils.isEmpty(user) && !StringUtils.isEmpty(password)) {
                        CredentialsProvider provider = new BasicCredentialsProvider();
                        provider.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials(user, password));
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(provider);
                    }
                    return httpAsyncClientBuilder;
                });
        return new RestHighLevelClient(builder);
    }
}
