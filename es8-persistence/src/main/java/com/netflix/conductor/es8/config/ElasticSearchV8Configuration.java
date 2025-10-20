/*
 * Copyright 2023 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.es8.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.es8.dao.index.ElasticSearchRestDAOV8;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ElasticSearchProperties.class)
@Conditional(ElasticSearchConditions.ElasticSearchV8Enabled.class)
public class ElasticSearchV8Configuration {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchV8Configuration.class);

    @Bean
    public RestClient restClient(ElasticSearchProperties properties) {
        HttpHost[] httpHosts = convertToHttpHosts(properties.toURLs());
        log.debug("httpHosts: " + Arrays.toString(httpHosts));
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(
                        properties.getUsername(), properties.getPassword()));

        CertificateFactory factory;
        Certificate trustedCa;
        KeyStore trustStore;
        SSLContextBuilder sslContextBuilder;
        SSLContext sslContext;
        try {
            factory = CertificateFactory.getInstance("X.509");
            String elasticCertificateFile = properties.getCertificateFile();
            try (InputStream is = new FileInputStream(elasticCertificateFile)) {
                trustedCa = factory.generateCertificate(is);
            }
            trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
            sslContext = sslContextBuilder.build();
        } catch (CertificateException
                | IOException
                | KeyStoreException
                | NoSuchAlgorithmException
                | KeyManagementException e) {
            throw new RuntimeException("Failed to initialize SSL context for Elasticsearch", e);
        }

        // Create the low-level client
        SSLContext finalSslContext = sslContext;
        RestClient restClient =
                RestClient.builder(httpHosts)
                        .setHttpClientConfigCallback(
                                httpClientBuilder -> {
                                    return httpClientBuilder
                                            .setSSLContext(finalSslContext)
                                            .setDefaultCredentialsProvider(credentialsProvider);
                                })
                        .build();

        return restClient;
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(RestClient restClient) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        RestClientTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper(objectMapper));
        return new ElasticsearchClient(transport);
    }

    @Bean
    public ElasticsearchAsyncClient elasticsearchAsyncClient(RestClient restClient) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        RestClientTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper(objectMapper));
        return new ElasticsearchAsyncClient(transport);
    }

    @Primary
    @Bean
    public IndexDAO es8IndexDAO(
            ElasticsearchClient elasticsearchClient,
            ElasticsearchAsyncClient elasticsearchAsyncClient,
            ElasticSearchProperties properties,
            ObjectMapper objectMapper) {
        return new ElasticSearchRestDAOV8(
                elasticsearchClient, elasticsearchAsyncClient, objectMapper, properties);
    }

    private HttpHost[] convertToHttpHosts(List<URL> hosts) {
        return hosts.stream()
                .map(host -> new HttpHost(host.getHost(), host.getPort(), host.getProtocol()))
                .toArray(HttpHost[]::new);
    }
}
