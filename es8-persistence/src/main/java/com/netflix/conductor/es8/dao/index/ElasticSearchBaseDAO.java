/*
 * Copyright 2025 Conductor Authors.
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
package com.netflix.conductor.es8.dao.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.es8.dao.query.parser.internal.ParserException;

import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

abstract class ElasticSearchBaseDAO implements IndexDAO {
    String indexPrefix;
    ObjectMapper objectMapper;

    protected String getIndexName(String suffix) {
        return indexPrefix + "_" + suffix;
    }

    // ES8: Document types are removed, so index templates should be used for mapping
    // Update mapping/template logic for ES8
    String loadIndexTemplateSource(String path) throws IOException {
        return applyIndexPrefixToTemplate(
                IOUtils.toString(ElasticSearchBaseDAO.class.getResourceAsStream(path)));
    }

    private String applyIndexPrefixToTemplate(String text) throws JsonProcessingException {
        String indexPatternsFieldName = "index_patterns";
        JsonNode root = objectMapper.readTree(text);
        if (root != null) {
            JsonNode indexPatternsNodeValue = root.get(indexPatternsFieldName);
            if (indexPatternsNodeValue != null && indexPatternsNodeValue.isArray()) {
                ArrayList<String> patternsWithPrefix = new ArrayList<>();
                indexPatternsNodeValue.forEach(
                        v -> {
                            String patternText = v.asText();
                            StringBuilder sb = new StringBuilder();
                            if (patternText.startsWith("*")) {
                                sb.append("*")
                                        .append(indexPrefix)
                                        .append("_")
                                        .append(patternText.substring(1));
                            } else {
                                sb.append(indexPrefix).append("_").append(patternText);
                            }
                            patternsWithPrefix.add(sb.toString());
                        });
                ((ObjectNode) root)
                        .set(indexPatternsFieldName, objectMapper.valueToTree(patternsWithPrefix));
                System.out.println(
                        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
                return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            }
        }
        return text;
    }

    // Query building for ES8
    Query buildQuery(String expression, String queryString) throws ParserException {
        Query query = Query.of(q -> q.matchAll(m -> m));
        if (StringUtils.isNotEmpty(expression)) {
            com.netflix.conductor.es8.dao.query.parser.Expression exp =
                    com.netflix.conductor.es8.dao.query.parser.Expression.fromString(expression);
            query = exp.getFilterBuilder();
        }
        if (StringUtils.isNotEmpty(queryString) && !queryString.equals("*")) {
            Query stringQuery = Query.of(q -> q.queryString(qs -> qs.query(queryString)));
            query = Query.of(q -> q.bool(b -> b.must(stringQuery)));
        }
        return query;
    }

    SearchRequest buildSearchRequest(
            String indexName, Query esQuery, int start, int count, List<String> sortOptions) {
        SearchRequest.Builder srBuilder = new SearchRequest.Builder();
        srBuilder.index(indexName).query(esQuery).from(start).size(count);
        if (sortOptions != null && !sortOptions.isEmpty()) {
            List<SortOptions> sortOptionsList = new ArrayList<>();
            for (String sortOption : sortOptions) {
                SortOrder order = SortOrder.Asc;
                String field = sortOption;
                int index = sortOption.indexOf(":");
                if (index > 0) {
                    field = sortOption.substring(0, index);
                    order =
                            SortOrder._DESERIALIZER.parse(
                                    sortOption.substring(index + 1).toLowerCase());
                    // order = SortOrder.valueOf(sortOption.substring(index + 1).toLowerCase());
                }
                final String finalField = field;
                final SortOrder finalOrder = order;
                sortOptionsList.add(
                        SortOptions.of(s -> s.field(f -> f.field(finalField).order(finalOrder))));
            }
            srBuilder.sort(sortOptionsList);
        }
        return srBuilder.build();
    }
}
