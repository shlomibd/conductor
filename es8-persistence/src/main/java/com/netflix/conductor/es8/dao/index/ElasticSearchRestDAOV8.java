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
package com.netflix.conductor.es8.dao.index;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.es8.config.ElasticSearchProperties;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ElasticSearchRestDAOV8 extends ElasticSearchBaseDAO implements IndexDAO {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestDAOV8.class);
    private final ElasticsearchClient esClient;
    private final ElasticsearchAsyncClient esAsyncClient;
    private final ObjectMapper objectMapper;
    private final ElasticSearchProperties properties;

    public ElasticSearchRestDAOV8(
            ElasticsearchClient esClient,
            ElasticsearchAsyncClient esAsyncClient,
            ObjectMapper objectMapper,
            ElasticSearchProperties properties) {
        this.esClient = esClient;
        this.esAsyncClient = esAsyncClient;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.indexPrefix = properties.getIndexPrefix();
    }

    // Index a document
    public <T> IndexResponse indexDocument(String indexSuffix, String id, T document)
            throws IOException {
        String indexName = getIndexName(indexSuffix);
        IndexRequest<T> request =
                IndexRequest.of(i -> i.index(indexName).id(id).document(document));
        return esClient.index(request);
    }

    // Get a document by ID
    public <T> GetResponse<T> getDocument(String indexSuffix, String id, Class<T> clazz)
            throws IOException {
        String indexName = getIndexName(indexSuffix);
        GetRequest request = GetRequest.of(g -> g.index(indexName).id(id));
        return esClient.get(request, clazz);
    }

    // Update a document
    public <T> UpdateResponse<T> updateDocument(
            String indexSuffix, String id, T document, Class<T> clazz) throws IOException {
        String indexName = getIndexName(indexSuffix);
        UpdateRequest<T, T> request =
                UpdateRequest.of(u -> u.index(indexName).id(id).doc(document));
        return esClient.update(request, clazz);
    }

    // Delete a document
    public DeleteResponse deleteDocument(String indexSuffix, String id) throws IOException {
        String indexName = getIndexName(indexSuffix);
        DeleteRequest request = DeleteRequest.of(d -> d.index(indexName).id(id));
        return esClient.delete(request);
    }

    // Search documents (sync)
    public <T> SearchResponse<T> searchDocuments(
            String indexSuffix, Query query, Class<T> clazz, int size) throws IOException {
        String indexName = getIndexName(indexSuffix);
        SearchRequest request = SearchRequest.of(s -> s.index(indexName).query(query).size(size));
        return esClient.search(request, clazz);
    }

    // Search documents (async)
    public <T> CompletableFuture<SearchResponse<T>> asyncSearchDocuments(
            String indexSuffix, Query query, Class<T> clazz, int size) {
        String indexName = getIndexName(indexSuffix);
        SearchRequest request = SearchRequest.of(s -> s.index(indexName).query(query).size(size));
        return esAsyncClient.search(request, clazz);
    }

    // Bulk operations (sync)
    public BulkResponse bulkOperations(BulkRequest request) throws IOException {
        return esClient.bulk(request);
    }

    // Bulk operations (async)
    public CompletableFuture<BulkResponse> asyncBulkOperations(BulkRequest request) {
        return esAsyncClient.bulk(request);
    }

    // Count documents (sync)
    public CountResponse countDocuments(String indexSuffix, Query query) throws IOException {
        String indexName = getIndexName(indexSuffix);
        CountRequest request = CountRequest.of(c -> c.index(indexName).query(query));
        return esClient.count(request);
    }

    // Count documents (async)
    public CompletableFuture<CountResponse> asyncCountDocuments(String indexSuffix, Query query) {
        String indexName = getIndexName(indexSuffix);
        CountRequest request = CountRequest.of(c -> c.index(indexName).query(query));
        return esAsyncClient.count(request);
    }

    // Index management
    public CreateIndexResponse createIndex(String indexSuffix) throws IOException {
        String indexName = getIndexName(indexSuffix);
        CreateIndexRequest request = CreateIndexRequest.of(c -> c.index(indexName));
        return esClient.indices().create(request);
    }

    public DeleteIndexResponse deleteIndex(String indexSuffix) throws IOException {
        String indexName = getIndexName(indexSuffix);
        DeleteIndexRequest request = DeleteIndexRequest.of(d -> d.index(indexName));
        return esClient.indices().delete(request);
    }

    public GetIndexResponse getIndex(String indexSuffix) throws IOException {
        String indexName = getIndexName(indexSuffix);
        GetIndexRequest request = GetIndexRequest.of(g -> g.index(indexName));
        return esClient.indices().get(request);
    }

    // Health check (returns true if index exists)
    public boolean isIndexHealthy(String indexSuffix) {
        try {
            GetIndexResponse response = getIndex(indexSuffix);
            return response.result().containsKey(getIndexName(indexSuffix));
        } catch (Exception e) {
            logger.warn("Index health check failed for {}: {}", indexSuffix, e.getMessage());
            return false;
        }
    }

    @Override
    public CompletableFuture<Void> asyncIndexWorkflow(WorkflowSummary workflow) {
        String indexName = getIndexName("workflow");
        IndexRequest<WorkflowSummary> request =
                IndexRequest.of(
                        i -> i.index(indexName).id(workflow.getWorkflowId()).document(workflow));
        return esAsyncClient
                .index(request)
                .thenAccept(response -> {})
                .exceptionally(
                        e -> {
                            logger.error(
                                    "Failed to async index workflow {}",
                                    workflow.getWorkflowId(),
                                    e);
                            return null;
                        });
    }

    @Override
    public CompletableFuture<Void> asyncIndexTask(TaskSummary task) {
        String indexName = getIndexName("task");
        IndexRequest<TaskSummary> request =
                IndexRequest.of(i -> i.index(indexName).id(task.getTaskId()).document(task));
        return esAsyncClient
                .index(request)
                .thenAccept(response -> {})
                .exceptionally(
                        e -> {
                            logger.error("Failed to async index task {}", task.getTaskId(), e);
                            return null;
                        });
    }

    public CompletableFuture<Void> asyncIndexEventExecution(EventExecution eventExecution) {
        String indexName = getIndexName("event");
        IndexRequest<EventExecution> request =
                IndexRequest.of(
                        i ->
                                i.index(indexName)
                                        .id(eventExecution.getId())
                                        .document(eventExecution));
        return esAsyncClient
                .index(request)
                .thenAccept(response -> {})
                .exceptionally(
                        e -> {
                            logger.error(
                                    "Failed to async index event execution {}",
                                    eventExecution.getId(),
                                    e);
                            return null;
                        });
    }

    public CompletableFuture<Void> asyncIndexTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
        if (taskExecLogs == null || taskExecLogs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String indexName = getIndexName("task_log");

        // Build bulk request with all task execution logs
        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();

        for (TaskExecLog log : taskExecLogs) {
            // Add index operation without specifying ID - let Elasticsearch auto-generate it
            bulkBuilder.operations(op -> op.index(idx -> idx.index(indexName).document(log)));
        }

        BulkRequest bulkRequest = bulkBuilder.build();

        return esAsyncClient
                .bulk(bulkRequest)
                .thenAccept(
                        response -> {
                            if (response.errors()) {
                                logger.warn(
                                        "Bulk indexing of task execution logs completed with errors");
                                response.items()
                                        .forEach(
                                                item -> {
                                                    if (item.error() != null) {
                                                        logger.error(
                                                                "Failed to index task log: {}",
                                                                item.error().reason());
                                                    }
                                                });
                            } else {
                                logger.debug(
                                        "Successfully indexed {} task execution logs",
                                        taskExecLogs.size());
                            }
                        })
                .exceptionally(
                        e -> {
                            logger.error("Failed to bulk index task execution logs", e);
                            return null;
                        });
    }

    public CompletableFuture<Void> asyncAddMessageToIndex(Message message) {
        String indexName = getIndexName("message");
        IndexRequest<Message> request =
                IndexRequest.of(i -> i.index(indexName).id(message.getId()).document(message));
        return esAsyncClient
                .index(request)
                .thenAccept(response -> {})
                .exceptionally(
                        e -> {
                            logger.error("Failed to async index message {}", message.getId(), e);
                            return null;
                        });
    }

    @Override
    public void setup() throws Exception {
        // Stub implementation for setup
        logger.info("Setting up ElasticSearch indices for version 8");
    }

    @Override
    public void indexWorkflow(WorkflowSummary workflow) {
        try {
            indexDocument("workflow", workflow.getWorkflowId(), workflow);
        } catch (Exception e) {
            logger.error("Failed to index workflow: {}", workflow.getWorkflowId(), e);
        }
    }

    @Override
    public void indexTask(TaskSummary task) {
        try {
            indexDocument("task", task.getTaskId(), task);
        } catch (Exception e) {
            logger.error("Failed to index task: {}", task.getTaskId(), e);
        }
    }

    @Override
    public void removeWorkflow(String workflowId) {
        try {
            deleteDocument("workflow", workflowId);
        } catch (Exception e) {
            logger.error("Failed to remove workflow: {}", workflowId, e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
        return CompletableFuture.runAsync(() -> removeWorkflow(workflowId));
    }

    @Override
    public void updateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
        // Stub implementation
        logger.debug("Update workflow: {} with keys: {}", workflowInstanceId, keys);
    }

    @Override
    public CompletableFuture<Void> asyncUpdateWorkflow(
            String workflowInstanceId, String[] keys, Object[] values) {
        return CompletableFuture.runAsync(() -> updateWorkflow(workflowInstanceId, keys, values));
    }

    @Override
    public void removeTask(String workflowId, String taskId) {
        try {
            deleteDocument("task", taskId);
        } catch (Exception e) {
            logger.error("Failed to remove task: {}", taskId, e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncRemoveTask(String workflowId, String taskId) {
        return CompletableFuture.runAsync(() -> removeTask(workflowId, taskId));
    }

    @Override
    public void updateTask(String workflowId, String taskId, String[] keys, Object[] values) {
        // Stub implementation
        logger.debug("Update task: {} with keys: {}", taskId, keys);
    }

    @Override
    public CompletableFuture<Void> asyncUpdateTask(
            String workflowId, String taskId, String[] keys, Object[] values) {
        return CompletableFuture.runAsync(() -> updateTask(workflowId, taskId, keys, values));
    }

    @Override
    public String get(String workflowInstanceId, String key) {
        // Stub implementation
        return null;
    }

    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> logs) {
        asyncAddTaskExecutionLogs(logs);
        /*  if (logs == null || logs.isEmpty()) {
            return;
        }
        try {
            for (TaskExecLog log : logs) {
                indexDocument("task_log", log.getTaskId(), log);
            }
        } catch (Exception e) {
            logger.error("Failed to add task execution logs", e);
        }*/
    }

    @Override
    public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
        return asyncIndexTaskExecutionLogs(logs);
    }

    @Override
    public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
        try {
            // Build query to search for logs by taskId using filter with match_phrase
            Query query =
                    Query.of(
                            q ->
                                    q.bool(
                                            b ->
                                                    b.filter(
                                                            f ->
                                                                    f.matchPhrase(
                                                                            mp ->
                                                                                    mp.field(
                                                                                                    "taskId")
                                                                                            .query(
                                                                                                    taskId)))));

            // Create search request with sorting and size limit
            String logIndexPattern = getIndexName("task_log");
            SearchRequest searchRequest =
                    SearchRequest.of(
                            s ->
                                    s.index(logIndexPattern)
                                            .query(query)
                                            .sort(
                                                    sort ->
                                                            sort.field(
                                                                    f ->
                                                                            f.field("createdTime")
                                                                                    .order(
                                                                                            co
                                                                                                    .elastic
                                                                                                    .clients
                                                                                                    .elasticsearch
                                                                                                    ._types
                                                                                                    .SortOrder
                                                                                                    .Asc)))
                                            .size(properties.getTaskLogResultLimit()));

            SearchResponse<TaskExecLog> response =
                    esClient.search(searchRequest, TaskExecLog.class);

            // Map the response hits to TaskExecLog objects
            return response.hits().hits().stream()
                    .map(hit -> hit.source())
                    .filter(log -> log != null)
                    .toList();
        } catch (Exception e) {
            logger.error("Failed to get task execution logs for task: {}", taskId, e);
            return Collections.emptyList();
        }
    }

    @Override
    public void addEventExecution(EventExecution eventExecution) {
        try {
            indexDocument("event", eventExecution.getId(), eventExecution);
        } catch (Exception e) {
            logger.error("Failed to add event execution: {}", eventExecution.getId(), e);
        }
    }

    @Override
    public List<EventExecution> getEventExecutions(String event) {
        // Stub implementation
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
        return asyncIndexEventExecution(eventExecution);
    }

    @Override
    public void addMessage(String queue, Message msg) {
        try {
            indexDocument("message", msg.getId(), msg);
        } catch (Exception e) {
            logger.error("Failed to add message: {}", msg.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncAddMessage(String queue, Message message) {
        return asyncAddMessageToIndex(message);
    }

    @Override
    public List<Message> getMessages(String queue) {
        // Stub implementation
        return Collections.emptyList();
    }

    @Override
    public List<String> searchArchivableWorkflows(String indexName, long archiveTtlDays) {
        try {
            LocalDate cutoffDate = LocalDate.now().minusDays(archiveTtlDays);
            LocalDate previousDay = cutoffDate.minusDays(1);

            Query query =
                    Query.of(
                            q ->
                                    q.bool(
                                            b ->
                                                    b.must(
                                                                    m ->
                                                                            m.range(
                                                                                    r ->
                                                                                            r.date(
                                                                                                    d ->
                                                                                                            d.field(
                                                                                                                            "endTime")
                                                                                                                    .lt(
                                                                                                                            cutoffDate
                                                                                                                                    .toString())
                                                                                                                    .gte(
                                                                                                                            previousDay
                                                                                                                                    .toString()))))
                                                            .should(
                                                                    s ->
                                                                            s.term(
                                                                                    t ->
                                                                                            t.field(
                                                                                                            "status")
                                                                                                    .value(
                                                                                                            "COMPLETED")))
                                                            .should(
                                                                    s ->
                                                                            s.term(
                                                                                    t ->
                                                                                            t.field(
                                                                                                            "status")
                                                                                                    .value(
                                                                                                            "FAILED")))
                                                            .should(
                                                                    s ->
                                                                            s.term(
                                                                                    t ->
                                                                                            t.field(
                                                                                                            "status")
                                                                                                    .value(
                                                                                                            "TIMED_OUT")))
                                                            .should(
                                                                    s ->
                                                                            s.term(
                                                                                    t ->
                                                                                            t.field(
                                                                                                            "status")
                                                                                                    .value(
                                                                                                            "TERMINATED")))
                                                            .mustNot(
                                                                    mn ->
                                                                            mn.exists(
                                                                                    e ->
                                                                                            e.field(
                                                                                                    "archived")))
                                                            .minimumShouldMatch("1")));

            SearchRequest request =
                    SearchRequest.of(s -> s.index(indexName).query(query).size(1000));

            SearchResponse<WorkflowSummary> response =
                    esClient.search(request, WorkflowSummary.class);
            return response.hits().hits().stream().map(hit -> hit.id()).toList();
        } catch (Exception e) {
            logger.error("Unable to search archivable workflows", e);
            return Collections.emptyList();
        }
    }

    @Override
    public long getWorkflowCount(String query, String freeText) {
        try {
            Query esQuery = buildQuery(query, freeText);
            CountRequest request =
                    CountRequest.of(c -> c.index(getIndexName("workflow")).query(esQuery));
            CountResponse response = esClient.count(request);
            return response.count();
        } catch (Exception e) {
            logger.error("Failed to get workflow count", e);
            return 0L;
        }
    }

    @Override
    public SearchResult<String> searchWorkflows(
            String query, String freeText, int start, int count, List<String> sort) {
        try {
            Query esQuery = buildQuery(query, freeText);
            SearchRequest request =
                    buildSearchRequest(getIndexName("workflow"), esQuery, start, count, sort);
            SearchResponse<WorkflowSummary> response =
                    esClient.search(request, WorkflowSummary.class);
            List<String> ids = response.hits().hits().stream().map(hit -> hit.id()).toList();
            return new SearchResult<String>(response.hits().total().value(), ids);
        } catch (Exception e) {
            logger.error("Failed to search workflows", e);
            return new SearchResult<String>(0L, Collections.emptyList());
        }
    }

    @Override
    public SearchResult<WorkflowSummary> searchWorkflowSummary(
            String query, String freeText, int start, int count, List<String> sort) {
        try {
            Query esQuery = buildQuery(query, freeText);
            SearchRequest request =
                    buildSearchRequest(getIndexName("workflow"), esQuery, start, count, sort);
            SearchResponse<WorkflowSummary> response =
                    esClient.search(request, WorkflowSummary.class);
            List<WorkflowSummary> summaries =
                    response.hits().hits().stream().map(hit -> hit.source()).toList();
            return new SearchResult<WorkflowSummary>(response.hits().total().value(), summaries);
        } catch (Exception e) {
            logger.error("Failed to search workflow summaries", e);
            return new SearchResult<WorkflowSummary>(0L, Collections.emptyList());
        }
    }

    @Override
    public SearchResult<String> searchTasks(
            String query, String freeText, int start, int count, List<String> sort) {
        try {
            Query esQuery = buildQuery(query, freeText);
            SearchRequest request =
                    buildSearchRequest(getIndexName("task"), esQuery, start, count, sort);
            SearchResponse<TaskSummary> response = esClient.search(request, TaskSummary.class);
            List<String> ids = response.hits().hits().stream().map(hit -> hit.id()).toList();
            return new SearchResult<String>(response.hits().total().value(), ids);
        } catch (Exception e) {
            logger.error("Failed to search tasks", e);
            return new SearchResult<String>(0L, Collections.emptyList());
        }
    }

    @Override
    public SearchResult<TaskSummary> searchTaskSummary(
            String query, String freeText, int start, int count, List<String> sort) {
        try {
            Query esQuery = buildQuery(query, freeText);
            SearchRequest request =
                    buildSearchRequest(getIndexName("task"), esQuery, start, count, sort);
            SearchResponse<TaskSummary> response = esClient.search(request, TaskSummary.class);
            List<TaskSummary> summaries =
                    response.hits().hits().stream().map(hit -> hit.source()).toList();
            return new SearchResult<TaskSummary>(response.hits().total().value(), summaries);
        } catch (Exception e) {
            logger.error("Failed to search task summaries", e);
            return new SearchResult<TaskSummary>(0L, Collections.emptyList());
        }
    }

    @Override
    Query buildQuery(String query, String freeText) {
        try {
            return super.buildQuery(query, freeText);
        } catch (Exception e) {
            logger.error("Failed to build query", e);
            return Query.of(q -> q.matchAll(m -> m));
        }
    }
}
