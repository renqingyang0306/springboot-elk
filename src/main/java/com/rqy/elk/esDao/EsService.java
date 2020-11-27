package com.rqy.elk.esDao;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.alibaba.fastjson.TypeReference;
import com.rqy.elk.core.Constant;
import com.rqy.elk.core.RestResponse;
import net.logstash.logback.encoder.org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 封装es操作
 */
@Service
public class EsService {

    private static final String INDEX_KEY = "index";

    private static final String TYPE_KEY = "type";

    private static final String INDEX = "logstash";

    private static final String TYPE = "doc";

    private static final String TIMESTAMP = "createTime";


    private static final Logger logger = LoggerFactory.getLogger(EsService.class);

    private final RestHighLevelClient restHighLevelClient;

    public EsService(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }


    /**
     * 组装es request
     * @param indexList 索引列表
     * @param searchSourceBuilder 查询条件
     * @return
     */
    public SearchRequest getStatisticRequest (List<String> indexList, SearchSourceBuilder searchSourceBuilder) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.scroll(TimeValue.timeValueSeconds(Constant.ES_BATCH_SCROLL_ID_LIFE));
        searchRequest.indices(indexList.toArray(new String[0])).source(searchSourceBuilder);
        return searchRequest;
    }


    /**
     * 创建索引(默认分片数为5和副本数为1)
     * @param indexName
     * @throws IOException
     */
    public void createIndex(String indexName) throws IOException {
        if (checkIndexExists(indexName)) {
            logger.error("index={}索引已经存在！", indexName);
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.mapping(TYPE, generateBuilder());
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        // 指示是否所有节点都已确认请求
        boolean acknowledged = response.isAcknowledged();
        // 指示是否在超时之前为索引中的每个分片启动了必需的分片副本数
        boolean shardsAcknowledged = response.isShardsAcknowledged();
        if (acknowledged || shardsAcknowledged) {
            logger.info("创建索引成功！索引名称为{}", indexName);
        }
    }

    /**
     * 判断索引是否存在
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean checkIndexExists(String indexName) {
        GetIndexRequest request = new GetIndexRequest().indices(indexName);
        try {
            return restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("判断索引是否存在，操作异常！");
        }
        return false;
    }

    /**
     * 创建索引(指定 index 和 type)
     * @param index
     * @param type
     * @throws IOException
     */
    public void createIndex(String index, String type) throws IOException {
        if (checkIndexExists(index)) {
            logger.error("index={}索引已存在！", index);
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(index);
        request.mapping(type, generateBuilder());
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        boolean shardsAcknowledged = response.isShardsAcknowledged();
        if (acknowledged || shardsAcknowledged) {
            logger.info("索引创建成功！索引名称为{}", index);
        }
    }

    /**
     * 创建索引(传入参数：分片数、副本数)
     * @param indexName
     * @param shards
     * @param replicas
     * @throws IOException
     */
    public void createIndex(String indexName, int shards, int replicas) throws IOException {
        if (checkIndexExists(indexName)) {
            logger.error("index={}索引已存在！", indexName);
            return;
        }
        Settings.Builder builder = Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", replicas);
        CreateIndexRequest request = new CreateIndexRequest(indexName).settings(builder);
        request.mapping(TYPE, generateBuilder());
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        if (response.isAcknowledged() || response.isShardsAcknowledged()) {
            logger.info("创建索引成功！索引名称为{}", indexName);
        }
    }

    /**
     * 删除索引
     * @param indexName
     * @throws IOException
     */
    public void deleteIndex(String indexName) throws IOException {
        try {
            AcknowledgedResponse response =  restHighLevelClient.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                logger.info("{} 索引删除成功！", indexName);
            }
        } catch (ElasticsearchException ex) {
            if (ex.status() == RestStatus.NOT_FOUND) {
                logger.error("{} 索引名不存在", indexName);
            }
            logger.error("删除失败！");
        }
    }

    /**
     * 开启索引
     * @param indexName
     * @throws IOException
     */
    public void openIndex(String indexName) throws IOException{
        if (!checkIndexExists(indexName)) {
            logger.error("索引不存在！");
            return;
        }
        OpenIndexRequest request = new OpenIndexRequest(indexName);
        AcknowledgedResponse response = restHighLevelClient.indices().open(request, RequestOptions.DEFAULT);
        if (response.isAcknowledged()) {
            logger.info("{} 索引开启成功！", indexName);
        }
    }

    /**
     * 关闭索引
     * @param indexName
     * @throws IOException
     */
    public void closeIndex(String indexName) throws IOException {
        if (!checkIndexExists(indexName)) {
            logger.error("索引不存在！");
            return;
        }
        CloseIndexRequest request = new CloseIndexRequest(indexName);
        AcknowledgedResponse response = restHighLevelClient.indices().close(request, RequestOptions.DEFAULT);
        if (response.isAcknowledged()) {
            logger.info("{} 索引已关闭！", indexName);
        }
    }

    /**
     * 设置文档的静态映射(主要是为 message字段 设置ik分词器)
     * @param index
     * @param type
     * @throws IOException
     */
    public void setFieldsMapping(String index, String type) {
        PutMappingRequest request = new PutMappingRequest(index).type(type);
        try {
            request.source(generateBuilder());
            AcknowledgedResponse response = restHighLevelClient.indices().putMapping(request, RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                logger.info("已成功对\"index={}, type={}\"的文档设置类型映射！", index, type);
            }
        } catch (IOException e) {
            logger.error("\"index={}, type={}\"的文档设置类型映射失败，请检查参数！", index, type);
        }
    }

    /**
     * 增加文档
     * @param indexName
     * @param typeName
     * @param id
     * @param jsonString
     */
    public void addDocByJson(String indexName, String typeName, String id, String jsonString) throws IOException{
        if (!checkIndexExists(indexName)) {
            createIndex(indexName, typeName);
        }
        IndexRequest request = new IndexRequest(indexName, typeName, id).source(jsonString, XContentType.JSON);
        // request的opType默认是INDEX(传入相同id会覆盖原document，CREATE则会将旧的删除)
        // request.opType(DocWriteRequest.OpType.CREATE)
        IndexResponse response = null;
        try {
            response = restHighLevelClient.index(request, RequestOptions.DEFAULT);

            String index = response.getIndex();
            String type = response.getType();
            String documentId = response.getId();
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                logger.info("新增文档成功！ index: {}, type: {}, id: {}", index , type, documentId);
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                logger.info("修改文档成功！ index: {}, type: {}, id: {}", index , type, documentId);
            }
            // 分片处理信息
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                logger.error("文档未写入全部分片副本！");
            }
            // 如果有分片副本失败，可以获得失败原因信息
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    logger.error("副本失败原因：{}", reason);
                }
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                logger.error("版本异常！");
            }
            logger.error("文档新增失败！");
        }
    }
    /**
     * 查找文档
     * @param index
     * @param type
     * @param id
     * @return
     * @throws IOException
     */
    public Map<String, Object> getDocument(String index, String type, String id) throws IOException{
        Map<String, Object> resultMap = new HashMap<>();
        GetRequest request = new GetRequest(index, type, id);
        // 实时(否)
        request.realtime(false);
        // 检索之前执行刷新(是)
        request.refresh(true);

        GetResponse response = null;
        try {
            response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                logger.error("文档未找到，请检查参数！" );
            }
            if (e.status() == RestStatus.CONFLICT) {
                logger.error("版本冲突！" );
            }
            logger.error("查找失败！");
        }

        if(Objects.nonNull(response)) {
            if (response.isExists()) { // 文档存在
                resultMap = response.getSourceAsMap();
            } else {
                // 处理未找到文档的方案。 请注意，虽然返回的响应具有404状态代码，但仍返回有效的GetResponse而不是抛出异常。
                // 此时此类响应不持有任何源文档，并且其isExists方法返回false。
                logger.error("文档未找到，请检查参数！" );
            }
        }
        return resultMap;
    }

    /**
     * 删除文档
     * @param index
     * @param type
     * @param id
     * @throws IOException
     */
    public void deleteDocument(String index, String type, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index, type, id);
        DeleteResponse response = null;
        try {
            response = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                logger.error("版本冲突！" );
            }
            logger.error("删除失败!");
        }
        if (Objects.nonNull(response)) {
            if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                logger.error("不存在该文档，请检查参数！");
            }
            logger.info("文档已删除！");
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                logger.error("部分分片副本未处理");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    logger.error("失败原因：{}", reason);
                }
            }
        }
    }

    /**
     * 通过一个JSON字符串更新文档(如果该文档不存在，则根据参数创建这个文档)
     * @param index
     * @param type
     * @param id
     * @param jsonString
     * @throws IOException
     */
    public void updateDocByJson(String index, String type, String id, String jsonString) throws IOException {

        if (!checkIndexExists(index)) {
            createIndex(index, type);
        }
        UpdateRequest request = new UpdateRequest(index, type, id);
        request.doc(jsonString, XContentType.JSON);
        // 如果要更新的文档不存在，则根据传入的参数新建一个文档
        request.docAsUpsert(true);
        try {
            UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
            String indexName = response.getIndex();
            String typeName = response.getType();
            String documentId = response.getId();
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                logger.info("文档新增成功！index: {}, type: {}, id: {}", indexName, typeName, documentId);
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                logger.info("文档更新成功！");
            } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
                logger.error("index={},type={},id={}的文档已被删除，无法更新！", indexName, typeName, documentId);
            } else if (response.getResult() == DocWriteResponse.Result.NOOP) {
                logger.error("操作没有被执行！");
            }

            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                logger.error("分片副本未全部处理");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    logger.error("未处理原因：{}", reason);
                }
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                logger.error("不存在这个文档，请检查参数！" );
            } else if (e.status() == RestStatus.CONFLICT) {
                logger.error("版本冲突异常！" );
            }
            logger.error("更新失败！");
        }
    }
    /**
     * 批量增加文档
     * @param params
     * @throws IOException
     */
    public void bulkAdd(List<Map<String, String>> params) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String type = dataMap.getOrDefault(TYPE_KEY, TYPE);
            String id = dataMap.get("id");
            String jsonString = dataMap.get("json");
            if (StringUtils.isNotBlank(id)) {
                IndexRequest request = new IndexRequest(index, type, id).source(jsonString, XContentType.JSON);
                bulkRequest.add(request);
            }
        }
        // 超时时间(2分钟)
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));
        // 刷新策略
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        if (bulkRequest.numberOfActions() == 0) {
            logger.error("参数错误，批量增加操作失败！");
            return;
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        // 全部操作成功
        if (!bulkResponse.hasFailures()) {
            logger.info("批量增加操作成功！");
        } else {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    logger.error("index={}, type={}, id={}的文档增加失败！", failure.getIndex(), failure.getType(), failure.getId());
                    logger.error("增加失败详情: {}", failure.getMessage());
                } else {
                    logger.info("index={}, type={}, id={}的文档增加成功！", bulkItemResponse.getIndex(), bulkItemResponse.getType(), bulkItemResponse.getId());
                }
            }
        }
    }

    /**
     * 批量更新文档
     * @param params
     * @throws IOException
     */
    public void bulkUpdate(List<Map<String, String>> params) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String type = dataMap.getOrDefault(TYPE_KEY, TYPE);
            String id = dataMap.get("id");
            String jsonString = dataMap.get("json");
            if (StringUtils.isNotBlank(id)) {
                UpdateRequest request = new UpdateRequest(index, type, id).doc(jsonString, XContentType.JSON);
                request.docAsUpsert(true);
                bulkRequest.add(request);
            }
        }
        if (bulkRequest.numberOfActions() == 0) {
            logger.error("参数错误，批量更新操作失败！");
            return;
        }
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (!bulkResponse.hasFailures()) {
            logger.info("批量更新操作成功！");
        } else {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    logger.error("\"index={}, type={}, id={}\"的文档更新失败！", failure.getIndex(), failure.getType(), failure.getId());
                    logger.error("更新失败详情: {}", failure.getMessage());
                } else {
                    logger.info("\"index={}, type={}, id={}\"的文档更新成功！", bulkItemResponse.getIndex(), bulkItemResponse.getType(), bulkItemResponse.getId());
                }
            }
        }
    }

    /**
     * 批量删除文档
     * @param params
     * @throws IOException
     */
    public void bulkDelete(List<Map<String, String>> params) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String type = dataMap.getOrDefault(TYPE_KEY, TYPE);
            String id = dataMap.get("id");
            if (StringUtils.isNotBlank(id)){
                DeleteRequest request = new DeleteRequest(index, type, id);
                bulkRequest.add(request);
            }
        }
        if (bulkRequest.numberOfActions() == 0) {
            logger.error("操作失败，请检查参数！");
            return;
        }
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (!bulkResponse.hasFailures()) {
            logger.info("批量删除操作成功！");
        } else {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    logger.error("index={}, type={}, id={}的文档删除失败！", failure.getIndex(), failure.getType(), failure.getId());
                    logger.error("删除失败详情: {}", failure.getMessage());
                } else {
                    logger.info("index={}, type={}, id={}的文档删除成功！", bulkItemResponse.getIndex(), bulkItemResponse.getType(), bulkItemResponse.getId());
                }
            }
        }
    }

    /**
     * 批量查找文档
     * @param params
     * @return
     * @throws IOException
     */
    public List<Map<String, Object>> multiGet(List<Map<String, String>> params) throws IOException {
        List<Map<String, Object>> resultList = new ArrayList<>();

        MultiGetRequest request = new MultiGetRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String type = dataMap.getOrDefault(TYPE_KEY, TYPE);
            String id = dataMap.get("id");
            if (StringUtils.isNotBlank(id)) {
                request.add(new MultiGetRequest.Item(index, type, id));
            }
        }
        request.realtime(false);
        request.refresh(true);
        MultiGetResponse response = restHighLevelClient.mget(request, RequestOptions.DEFAULT);
        List<Map<String, Object>> list = parseMGetResponse(response);
        if (!list.isEmpty()) {
            resultList.addAll(list);
        }
        return resultList;
    }
    private List<Map<String, Object>> parseMGetResponse(MultiGetResponse response) {
        List<Map<String, Object>> list = new ArrayList<>();
        MultiGetItemResponse[] responses = response.getResponses();
        for (MultiGetItemResponse item : responses) {
            GetResponse getResponse = item.getResponse();
            if (Objects.nonNull(getResponse)) {
                if (!getResponse.isExists()) {
                    logger.error("index={}, type={}, id={}的文档查找失败，请检查参数！", getResponse.getIndex(), getResponse.getType(), getResponse.getId());
                } else {
                    list.add(getResponse.getSourceAsMap());
                }
            } else {
                MultiGetResponse.Failure failure = item.getFailure();
                ElasticsearchException e = (ElasticsearchException) failure.getFailure();
                if (e.status() == RestStatus.NOT_FOUND) {
                    logger.error("index={}, type={}, id={}的文档不存在！", failure.getIndex(), failure.getType(), failure.getId());
                } else if (e.status() == RestStatus.CONFLICT) {
                    logger.error("index={}, type={}, id={}的文档版本冲突！", failure.getIndex(), failure.getType(), failure.getId());
                }
            }
        }
        return list;
    }


    private XContentBuilder generateBuilder() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
            builder.startObject("properties");
                builder.startObject("message");
                    builder.field("type", "text");
                    // 为message字段，设置分词器为 ik_smart(最粗粒度)
                    builder.field("analyzer", "ikSearchAnalyzer");
                    builder.field("search_analyzer", "ikSearchAnalyzer");
                builder.endObject();

                builder.startObject("level");
                    builder.field("type", "keyword");
                    builder.field("ignore_above", "50");
                builder.endObject();
                builder.startObject(TIMESTAMP);
                    builder.field("type", "date");
                    builder.field("format", "yyyy-MM-dd HH:mm:ss");
                builder.endObject();
            builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * 根据条件搜索日志内容(参数level和messageKey不能同时为空)
     * @param level 日志级别，可以为空
     * @param messageKey 日志信息关键字，可以为空
     * @param startTime 日志起始时间，可以为空
     * @param endTime 日志结束时间，可以为空
     * @param size 返回记录数，可以为空，默认最大返回10条。该值必须小于10000，如果超过10000请使用  {@link #queryAllByConditions}
     * @return
     * @throws IOException
     */
    public List<Map<String, Object>> queryByConditions(String level, String messageKey, Long startTime, Long endTime, Integer size) throws IOException {
        List<Map<String, Object>> resultList = new ArrayList<>();
        if (StringUtils.isBlank(level) && StringUtils.isBlank(messageKey)) {
            logger.error("参数level(日志级别)和messageKey(日志信息关键字)不能同时为空！");
            return resultList;
        }

        QueryBuilder query = generateQuery(level, messageKey, startTime, endTime);
        FieldSortBuilder order = SortBuilders.fieldSort(TIMESTAMP).order(SortOrder.DESC);
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.timeout(TimeValue.timeValueMinutes(2L));
        searchBuilder.query(query);
        searchBuilder.sort(order);
        if (Objects.nonNull(size)) {
            searchBuilder.size(size);
        }

        SearchRequest request = new SearchRequest(INDEX).types(TYPE);
        request.source(searchBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        int failedShards = response.getFailedShards();
        if (failedShards > 0) {
            logger.error("部分分片副本处理失败！");
            for (ShardSearchFailure failure : response.getShardFailures()) {
                String reason = failure.reason();
                logger.error("分片处理失败原因：{}", reason);
            }
        }
        List<Map<String, Object>> list = parseSearchResponse(response);
        if (!list.isEmpty()) {
            resultList.addAll(list);
        }
        return resultList;
    }

    private QueryBuilder generateQuery(String level, String messageKey, Long startTime, Long endTime) {
        // term query(检索level)
        TermQueryBuilder levelQuery = null;
        if (StringUtils.isNotBlank(level)) {
            levelQuery = QueryBuilders.termQuery("level", level.toLowerCase());
        }
        // match query(检索message)
        MatchQueryBuilder messageQuery = null;
        if (StringUtils.isNotBlank(messageKey)) {
            messageQuery = QueryBuilders.matchQuery("message", messageKey);
        }
        // range query(检索timestamp)
        RangeQueryBuilder timeQuery = QueryBuilders.rangeQuery(TIMESTAMP);
        timeQuery.format("yyyy-MM-dd HH:mm:ss");
        if (Objects.isNull(startTime)) {
            if (Objects.isNull(endTime)) {
                timeQuery = null;
            } else {
                timeQuery.lte(endTime);
            }
        } else {
            if (Objects.isNull(endTime)) {
                timeQuery.gte(startTime);
            } else {
                timeQuery.gte(startTime).lte(endTime);
            }
        }
        // 将上述三个query组合
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (Objects.nonNull(levelQuery)) {
            boolQuery.must(levelQuery);
        }
        if (Objects.nonNull(messageQuery)) {
            boolQuery.must(messageQuery);
        }
        if (Objects.nonNull(timeQuery)) {
            boolQuery.must(timeQuery);
        }
        return boolQuery;
    }

    private List<Map<String, Object>> parseSearchResponse(SearchResponse response){
        List<Map<String, Object>> resultList = new ArrayList<>();
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            resultList.add(hit.getSourceAsMap());
        }
        return resultList;
    }

    /**
     * 根据条件，搜索全部符合的记录(参数level和messageKey不能同时为空)
     * @param level 日志级别，可以为空
     * @param messageKey 日志信息关键字，可以为空
     * @param startTime 日志起始时间，可以为空
     * @param endTime 日志结束时间，可以为空
     * @return
     */
    public List<Map<String, Object>> queryAllByConditions(String level, String messageKey, Long startTime, Long endTime) throws IOException {
        List<Map<String, Object>> resultList = new ArrayList<>();
        if (StringUtils.isBlank(level) && StringUtils.isBlank(messageKey)) {
            logger.error("参数level(日志级别)和messageKey(日志信息关键字)不能同时为空！");
            return resultList;
        }

        QueryBuilder query = generateQuery(level, messageKey, startTime, endTime);
        FieldSortBuilder order = SortBuilders.fieldSort(TIMESTAMP).order(SortOrder.DESC);
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.query(query).sort(order);
        searchBuilder.size(500);

        // 初始化 scroll 上下文
        SearchRequest request = new SearchRequest(INDEX).types(TYPE);
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        request.source(searchBuilder).scroll(scroll);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();
        SearchHit[] searchHits = response.getHits().getHits();
        // 把第一次scroll的数据添加到结果List中
        for (SearchHit searchHit : searchHits) {
            resultList.add(searchHit.getSourceAsMap());
        }
        // 通过传递scrollId循环取出所有相关文档
        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            response = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = response.getScrollId();
            searchHits = response.getHits().getHits();
            // 循环添加剩下的数据
            for (SearchHit searchHit : searchHits) {
                resultList.add(searchHit.getSourceAsMap());
            }
        }
        // 清理 scroll 上下文
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        return resultList;
    }

    /**
     * 根据条件做分页查询(参数level和messageKey不能同时为空)
     * @param level 日志级别，可以为空
     * @param messageKey 日志信息关键字，可以为空
     * @param startTime 日志起始时间，可以为空
     * @param endTime 日志结束时间，可以为空
     * @param pageNum 当前页码，可以为空(默认设为1)
     * @param pageSize 页记录数，可以为空(默认设为10)
     * @return
     * @throws IOException
     */
//    public Page<Map<String, Object>> queryPageByConditions(String level, String messageKey, Long startTime, Long endTime, Integer pageNum, Integer pageSize) throws IOException {
//        if (StringUtils.isBlank(level) && StringUtils.isBlank(messageKey)) {
//            LOGGER.error("参数level(日志级别)、messageKey(日志信息关键字)不能同时为空！");
//            return null;
//        }
//
//        if (Objects.isNull(pageNum)) {
//            pageNum = 1;
//        }
//        if (Objects.isNull(pageSize)) {
//            pageSize = 10;
//        }
//        QueryBuilder query = generateQuery(level, messageKey, startTime, endTime);
//        FieldSortBuilder order = SortBuilders.fieldSort(TIMESTAMP).order(SortOrder.DESC);
//        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
//        searchBuilder.timeout(TimeValue.timeValueMinutes(2L));
//        searchBuilder.query(query);
//        searchBuilder.sort(order);
//        searchBuilder.from(pageNum - 1).size(pageSize);
//
//        SearchRequest request = new SearchRequest(INDEX).types(TYPE);
//        request.source(searchBuilder);
//        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
//        SearchHits hits = response.getHits();
//        int totalRecord = (int) hits.getTotalHits();
//        List<Map<String, Object>> results = new ArrayList<>();
//        for (SearchHit hit : hits.getHits()) {
//            results.add(hit.getSourceAsMap());
//        }
//
//        RestResponse<Map<String, Object>> page = new RestResponse<>();
//        page.setPageNum(pageNum);
//        page.setPageSize(pageSize);
//        page.setTotalRecord(totalRecord);
//        page.setResults(results);
//        return page;
//    }
//}






    /**
     * 开启索引
     * @param indexName
     * @return
     */
    public boolean openGroupIndex (String... indexName) {
        if (indexName.length == 0) return true;
        try {
            //分批开启索引
            List<String> indexList = Arrays.asList(indexName);
            for (int i = 0; i < indexList.size();) {
                int limit = i + Constant.ES_BATCH_OPEN_LIMIT;
                Set<String> indexs = subList(indexList, i, limit);
                //final
                logger.info("open index list is {}", indexs.toString());
                OpenIndexRequest request = new OpenIndexRequest(indexs.toArray(new String[0]));
                restHighLevelClient.indices().open(request, RequestOptions.DEFAULT);
                i += Constant.ES_BATCH_OPEN_LIMIT;
            }
            return true;
        } catch (Exception e) {
            logger.error("open index error {}", e.getLocalizedMessage(), e);
            return false;
        }
    }

    /**
     * 关闭索引
     * @param indexList
     */
    public void closeIndex (List<String> indexList) {
        if (indexList.isEmpty()) return;
        for (int i = 0; i < indexList.size();) {
            int limit = i + Constant.ES_BATCH_CLOSE_LIMIT;
            Set<String> indexs = subList(indexList, i, limit);
            logger.info("close index list is {}", indexs.toString());
            CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indexs.toArray(new String[0]));
            restHighLevelClient.indices().closeAsync(closeIndexRequest, RequestOptions.DEFAULT, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse closeIndexResponse) {
                    String acknowledged = closeIndexResponse.isAcknowledged() == true ? "success" : "fail" ;
                    logger.info("close index" + acknowledged);
                }
                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ElasticsearchStatusException) {
                        if (e.getMessage().contains("index_closed_exception")) {
                            return;
                        }
                    }
                    logger.error("close index exception: {}", e.getLocalizedMessage(), e);
                }
            });
            indexs = null;
            i+= Constant.ES_BATCH_CLOSE_LIMIT;
        }
    }

    //异步查询
    public void getGroupStatisticAsync (List<String> indexList, Supplier<SearchRequest> requestSupplier, Supplier<ActionListener<SearchResponse>> responseSupplier) {
        SearchRequest searchRequest = requestSupplier.get();
        try {
            ActionListener<SearchResponse> listener = responseSupplier.get();
            restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, listener);
        } catch (Exception e) {
            //抛出异常说明该批查询都出现问题
            logger.error("multi request error {}", e.getLocalizedMessage(), e);
        }
    }

    //同步查询
    public void getGroupStatisticSync (List<String> indexList, Supplier<SearchRequest> requestSupplier, Consumer<SearchResponse> consumerResponse) {
        SearchRequest searchRequest = requestSupplier.get();
        try {
            //ActionListener<MultiSearchResponse> listener = responseSupplier.get();
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            consumerResponse.accept(searchResponse);
        } catch (Exception e) {
            //抛出异常说明该批查询都出现问题 查询出现异常，直接不操作
            logger.error("multi request error {}", e.getLocalizedMessage(), e);
        }
    }

    /**
     * 判断索引是否开启还是关闭
     * @param indexList
     * @return
     *        开启的索引
     *        关闭的索引
     *        不存在的索引
     */
    public Map<String, Set<String>> isExistOrOpenIndex (List<String> indexList) {
        Set<String> openList = new HashSet<>();
        Set<String> closeList = new HashSet<>();
        Set<String> notExistList = new HashSet<>();
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        indexList.forEach((index) -> multiSearchRequest.add(new SearchRequest(index).source(searchSourceBuilder.from(1).size(1))));
        try {
            MultiSearchResponse msearch = restHighLevelClient.msearch(multiSearchRequest, RequestOptions.DEFAULT);
            MultiSearchResponse.Item[] responses = msearch.getResponses();
            for (MultiSearchResponse.Item res : responses) {
                if (Objects.isNull(res)) {
                    logger.info("check index response is null------------");
                } else {
                    SearchResponse searchResponse = res.getResponse();
                    if (Objects.isNull(searchResponse)) {
                        ElasticsearchException exception = (ElasticsearchException) res.getFailure();
                        List<String> metadata = exception.getMetadata(Constant.INDEX_METADATA_KEY);
                        if (!exception.getDetailedMessage().contains("index_not_found_exception")) {
                            //将关闭的索引加入队列中
                            closeList.addAll(metadata);
                        } else {
                            notExistList.addAll(metadata);
                        }
                    } else {
                        JSONObject result = JSONObject.parseObject(searchResponse.toString());
                        Assert.notNull(result, "result is null");
                        SearchHits hits = searchResponse.getHits();
                        SearchHit[] innerHit = hits.getHits();
                        //过滤拿到正确的索引
                        for (SearchHit fields : innerHit) {
                            String index = fields.getIndex();
                            if (StringUtils.isNotEmpty(index)) {
                                openList.add(index);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.info("msearch error {}", e.getLocalizedMessage(), e);
            closeList.addAll(indexList);
        }
        Map<String, Set<String>> resultMap = new HashMap<>();
        resultMap.put(Constant.ES_INDEX_MAP_OPEN_KEY, openList);
        resultMap.put(Constant.ES_INDEX_MAP_CLOSE_KEY, closeList);
        resultMap.put(Constant.ES_INDEX_MAP_NOT_EXIST_KEY, notExistList);
        return resultMap;
    }


    /**
     * @param
     * @param indexList
     * @param
     * @return
     */
    public void multiSearchSync (List<String> indexList, SearchSourceBuilder searchSourceBuilder, Consumer<SearchResponse> consumer) {
        //限制个数一组去查询
        for (int i = 0; i < indexList.size();) {
            int limit = i + Constant.ES_BATCH_SEARCH_LIMIT;
            Set<String> indexs = subList(indexList, i, limit);
            //final
            List<String> tempList = new ArrayList<>(indexs);
            //同步查询
            logger.info("check tempList is {}", tempList.toString());
            getGroupStatisticSync(new ArrayList<>(indexs), () ->  getStatisticRequest(tempList, searchSourceBuilder), (searchResponse) -> {
                try {
                    if (searchResponse.status() == RestStatus.OK) {
                        consumer.accept(searchResponse);
                    }
                    //每批数据休眠一段时间
                    TimeUnit.MILLISECONDS.sleep(Constant.ES_BATCH_SEARCH_SLEEP_MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error("sleep exception {}", e.getLocalizedMessage(), e);
                }
            });
            i = i + Constant.ES_BATCH_SEARCH_LIMIT;
        }
    }

    public  <T> List<T> process(SearchResponse response, TypeReference<T> type) throws IOException {
        if (Objects.nonNull(response) && Objects.nonNull(response.getHits())) {
            SearchHit[] hits = response.getHits().getHits();
            List<T> messages = new ArrayList<>(hits.length);
            for (SearchHit hit : hits) {
                messages.add(JSONObject.parseObject(hit.getSourceAsString(), type));
            }
            return messages;
        }
        return new ArrayList<>();
    }

    private Set<String> subList (List<String> indexList, int start, int limit) {
        Set<String> indexs;
        if (limit < indexList.size()) {
            indexs = new HashSet<>(indexList.subList(start, limit));
        } else {
            indexs = new HashSet<>(indexList.subList(start, indexList.size() - 1));
        }
        return indexs;
    }
}
