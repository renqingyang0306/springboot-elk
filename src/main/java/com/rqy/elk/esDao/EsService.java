package com.rqy.elk.esDao;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.rqy.elk.core.Constant;
import net.logstash.logback.encoder.org.apache.commons.lang.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
     * 根据时间（天）构造查询条件 0 代表所有
     * @return
     */
//    public SearchSourceBuilder getSearchSourceBuilderByDays (long days) {
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        //TODO
//        searchSourceBuilder.size(Constant.ES_BATCH_SCROLL_SIZE);
//        if (days > 0) {
//            long endTime = LocalDateTime.now().minusDays(days).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//            searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery(Constant.MESSAGE_FIELD_NAME_SEND_TIME).gte(endTime)));
//        } else {
//            searchSourceBuilder.query(QueryBuilders.boolQuery());
//        }
//        return searchSourceBuilder;
//    }

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
