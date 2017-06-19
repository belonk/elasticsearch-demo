package com.belonk.es;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollTask;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by sun on 2017/6/5.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class EsDemo {
    //~ Static fields/initializers =====================================================================================
    private static Logger log = LoggerFactory.getLogger(EsDemo.class);

    public static List<User> users = new LinkedList<>();

    public static String indexName = "user";

    public static String indexType = "info";

    //~ Instance fields ================================================================================================

    private TransportClient client;
    //~ Constructors ===================================================================================================

    static {
        users.add(new User(1L, "sun1", "123456", 30, 1,
                new String[]{"eat", "music", "programming"}));
        users.add(new User(2L, "sun2", "123456", 30, 1,
                new String[]{"read", "music", ""}));
        users.add(new User(3L, "sun3", "123456", 30, 1,
                new String[]{"read", "dota", "programming"}));
        users.add(new User(4L, "sun4", "123456", 30, 1,
                new String[]{"sport", "dota"}));
    }

    public EsDemo() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
    }

    //~ Methods ========================================================================================================

    public void index() {
        users.forEach(user -> {
            String json = user.toJson();
            IndexRequestBuilder requestBuilder = client.prepareIndex(indexName, indexType, String.valueOf(user.getId()));
            requestBuilder.setSource(json, XContentType.JSON);
            IndexResponse response = requestBuilder.get();
            print(response);
        });
    }

    private void print(DocWriteResponse response) {
        // Index name
        String index = response.getIndex();
        // Type name
        String type = response.getType();
        // Document ID (generated or not)
        String id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long version = response.getVersion();
        // status has stored current instance statement.
        RestStatus status = response.status();

        System.out.println("name    : " + index);
        System.out.println("type    : " + type);
        System.out.println("id      : " + id);
        System.out.println("version : " + version);
        System.out.println("st      : " + status.getStatus());
    }

    public void getAll() {
        users.forEach(user -> {
            GetRequestBuilder requestBuilder = client.prepareGet(indexName, indexType, String.valueOf(user.getId()));
            requestBuilder.setOperationThreaded(true); // 单独的线程执行
            GetResponse response = requestBuilder.get();
            String source = response.getSourceAsString();
            System.out.println("source string : " + source);
//            Map map = response.getSource();
//            System.out.println("source map    : " + map);
//            Map map1 = response.getSourceAsMap();
//            System.out.println("source map1   : " + map1);

//            // Index name
//            String index = response.getIndex();
//            // Type name
//            String type = response.getType();
//            // Document ID (generated or not)
//            String id = response.getId();
//            // Version (if it's the first time you index this document, you will get: 1)
//            long version = response.getVersion();

//            System.out.println("name    : " + index);
//            System.out.println("type    : " + type);
//            System.out.println("id      : " + id);
//            System.out.println("version : " + version);
        });
    }

    public void get(String id) {
        GetRequestBuilder requestBuilder = client.prepareGet(indexName, indexType, id);
        requestBuilder.setOperationThreaded(true); // 单独的线程执行
        GetResponse response = requestBuilder.get();
        String source = response.getSourceAsString();
        System.out.println("source string : " + source);
//        Map map = response.getSource();
//        System.out.println("source map    : " + map);
//        Map map1 = response.getSourceAsMap();
//        System.out.println("source map1   : " + map1);
    }

    public void delete() {
        DeleteResponse response = client.prepareDelete(indexName, indexType, "1").get();
        print(response);
    }

    public void deleteByQuery() {
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client).filter(QueryBuilders.matchQuery("id", "1"))
                .source(indexName).get();
        long deleted = response.getDeleted();
        long batches = response.getBatches();
        long bulkRetries = response.getBulkRetries();
        BulkByScrollTask.Status status = response.getStatus();
        System.out.println("deleted : " + bulkRetries);
        System.out.println("batches : " + batches);
        System.out.println("bulkRetries : " + deleted);
        System.out.println("status : " + status);
    }

    public void deleteByQueryAsync() {
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client).filter(QueryBuilders.matchQuery("id", "1"))
                .source(indexName)
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) { // 未执行？？？
                        long deleted = response.getDeleted();
                        long batches = response.getBatches();
                        long bulkRetries = response.getBulkRetries();
                        BulkByScrollTask.Status status = response.getStatus();
                        System.out.println("deleted : " + bulkRetries);
                        System.out.println("batches : " + batches);
                        System.out.println("bulkRetries : " + deleted);
                        System.out.println("status : " + status);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        System.out.println("delete failed.");
                    }
                });
    }

    public void update() throws ExecutionException, InterruptedException {
        User user = new User(1L, "sun3", "123456", 30, 1,
                new String[]{"dota2", "programming", "music"});
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName);
        updateRequest.type(indexType);
        updateRequest.id("2");
        updateRequest.doc(user.toJson(), XContentType.JSON);
        UpdateResponse response = client.update(updateRequest).get();
//        print(response);
    }

    public void upsert(User user) throws ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest(indexName, indexType, String.valueOf(user.getId()))
                .source(users.get(0).toJson(), XContentType.JSON);
        UpdateRequest updateRequest = new UpdateRequest(indexName, indexType, String.valueOf(user.getId()))
                .doc(user.toJson(), XContentType.JSON)
                .upsert(indexRequest);
        // If it does not exist, we will have a new document:
        client.update(updateRequest).get();
    }

    public void multiGet(String[] ids) {
        MultiGetRequestBuilder requestBuilder = client.prepareMultiGet();
        //单个添加
//        for (String id : ids) {
//            requestBuilder.add(indexName, indexType, id);
//        }
        // 批量添加，by a list of ids for the same index / type
        requestBuilder.add(indexName, indexType, ids);

        MultiGetResponse multiGetItemResponses = requestBuilder.get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) { // you can check if the document exists
                String json = response.getSourceAsString();
                System.out.println("Get source : " + json);
            }
        }
    }

    // bulk test start =================================================================================================

    public void bulk() throws IOException {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        // either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
        );

        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "another post")
                        .endObject()
                )
        );

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
            System.out.println("bulk failed.");
        }
    }

    // 异步执行
    public void bulkProccessor() throws InterruptedException, IOException {
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client, // 客户端
                new BulkProcessor.Listener() {
                    /**
                     * This method is called just before bulk is executed. You can for example see the numberOfActions
                     * with request.numberOfActions().
                     *
                     * @param executionId
                     * @param request
                     */
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        System.out.println("beforeBulk...");
                        System.out.println("executionId : " + executionId);
                        System.out.println("request.numberOfActions() : " + request.numberOfActions());
                    }

                    /**
                     * This method is called after bulk execution. You can for example check if there was some
                     * failing requests with response.hasFailures().
                     *
                     * @param executionId
                     * @param request
                     * @param response
                     */
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        System.out.println("after bulk is execution finished...");
                        System.out.println("executionId : " + executionId);
                        System.out.println("response.hasFailures() : " + response.hasFailures());
                    }

                    /**
                     * This method is called when the bulk failed and raised a Throwable
                     */
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        System.out.println("after bulk execution is failed ...");
                        System.out.println("executionId : " + executionId);
                    }
                })
                /*
                 * We want to execute the bulk every 10 000 requests, default is 1000.
                 */
                .setBulkActions(10000)
                /*
                 * We want to flush the bulk every 5mb, default value is 5m.
                 */
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                /*
                 * We want to flush the bulk every 5 seconds whatever the number of requests. BulkProcessor does not set
                 * flushinterval by default.
                 */
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                /*
                 * Set the number of concurrent requests. A value of 0 means that only a single request will be allowed
                 * to be executed. A value of 1 means 1 concurrent request is allowed to be executed while accumulating
                 * new bulk requests.
                 *
                 * sets concurrentRequests to 1, which means an asynchronous execution of the flush operation.
                 */
                .setConcurrentRequests(1)
                /*
                 * Set a custom backoff policy which will initially wait for 100ms, increase exponentially and retries
                 * up to three times. A retry is attempted whenever one or more bulk item requests have failed with an
                 * EsRejectedExecutionException which indicates that there were too little compute resources available
                 * for processing the request. To disable backoff, pass BackoffPolicy.noBackoff().
                 *
                 * sets backoffPolicy to an exponential backoff with 8 retries and a start delay of 50ms. The total wait
                 * time is roughly 5.1 seconds.
                 */
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        // add requests.
        bulkProcessor.add(new IndexRequest("twitter", "tweet", "1").source(
                XContentFactory.jsonBuilder().startObject()
                        .field("name", "testtwitter")
                        .field("date", new Date())
                        .field("content", "haha, this is a test twitter.")
                        .endObject()
        ));
        bulkProcessor.add(new DeleteRequest("twitter", "tweet", "2"));

        /*
         * close the bulk processor.
         *
         * Both methods flush any remaining documents and disable all other scheduled flushes if they were scheduled by
         * setting flushInterval
         */
        /*
         *  If concurrent requests were enabled the awaitClose method waits for up to the specified timeout for all bulk
         *  requests to complete then returns true, if the specified waiting time elapses before all bulk requests
         *  complete, false is returned.
         */
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        /*
         * The close method doesn’t wait for any remaining bulk requests to complete and exits immediately.
         */
        // bulkProcessor.close()
    }

    // 同步执行，测试用
    public void bulkProcessorSync() throws IOException {
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("beforeBulk...");
                System.out.println("executionId : " + executionId);
                System.out.println("request.numberOfActions() : " + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("after bulk is execution finished...");
                System.out.println("executionId : " + executionId);
                System.out.println("response.hasFailures() : " + response.hasFailures());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("after bulk execution is failed ...");
                System.out.println("executionId : " + executionId);
                failure.printStackTrace();
            }
        })
                .setBulkActions(10000)
                .setConcurrentRequests(0)
                .build();

        // Add your requests
        bulkProcessor.add(new IndexRequest("twitter", "tweet", "1").source(
                XContentFactory.jsonBuilder().startObject()
                        .field("name", "testtwitter")
                        .field("date", new Date())
                        .field("content", "haha, this is a test twitter.")
                        .endObject()
        ));
        bulkProcessor.add(new DeleteRequest("twitter", "tweet", "22"));

        // Flush any remaining requests
        bulkProcessor.flush();

        // Or close the bulkProcessor if you don't need it anymore
        bulkProcessor.close();

        // Refresh your indices
        client.admin().indices().prepareRefresh().get();

        // Now you can start searching!
        client.prepareSearch().get();
    }

    // bulk test end ===================================================================================================

    // search api test start ===========================================================================================
    public void basicSearch() {
        /*
        $ CURL -XGET "localhost:9200/user/info/_search?q=username:sun1&pretty"
        {
          "took" : 7,
          "timed_out" : false,
          "_shards" : {
            "total" : 5,
            "successful" : 4,
            "failed" : 0
          },
          "hits" : {
            "total" : 1,
            "max_score" : 0.2876821,
            "hits" : [
              {
                "_index" : "user",
                "_type" : "info",
                "_id" : "1",
                "_score" : 0.2876821,
                "_source" : {
                  "age" : 100,
                  "gender" : 1,
                  "hovers" : [
                    "eat",
                    "music",
                    "programming"
                  ],
                  "id" : 1,
                  "password" : "123456",
                  "username" : "sun1"
                }
              }
            ]
          }
        }
        */
        SearchResponse response = client.prepareSearch(indexName)
                .setTypes(indexType) // empty is all types
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("username", "sun1"))                 // Query
                .setPostFilter(QueryBuilders.rangeQuery("age").gte(100))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .get();
        print(response);
    }

    public void basicSearch1() {
        print(client.prepareSearch().get());
    }

    private void print(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        System.out.println("total hits     : " + searchHits.getTotalHits());
        System.out.println("hits           : " + searchHits.getHits().length);
        System.out.println("getMaxScore()  : " + searchHits.getMaxScore());
        System.out.println("internalHits() :" + searchHits.internalHits().length);
        System.out.println("............................");
        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit);
            System.out.println("docId      ： " + searchHit.docId());

            Map<String, SearchHitField> map = searchHit.getFields();
            System.out.println("fields     : " + map.size());
            for (String s : map.keySet()) {
                System.out.println(s + " : " + map.get(s));
            }
            System.out.println("getScore() : " + searchHit.getScore());
            System.out.println("getShard() : " + searchHit.getShard());
            System.out.println("getIndex() : " + searchHit.getIndex());
            System.out.println("getType()  : " + searchHit.getType());

            Map<String, Object> sourceMap = searchHit.getSource();
            System.out.println("sources : " + sourceMap.size());
            for (String s : sourceMap.keySet()) {
                System.out.println(s + " : " + sourceMap.get(s));
            }
            System.out.println("getExplanation : " + searchHit.getExplanation());
        }
    }

    public void scroll() {
        QueryBuilder qb = termQuery("username", "sun1");

        SearchResponse scrollResp = client.prepareSearch(indexName)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(6000))
                .setQuery(qb)
                .setSize(100).get(); //max of 100 hits will be returned for each scroll
        //Scroll until no hits are returned
        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                System.out.println("total hits     : " + hit.getSourceAsString());
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        }
        while (scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
    }

    public void closeClient() {
        this.client.close();
    }

    public void basicDemo() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        String json = "{\n" +
                "    \"first_name\" : \"John\",\n" +
                "    \"last_name\" :  \"Smith\",\n" +
                "    \"age\" :        25,\n" +
                "    \"about\" :      \"I love to go rock climbing\",\n" +
                "    \"interests\": [ \"sports\", \"music\" ]\n" +
                "}\n";
        IndexResponse response = client.prepareIndex("megacorp", "employee", "1")
                .setSource(json, XContentType.JSON).get();

        // Index name
        String index = response.getIndex();
        // Type name
        String type = response.getType();
        // Document ID (generated or not)
        String id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long version = response.getVersion();
        // status has stored current instance statement.
        RestStatus status = response.status();

        System.out.println("name    : " + index);
        System.out.println("type    : " + type);
        System.out.println("id      : " + id);
        System.out.println("version : " + version);
        System.out.println("st      : " + status.getStatus());
        client.close();
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        EsDemo demo = new EsDemo();
//        demo.basicDemo();
//        demo.index();
//        demo.getAll();
//        demo.delete();
//        demo.deleteByQuery();
//        demo.deleteByQueryAsync();
//        demo.getAll();
//        System.out.println("get end.");
//        demo.update();
//        System.out.println("update end.");
//        demo.getAll();
//        System.out.println("get end.");

//        String id = "2";

//        demo.get(id);
//        User forUpdate = users.get(1);
//        forUpdate.setAge(33);
//        forUpdate.setUsername("sun2");
//        demo.upsert(forUpdate);
//        System.out.println("upsert finished.");
//        demo.get(id);
//        System.out.println("get all");
//        demo.getAll();

//        String[] ids = new String[]{"1", "3"};
//        demo.multiGet(ids);

//        demo.bulk();
//        demo.bulkProccessor();
//        demo.bulkProcessorSync();

//        demo.basicSearch();
//        demo.basicSearch1();

        demo.scroll();

        demo.closeClient();
    }
}
