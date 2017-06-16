package com.belonk.es;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollTask;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
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

    public void bulkProccessor() {

    }

    // search api test
    public void scroll() {
        QueryBuilder qb = termQuery("multi", "test");

        SearchResponse scrollResp = client.prepareSearch("test")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(100).get(); //max of 100 hits will be returned for each scroll
        //Scroll until no hits are returned
        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                //Handle the hit...
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

        demo.bulk();
        demo.closeClient();
    }
}
