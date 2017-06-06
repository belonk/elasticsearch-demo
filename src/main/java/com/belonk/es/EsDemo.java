package com.belonk.es;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollTask;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

    public void get() {
        users.forEach(user -> {
            GetRequestBuilder requestBuilder = client.prepareGet(indexName, indexType, String.valueOf(user.getId()));
            requestBuilder.setOperationThreaded(true); // 单独的线程执行
            GetResponse response = requestBuilder.get();
            String source = response.getSourceAsString();
            System.out.println("source string : " + source);
            Map map = response.getSource();
            System.out.println("source map    : " + map);
            Map map1 = response.getSourceAsMap();
            System.out.println("source map1   : " + map1);

            // Index name
            String index = response.getIndex();
            // Type name
            String type = response.getType();
            // Document ID (generated or not)
            String id = response.getId();
            // Version (if it's the first time you index this document, you will get: 1)
            long version = response.getVersion();

            System.out.println("name    : " + index);
            System.out.println("type    : " + type);
            System.out.println("id      : " + id);
            System.out.println("version : " + version);
        });
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

    public void update() {

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

    public static void main(String[] args) throws IOException {
        EsDemo demo = new EsDemo();
//        demo.basicDemo();
//        demo.index();
//        demo.get();
//        demo.delete();
//        demo.deleteByQuery();
        demo.deleteByQueryAsync();
        demo.update();

        demo.closeClient();
    }
}
