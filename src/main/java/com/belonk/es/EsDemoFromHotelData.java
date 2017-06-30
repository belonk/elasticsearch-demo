package com.belonk.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by sun on 2017/6/5.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class EsDemoFromHotelData {
    //~ Static fields/initializers =====================================================================================
    private static Logger log = LoggerFactory.getLogger(EsDemoFromHotelData.class);

    public static List<User> users = new LinkedList<>();

    public static String indexName = "hotel";

    public static String indexType = "data";

    //~ Instance fields ================================================================================================

    private TransportClient client;
    //~ Constructors ===================================================================================================

    public EsDemoFromHotelData() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "hmp").build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.0.106"), 9300));
    }

    //~ Methods ========================================================================================================

    public void matchAllQuery() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                // 设置仅仅想返回的字段
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "id", "cityName"}, null))
                // match all，匹配所有
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        System.out.println(sr);
    }

    public void matchQuery() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                // 设置仅仅想返回的字段
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "id", "cityName"}, null))
                // match query，分词后搜索
                .setQuery(QueryBuilders.matchQuery("name", "盛世酒店"))
                .get();
        System.out.println(sr);
    }

    public void multiMatchQuery() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "address", "cityName", "provinceName"}, null))
                //{"took":54,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":3,"max_score":15.941391,"hits":[{"_index":"hotel","_type":"data","_id":"12","_score":15.941391,"_source":{"address":"湖墅南路198号","cityName":"杭州市","name":"杭州逸酒店","provinceName":"浙江省"}},{"_index":"hotel","_type":"data","_id":"29","_score":13.092372,"_source":{"address":"吕康大街100号","cityName":"杭州市","name":"康哥饭店","provinceName":"浙江省"}},{"_index":"hotel","_type":"data","_id":"30","_score":6.398138,"_source":{"address":"水亭门508号","cityName":"衢州市","name":"远方酒店","provinceName":"浙江省"}}]}}
                .setQuery(QueryBuilders.multiMatchQuery("杭州", "name", "address", "cityName", "provinceName"))
                .get();
        System.out.println(sr);
    }

    public void multiMatchQueryWithPerFields() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "address", "cityName", "provinceName"}, null))
                // 匹配name字段和以Name结尾的字段
//                .setQuery(QueryBuilders.multiMatchQuery("杭州", "name", "address", "*Name"))
                //The name field is three times as important as the *Name field.
                // {"took":55,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":3,"max_score":13.092372,"hits":[{"_index":"hotel","_type":"data","_id":"29","_score":13.092372,"_source":{"address":"吕康大街100号","cityName":"杭州市","name":"康哥饭店","provinceName":"浙江省"}},{"_index":"hotel","_type":"data","_id":"12","_score":13.092372,"_source":{"address":"湖墅南路198号","cityName":"杭州市","name":"杭州逸酒店","provinceName":"浙江省"}},{"_index":"hotel","_type":"data","_id":"30","_score":6.398138,"_source":{"address":"水亭门508号","cityName":"衢州市","name":"远方酒店","provinceName":"浙江省"}}]}}
                // 与上边的方式结果排名和打分不同
                .setQuery(QueryBuilders.multiMatchQuery("杭州", "name^3", "address", "*Name"))
                .get();
        System.out.println(sr);
    }

    public void multiMatchQueryWithBestFieldType() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "address", "cityName", "provinceName"}, null))
                // best_fields：Finds documents which match any field, but uses the _score from the best field
//                .setQuery(QueryBuilders.multiMatchQuery("杭州", "name", "address", "*Name")
//                        .type(MultiMatchQueryBuilder.Type.BEST_FIELDS))
                //{"took":68,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":3,"max_score":15.941391,"hits":[{"_index":"hotel","_type":"data","_id":"12","_score":15.941391,"_source":{"address":"湖墅南路198号","cityName":"杭州市","name":"杭州逸酒店","provinceName":"浙江省"}},{"_index":"hotel","_type":"data","_id":"29","_score":13.092372,"_source":{"address":"吕康大街100号","cityName":"杭州市","name":"康哥饭店","provinceName":"浙江省"}},{"_index":"hotel","_type":"data","_id":"30","_score":6.398138,"_source":{"address":"水亭门508号","cityName":"衢州市","name":"远方酒店","provinceName":"浙江省"}}]}}

                // using tie breaker
                .setQuery(QueryBuilders.multiMatchQuery("杭州", "name", "address", "*Name")
                        .type(MultiMatchQueryBuilder.Type.BEST_FIELDS).tieBreaker(0.3f))
                .get();
        System.out.println(sr);
    }

    public void matchPhraseQuery() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                // 设置仅仅想返回的字段
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "id", "cityName"}, null))
                // match phrase query，短语搜索
                //.setQuery(QueryBuilders.matchPhraseQuery("name", "盛世酒店")) // 短语匹配，搜索不到
                .setQuery(QueryBuilders.matchPhraseQuery("name", "盛世酒店").slop(2)) // 间隔slop个词也能匹配。
                .get();
        System.out.println(sr);
    }

    public void termQuery() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                // 设置仅仅想返回的字段
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "id", "cityName"}, null))
                // term query，精确匹配
//                .setQuery(QueryBuilders.termQuery("name", "盛世王朝")) // 无法匹配，因为name默认被分词后存放，匹配不到包含盛世王朝的分词
//                .setQuery(QueryBuilders.termQuery("name", "王")) // 匹配成功
//                .setQuery(QueryBuilders.termQuery("name", "盛世")) // 匹配失败
                .setQuery(QueryBuilders.termQuery("name", "王朝")) // 匹配失败，被解析为单个字符存入token
                .get();
        System.out.println(sr);
    }

    /**
     * The common terms query divides the query terms into two groups: more important (ie low frequency terms) and less
     * important (ie high frequency terms which would previously have been stopwords).
     * <p>
     * First it searches for documents which match the more important terms. These are the terms which appear in fewer
     * documents and have a greater impact on relevance.
     * <p>
     * Then, it executes a second query for the less important terms — terms which appear frequently and have a low impact
     * on relevance. But instead of calculating the relevance score for all matching documents, it only calculates the
     * _score for documents already matched by the first query. In this way the high frequency terms can improve the
     * relevance calculation without paying the cost of poor performance.
     */
    public void commonTermQuery() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "id", "cityName"}, new String[]{"id"}))// 排除id
                .setQuery(
                        QueryBuilders.commonTermsQuery("name", "王朝") // 能够匹配
                )
                .get();
        System.out.println(sr);
    }

    public void boolQuery() {
        // 盛世王朝酒店
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "id", "cityName"}, new String[]{"id"}))// 排除id
                .setQuery(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("name", "王"))
                                .must(QueryBuilders.termQuery("name", "朝"))
                                .must(QueryBuilders.matchQuery("name", "盛世"))
                )
                .get();
        System.out.println(sr);
    }

    public void boolQuery1() {
        // 盛世王朝酒店
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "id", "cityName"}, new String[]{"id"}))// 排除id
                .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("name", "饭世")))
//                .setQuery(QueryBuilders.boolQuery()
//                        .should(QueryBuilders.matchQuery("name", "饭"))
//                        .should(QueryBuilders.matchQuery("name", "盛世"))
//                )
                .get();
        System.out.println(sr);
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/5.4/query-dsl-query-string-query.html
     */
    public void queryStringQuery() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "id", "cityName"}, new String[]{"id"}))// 排除id
                .setQuery(
//                        QueryBuilders.queryStringQuery("name:汉克") // 分词匹配
//                        QueryBuilders.queryStringQuery("name:(7天 OR 王朝)") // 分词匹配，多个内容
//                        QueryBuilders.queryStringQuery("name:\"7天\"") // 精确短语匹配
                        QueryBuilders.queryStringQuery("_exists_:phone") // phone非null

                )
                .get();
        System.out.println(sr);
    }

    public void simpleQueryStringQuery() {
        SearchResponse sr = client.prepareSearch(indexName)
                .setTypes(indexType).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSource(SearchSourceBuilder.searchSource().fetchSource(new String[]{"name", "id", "cityName"}, new String[]{"id"}))// 排除id
                .setQuery(
//                        QueryBuilders.simpleQueryStringQuery("name:汉克")
//                        QueryBuilders.queryStringQuery("name:(7天|王朝)")
//                        QueryBuilders.queryStringQuery("name:杭州")
                        QueryBuilders.queryStringQuery("杭州").field("name").field("provinceName").field("cityName")
//                        QueryBuilders.queryStringQuery("+7天 -测试").field("name")
                )
                .get();
        System.out.println(sr);
    }

    public static void main(String[] args) throws UnknownHostException {
        EsDemoFromHotelData demo = new EsDemoFromHotelData();
//        demo.matchAllQuery();
//        demo.matchQuery();
//        demo.multiMatchQuery();
//        demo.multiMatchQueryWithPerFields();
//        demo.multiMatchQueryWithBestFieldType();
//        demo.matchPhraseQuery();
//        demo.termQuery();
//        demo.commonTermQuery();
//        demo.boolQuery();
//        demo.boolQuery1();
//        demo.queryStringQuery();
        demo.simpleQueryStringQuery();
    }
}
