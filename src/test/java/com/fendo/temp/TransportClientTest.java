package com.fendo.temp;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;



public class TransportClientTest {


    private TransportClient client;


    private final static String article="article";
    private final static String content="content";

    @Before
    public void getClient() throws Exception{
        //设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();// 集群名
        //创建client
//        client  = new PreBuiltTransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.0.48"), 9300));
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.0.48"), 9300));
    }

    /**
     * -----------------------------------------增(创建索引,增加映射,新增文档)
     */


    /**
     * 创建索引的四种方法
     */

    @Test
    public void JSON(){
        String json = "{" +
                "\"id\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

    }

    /**
     * 创建索引并添加映射
     * @throws IOException
     */
    @Test
    public void CreateIndexAndMapping() throws Exception{

        CreateIndexRequestBuilder  cib=client.admin().indices().prepareCreate(article);
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties") //设置之定义字段
                .startObject("author")
                .field("type","string") //设置数据类型
                .endObject()
                .startObject("title")
                .field("type","string")
                .endObject()
                .startObject("content")
                .field("type","string")
                .endObject()
                .startObject("price")
                .field("type","string")
                .endObject()
                .startObject("view")
                .field("type","string")
                .endObject()
                .startObject("tag")
                .field("type","string")
                .endObject()
                .startObject("date")
                .field("type","date")  //设置Date类型
                .field("format","yyyy-MM-dd HH:mm:ss") //设置Date的格式
                .endObject()
                .endObject()
                .endObject();
        cib.addMapping(content, mapping);

        CreateIndexResponse res=cib.execute().actionGet();

        System.out.println("----------添加映射成功----------");
    }

    /**
     *  创建索引并添加文档
     * @throws Exception
     */
    @Test
    public void addIndexAndDocument() throws Exception{

        Date time = new Date();

        IndexResponse response = client.prepareIndex(article, content)
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("id","447")
                        .field("author","fendo")
                        .field("title","192.138.1.2")
                        .field("content","这是JAVA有关的书籍")
                        .field("price","20")
                        .field("view","100")
                        .field("tag","a,b,c,d,e,f")
                        .field("date",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time))
                        .endObject())
                .get();
        System.out.println("添加索引成功,版本号："+response.getVersion());
    }


    /**
     * -------------------------------------Bulk---------------------------------
     */


    /**
     * bulkRequest
     * @throws Exception
     */
    @Test
    public void bulkRequest() throws Exception {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        Date time = new Date();

        // either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex(article, content, "199")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id","199")
                        .field("author","fendo")
                        .field("title","BULK")
                        .field("content","这是BULK有关的书籍")
                        .field("price","40")
                        .field("view","300")
                        .field("tag","a,b,c")
                        .field("date",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time))
                        .endObject()
                )
        );

        bulkRequest.add(client.prepareIndex(article,content, "101")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id","101")
                        .field("author","fendo")
                        .field("title","ACKSE")
                        .field("content","这是ACKSE有关的书籍")
                        .field("price","50")
                        .field("view","200")
                        .field("tag","a,b,c")
                        .field("date",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time))
                        .endObject()
                )
        );

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
            //System.out.println(bulkResponse.getTook());
        }
    }





    //手动 批量更新
    @Test
    public void multipleBulkProcessor() throws Exception {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for(int i=500;i<1000;i++){
            //业务对象
            String jsons = "{\"id\":\""+i+"\",\"author\":\"ckse\",\"title\":\"windows编程\",\"content\":\"windows 32 API编程\",\"price\":\"99\",\"view\":\"222\",\"date\":\"2017-08-01 17:21:18\"}";
            IndexRequestBuilder indexRequest = client.prepareIndex("article", "content")
                    //指定不重复的ID
                    .setSource(jsons).setId(String.valueOf(i));
            //添加到builder中
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
            System.out.println(bulkResponse.buildFailureMessage());
        }
        System.out.println("创建成功!!!");
    }

    /**
     * 使用Bulk批量添加导入数据
     *
     */
    @Test
    public void ImportBulk(){
        FileReader fr = null;
        BufferedReader bfr = null;
        String line=null;
        try {
            File file = new File("F:\\Source\\Elasticsearch\\TransportClient\\src\\main\\resources\\bulk.txt");
            fr=new FileReader(file);
            bfr=new BufferedReader(fr);
            BulkRequestBuilder bulkRequest=client.prepareBulk();
            int count=0;
            while((line=bfr.readLine())!=null){
                bulkRequest.add(client.prepareIndex(article,content).setSource(line));
                if (count%10==0) {
                    bulkRequest.execute().actionGet();
                }
                count++;
            }
            bulkRequest.execute().actionGet();
            System.out.println("导入成功!!!");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                bfr.close();
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 使用Bulk批量导出数据
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void ExportBulk() throws Exception{


        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("article").setQuery(queryBuilder).get();

        SearchHits resultHits = response.getHits();
        System.out.println(JSONObject.toJSON(resultHits));

        FileWriter fw=null;
        BufferedWriter bfw =null;
        try {
            File file = new File("F:\\Source\\Elasticsearch\\TransportClient\\src\\main\\resources\\bulk.txt");
            fw = new FileWriter(article);
            bfw = new BufferedWriter(fw);
            if (resultHits.getHits().length == 0) {
                System.out.println("查到0条数据!");
            } else {
                for (int i = 0; i < resultHits.getHits().length; i++) {
                    String jsonStr = resultHits.getHits()[i]
                            .getSourceAsString();
                    System.out.println(jsonStr);
                    bfw.write(jsonStr);
                    bfw.write("\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                bfw.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * -----------------------------------------删(删除索引,删除文档)
     */


    /**
     * 删除整个索引库
     */
    @Test
    public void deleteAllIndex(){

        String indexName="article";

        /**
         * 两种方式如下:
         */

        //1)
        //可以根据DeleteIndexResponse对象的isAcknowledged()方法判断删除是否成功,返回值为boolean类型.
        DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(indexName)
                .execute().actionGet();
        System.out.println("是否删除成功:"+dResponse.isAcknowledged());

        //2)
        //如果传人的indexName不存在会出现异常.可以先判断索引是否存在：
        IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);

        IndicesExistsResponse inExistsResponse = client.admin().indices()
                .exists(inExistsRequest).actionGet();

        //根据IndicesExistsResponse对象的isExists()方法的boolean返回值可以判断索引库是否存在.
        System.out.println("是否删除成功:"+inExistsResponse.isExists());
    }

    /**
     * 通过ID删除
     */
    @Test
    public void deleteById(){
        DeleteResponse dResponse = client.prepareDelete(article,content, "AV49wyfCWmWw7AxKFxeb").execute().actionGet();
        if ("OK".equals(dResponse.status())) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }


    /**
     * 通过Query delete删除
     */
    @Test
    public void queryDelete() {
//		   String guid="AV49wyfCWmWw7AxKFxeb";
//		   String author="kkkkk";
//		   DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
//		         .source(article)
//		         .filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("id", guid)).must(QueryBuilders.termQuery("author", author)).must(QueryBuilders.typeQuery(content)))
//		         .get();
    }


    /**
     * 使用matchAllQuery删除所有文档
     */
    @Test
    public void deleteAll(){
//		   DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
//	         .source(article)
//	         .filter(QueryBuilders.matchAllQuery())
//	         .get();
    }


    /**
     * bulk批量通过指定id删除方法
     */
    @Test
    public void batchUndercarriageFamilies() {
        List<String> publishIds=new ArrayList<>();
        publishIds.add("AV49wyfCWmWw7AxKFxeY");
        publishIds.add("AV49wyfCWmWw7AxKFxea");
        BulkRequestBuilder builder=client.prepareBulk();
        for(String publishId:publishIds){
            System.out.println(publishId);
            builder.add(client.prepareDelete(article, content, publishId).request());

        }
        BulkResponse bulkResponse = builder.get();
        System.out.println(bulkResponse.status());
    }




    /**
     * -----------------------------------------改()
     */


    /**
     * 更新文档
     * @throws Exception
     */
    @Test
    public void updateDocument() throws Exception{

        Date time = new Date();

        //创建修改请求
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(article);
        updateRequest.type(content);
        updateRequest.id("AV4xv5gAZLX8AvCc6ZWZ");
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("author","FKSE")
                .field("title","JAVA思想")
                .field("content","注意:这是JAVA有关的书籍")
                .field("date",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time))
                .endObject());

        UpdateResponse response = client.update(updateRequest).get();
        System.out.println("更新索引成功");
    }



    /**
     * -----------------------------有问题：要引入:reindex
     */


    /**
     * UpdateByQueryRequestBuilder
     * @throws Exception
     */
    @Test
    public void updateByQueryRequestBuilder() throws Exception {
//        UpdateByQueryRequestBuilder updateByQueryRequestBuilder = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
//        updateByQueryRequestBuilder
//                .script(new Script(ScriptType.INLINE,"painless","ctx_source.likes++",null))
//                .source()
//                .setQuery(QueryBuilders.termQuery("author","kkkkk"))
//                .setIndices(article)
//                .get();
    }


    /**
     * updateByQueryRequestBuilders
     */
    @Test
    public void updateByQueryRequestBuilders(){

//    	Map<String, Object> maps=new HashMap<>();
//    	maps.put("orgin_session_id", 10);
//    	maps.put("orgin_session_id", 11);
//    	maps.put("orgin_session_id", 12);
//    	maps.put("orgin_session_id", 13);
//
//    	Set<Map<String, Object>> docs = new HashSet<>();
//    	docs.add(maps);
//
//    	UpdateByQueryRequestBuilder  ubqrb = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
//        for (Map<String, Object> doc : docs) {
//            if (doc==null || doc.isEmpty()){
//                return;
//            }
//            Script script = new Script("ctx._source.price = ctx._version");
//
//            System.out.println(doc.get("orgin_session_id"));
//
//            //BulkIndexByScrollResponse
//            BulkByScrollResponse scrollResponse = ubqrb.source(article).script(script)
//                            .filter(QueryBuilders.matchAllQuery()).get();
//            for (BulkItemResponse.Failure failure : scrollResponse.getBulkFailures()) {
//            	System.out.println(failure.getMessage());
//            }
//        }
    }

    /**
     * prepareUpdate
     * @throws Exception
     */
    @Test
    public void prepareUpdate() throws Exception {

        XContentBuilder endObject = XContentFactory.jsonBuilder().startObject().field("author","AAAAAAAAAAAAAAA").endObject();
        UpdateResponse response = client.prepareUpdate(article, content, "AV49wyfCWmWw7AxKFxeb").setDoc(endObject).get();
        System.out.println(response.getVersion());

    }}







