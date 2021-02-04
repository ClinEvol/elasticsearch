package com.ujiuye.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.ujiuye.elasticsearch.pojo.Student;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.bind.annotation.RequestMapping;
import sun.jvm.hotspot.debugger.win32.coff.MachineTypes;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ElasticsearchApplicationTests {

    @Resource
    private RestHighLevelClient client;

    //创建索引
    @Test
    void createIndex() throws IOException {
        ////创建索引的请求
        CreateIndexRequest index = new CreateIndexRequest("ujiuye_index");
        //执行请求
        CreateIndexResponse indexResponse = client.indices()
                .create(index, RequestOptions.DEFAULT);
        System.out.println(indexResponse);
    }
    //获取索引
    @Test
    void getIndex() throws IOException {
        //获取索引的请求
        GetIndexRequest index = new GetIndexRequest("ujiuye_index");
        //判断索引是否存在
        boolean exists = client.indices().exists(index, RequestOptions.DEFAULT);
        System.out.println(exists);
        if(exists){
            //获取索引的响应
            GetIndexResponse indexResponse = client.indices().get(index, RequestOptions.DEFAULT);
            //获取索引
            String[] indices = indexResponse.getIndices();
            System.out.println(indices.length);
            System.out.println(indices[0]);
        }
    }
    //删除索引
    @Test
    void deleteIndex() throws IOException {
        //删除索引的请求
        DeleteIndexRequest index = new DeleteIndexRequest("ujiuye_index");
        //执行请求   AcknowledgedResponse表示请求已被确认的响应
        AcknowledgedResponse delete = client.indices().delete(index, RequestOptions.DEFAULT);
        //如果响应被确认，则为true，否则为false
        System.out.println(delete.isAcknowledged());
    }

    //添加文档
    @Test
    void addDocument() throws IOException {
        //创建文档对象
        Student student=new Student("哈士奇",19,new Date(),"888@qq.com");
        //创建索引请求   文件要添加到哪个索引就写哪个索引
        IndexRequest indexRequest = new IndexRequest("ujiuye_index");
        indexRequest.id("1");//设置文件档id
        indexRequest.timeout("1s");//1秒没有响应就抛异常
        //将请求体（我们的数据student--json格式）放入请求对象中
        indexRequest.source(JSON.toJSONString(student), XContentType.JSON);
        //执行请求
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
        System.out.println(indexResponse.toString());
    }

    //查询文档
    @Test
    void getDocument() throws IOException {
        //根据指定的索引和文档ID构造一个新的get请求。
        GetRequest index = new GetRequest("ujiuye_index", "1");
        //不获取返回的——sources上下文了   设置这个文档内容会为空，只判断文档是否存在话的可以设置
        //index.fetchSourceContext(new FetchSourceContext(false));
        //显式地指定将返回的存储字段      设置这个文档内容会为空，只判断文档是否存在话的可以设置
        //index.storedFields("_none_");
        boolean exists = client.exists(index, RequestOptions.DEFAULT);
        if(exists){
            GetResponse documentFields = client.get(index, RequestOptions.DEFAULT);
            //打印文档内容
            System.out.println(documentFields.getSourceAsString());
            System.out.println(documentFields);
        }

    }

    //删除文件档
    @Test
    void deleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("ujiuye_index", "1");
        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }
    //修改文档
    @Test
    public void updateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("ujiuye_index", "1");
        Student student=new Student("小鬼",19,new Date(),"888@qq.com");
        updateRequest.doc(JSON.toJSONString(student),XContentType.JSON);
        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }

    //批量添加
    @Test
    public void name() throws IOException {
    	List<Student> list=new ArrayList<>();
    	list.add(new Student("王向东",19,new Date(),"111@qq.com"));
        list.add(new Student("王向前",19,new Date(),"222@qq.com"));
        list.add(new Student("王向北",19,new Date(),"333@qq.com"));
        list.add(new Student("王向西",19,new Date(),"444@qq.com"));
        list.add(new Student("王向南",19,new Date(),"555@qq.com"));
        list.add(new Student("王向上",19,new Date(),"666@qq.com"));

        BulkRequest bulkRequest=new BulkRequest();
        bulkRequest.timeout("10s");
        //将数据一个一个添加到BulkRequest对象中
        for (int i = 0; i < list.size(); i++) {
            //批量修改和删除同理，在这里修改对应的请求对象
            bulkRequest.add(new IndexRequest("ujiuye_index")
                    .id(""+i+3)
                    .source(JSON.toJSONString(list.get(i)),XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.status());
    }

    //查询
    @Test
    public void search() throws IOException {
        int page=3;//当前页码
        int size=5;//每页几条
        //查询请求
        SearchRequest searchRequest=new SearchRequest("ujiuye_index");
        //构建请求条件
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        sourceBuilder.highlighter();
        //设置需要精确查询的字段
        TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "向");
        sourceBuilder.query(termQuery);
        //设置分页
        sourceBuilder.from((page - 1) * size);
        sourceBuilder.size(size);
        //响应时间
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //关联搜索条件
        searchRequest.source(sourceBuilder);
        //执行查询
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHit[] hits = search.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            System.out.println(hits[i].getSourceAsString());
        }

    }

    @Test
    public void hight() throws IOException {
        int page=3;//当前页码
        int size=5;//每页几条
        //查询请求
        SearchRequest searchRequest=new SearchRequest("ujiuye_index");
        //构建请求条件
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        sourceBuilder.highlighter();
        //设置需要精确查询的字段
        TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "向");
        sourceBuilder.query(termQuery);
        //设置分页
        sourceBuilder.from((page - 1) * size);
        sourceBuilder.size(size);
        //响应时间
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //高亮
        HighlightBuilder highlightBuilder=new HighlightBuilder();
        highlightBuilder.field("name");//高亮的字段
        highlightBuilder.requireFieldMatch(false);////如果要多个字段高亮,这项要为false
        highlightBuilder.preTags("<span style='color:red;'>");//字段加前缀
        highlightBuilder.postTags("</span>");
        //下面这两项,如果你要高亮如文字内容等有很多字的字段,必须配置,不然会导致高亮不全,文章内容缺失等
        highlightBuilder.fragmentSize(800000); //最大高亮分片数
        highlightBuilder.numOfFragments(0); //从第一个分片获取高亮片段
        //把高亮关联到sourceBuilder
        sourceBuilder.highlighter(highlightBuilder);


        //把搜索条件关联到查询请求中
        searchRequest.source(sourceBuilder);
        //执行查询
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);


        //处理高亮的内容
        SearchHit[] hits = search.getHits().getHits();
        List<Map<String, Object>> list = new ArrayList<>();//保存处理后的内容


        for (SearchHit searchHit :hits){
            //查询结果
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();

            //获取高亮的字段内容 {name=[name], fragments[[陈<span style='color:red;'>向</span>南]]}
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            //把高亮的字段重新放到查询结果中
            HighlightField field = highlightFields.get("name");

            if(field!= null){
                Text[] fragments = field.fragments();
                String n_field = "";
                for (Text fragment : fragments) {
                    n_field += fragment;
                }
                //高亮标题覆盖原标题
                sourceAsMap.put("name",n_field);
            }
            list.add(searchHit.getSourceAsMap());
        }
        list.forEach(System.out::println);

    }





}
