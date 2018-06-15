package day16;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class ElasticSearch_Test {

    private TransportClient client = null;

    /**
     * 获取Transport 的客户端
     */
    @Before
    public void getClient() throws Exception {
        client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
    }

    /**
     * 删除索引下的内容
     */
    @Test
    public void deleteIndex() {
        final DeleteResponse response = client.prepareDelete("blog", "article", "1").get();
        System.err.println(response.isFound());
    }

    /**
     * 使用json字符串创建索引并创建文档,自动创建映射
     */
    @Test
    public void createIndexText_1() {
        String source = "{\"id\":\"1\",\"title\":\"今天天气真好\",\"content\":\"今天天气真好,外面没有雾霾,车还不堵,但是,读不读车管我毛事\"}";
        final IndexRequestBuilder builder = client.prepareIndex("blog", "article", "1").setSource(source);
        final IndexResponse response = builder.get();
        //获取响应的信息
        System.err.println("索引名称:" + response.getIndex());
        System.err.println("索引类型:" + response.getType());
        System.err.println("索引ID:" + response.getId());
        System.err.println("索引版本号:" + response.getVersion());
        System.err.println("是否创建成功:" + response.isCreated());
        //关闭连接
        client.close();
    }

    /**
     * 使用map创建文档
     */
    @Test
    public void createDocumentWithMapSource() {
        Map<String, Object> source = new HashMap<>();
        source.put("id", "2");
        source.put("title", "喜迎GIF 31周年：谷歌在GitHub上发布开源的CLI终端转义工具");
        source.put("content", "为了迎接 GIF 的 31 周岁生日，谷歌特地在 GitHub 上发布了开源的 GIF 转 CLI 终端工具。作为一项被“万恶之源表情包”所采用的动图技术，其可以一路追溯到 1987 年。谷歌发布的这款工具，全称为“GIF for CLI”，用户可以借助它来将图像转成基于 ACSII 码的“图形交换格式”。\n" +
                "\n" +
                "Sean Hayes 在谷歌博客上写到：这是一款可以将 GIF 动图、短视频、或者 Tenor GIF API 转换为 ASCII 艺术的工具。\n" +
                "\n" +
                "在登陆工作站的时候，你喜欢的 GIF 图像就能够以 ASCII 码的形式向你表示欢迎。借助 ANSI 转义序列，可以实现对动画和色彩的支持。\n" +
                "\n" +
                "对于选中 GIF 格式文件（或网址 / Tenor 查询），当命令行程序运行时，它会通过 ffmpeg 将动画 GIF 或短视频分割为静态 jpg 格式。\n" +
                "\n" +
                "然后这些 jpg 帧会被转换成 ASCII 帧（缓存在 $HOME/.cache/gif-for-cli 中），程序将每一帧打印到控制台，用 ANSI 转义序列在每帧之间清理控制台。");
        final IndexRequestBuilder indexRequestBuilder = client.prepareIndex("blog", "article", "2").setSource(source);
        final IndexResponse response = indexRequestBuilder.get();
        //获取响应的信息
        System.err.println("索引名称:" + response.getIndex());
        System.err.println("索引类型:" + response.getType());
        System.err.println("索引ID:" + response.getId());
        System.err.println("索引版本号:" + response.getVersion());
        System.err.println("是否创建成功:" + response.isCreated());
        //关闭连接
        client.close();
    }


    public void testUpdate_1() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("blog");
        updateRequest.type("article");
        updateRequest.id("1");
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("id", "1"))
                .fields("title", "看")
                .fields("content", "看,前方的那个小伙子被打了,为啥?我怎么知道为啥");

    }

    /**
     * 更新文档数据
     *
     * @throws Exception
     */
    @Test
    public void testUpdate_2() throws Exception {
        UpdateResponse updateResponse = client.update(new UpdateRequest("blog", "article", "2").doc(XContentFactory.jsonBuilder().startObject()
                .field("id", "2")
                .field("title", "更新:这是更新后的内容落")
                .field("content", "这是更新后的Content后的数据")
                .endObject())).get();
        //获取响应的信息
        System.err.println("索引名称:" + updateResponse.getIndex());
        System.err.println("索引类型:" + updateResponse.getType());
        System.err.println("索引ID:" + updateResponse.getId());
        System.err.println("索引版本号:" + updateResponse.getVersion());
        System.err.println("是否创建成功:" + updateResponse.isCreated());
        //关闭连接
        client.close();
    }

    /**
     * 更新文档数据
     *
     * @throws Exception
     */
    @Test
    public void testUpdate_3() throws Exception {

        // 设置一个查询的条件
        //使用ID进行查询,如果查找不到数据,就添加request的数据
        IndexRequest request = new IndexRequest("blog", "article", "4").source(XContentFactory.jsonBuilder().startObject()
                .field("id", "5")
                .field("title", "世界杯你看球吗?")
                .field("content", "看啊,那些踢得真好")
                .endObject());

        //设置更新的数据,使用ID查询,如果查找到,就更新updateRequest的数据
        final UpdateRequest updateRequest = new UpdateRequest("blog", "article", "5");
        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
                .field("title", "ElasticSearch").endObject()).upsert(request);
        final UpdateResponse updateResponse = client.update(updateRequest).get();

        //获取响应的信息
        System.err.println("索引名称:" + updateResponse.getIndex());
        System.err.println("索引类型:" + updateResponse.getType());
        System.err.println("索引ID:" + updateResponse.getId());
        System.err.println("索引版本号:" + updateResponse.getVersion());
        System.err.println("是否创建成功:" + updateResponse.isCreated());
        //关闭连接
        client.close();
    }


    /**
     * 删除文档数据
     *
     * @throws Exception
     */
    @Test
    public void testDeleteDoc_1() throws Exception {
        client.prepareDelete("blog", "article", "4").execute().actionGet();
        client.close();
    }


    /**
     * SearchResponse 获取,支持各种查询
     */
    @Test
    public void testSearch() {
        /**queryStringQuery("搜索关键字"),
         * 会在每个字段去进行条件搜索
         *  此时并没有加入分词器,所以在搜索的时候是以单个字来搜索的
         *  称为queryStringQuery查询
         * */
//        final SearchResponse response = client.prepareSearch("blog")
//                .setTypes("article")
//
//                .setQuery(QueryBuilders.queryStringQuery("球")).get();


        /**
         * 词条查询,经匹配在给定字段中含有该词条的文档,而且是确切的未经分析的词条
         * termQuery---词条查询
         *
         * 没有查询到"真好看"(content中实际存在这个词)
         * 但是可以查询到单个字"真"的结果
         */
//        SearchResponse response = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.termQuery("content", "真")).get();


        /**
         * 通配符查询 wildcardQuery("含有通配符的条件")
         * 在查询中可以使用*:0个或匹配多个
         * ?:一个字符
         * 等通配符
         */
//        SearchResponse response = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.wildcardQuery("content", "真*")).get();

        /**
         * 模糊查询,fuzzy
         * 它基于距离算法来匹配文档
         * 比如:当用户拼写错误时,也可以查询得到我们想要的结果
         */
        SearchResponse response = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.fuzzyQuery("content", "GitHub")).get();

        /**获取数据的结果集对象,获取命中次数*/
        final SearchHits hits = response.getHits();
        System.out.println("查询的结果数据有多少条:" + hits.getTotalHits());

        final Iterator<SearchHit> it = hits.iterator();
        while (it.hasNext()) {
            final SearchHit next = it.next();
            //获取整条数据
            System.err.println("所有的数据以json格式输出:" + next.getSourceAsString());

            System.out.println("==========================");

            //获取每个字段的值
            System.err.println("id:" + next.getSource().get("id"));
            System.err.println("content:" + next.getSource().get("content"));
            System.err.println("title:" + next.getSource().get("title"));
        }
        client.close();

    }
}
