package com.atguigu.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 发布微博
 * 互粉
 * 取关
 * 查看微博
 */
public class WeiBo {//12 36

    public static Configuration conf;
    static {
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.101");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }
    //创建微博这个业务的命名空间，3张表
    private static final byte[] NS_WEIBO = Bytes.toBytes("ns_weibo");
    private static final byte[] TABLE_CONCENT = Bytes.toBytes("ns_weibo:contecnt");
    private static final byte[] TABLE_RELATION = Bytes.toBytes("ns_weibo:relation");
    private static final byte[] TABLE_INBOX = Bytes.toBytes("ns_weibo:inbox");

    private void init() throws Exception{
        //创建微博业务命名空间
        initNamespace();
        //创建微博内容表
        initTableContent();
        //创建用户关系表
        initTableRelation();
        //创建收件箱表
        initTableInbox();
    }

    private void initNamespace() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //创建命名空间描述器
        NamespaceDescriptor ns_weibo = NamespaceDescriptor
                .create("ns_weibo")
                .addConfiguration("creator", "yw")
                .addConfiguration("create_time", String.valueOf(System.currentTimeMillis()))
                .build();
        admin.createNamespace(ns_weibo);
        admin.close();
        connection.close();
    }
    //创建微博内容表
    private void initTableContent() throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_CONCENT));
        //创建列描述器
        HColumnDescriptor infocolumnDescriptor = new HColumnDescriptor("info");
        //设置块缓存
        infocolumnDescriptor.setBlockCacheEnabled(true);
        //设置块缓存大小
        infocolumnDescriptor.setBlocksize(2*1024*1024);
        //设置版本确界
        infocolumnDescriptor.setMinVersions(1);
        infocolumnDescriptor.setMaxVersions(1);
        //将列描述器添加到表描述器中
        tableDescriptor.addFamily(infocolumnDescriptor);
        admin.createTable(tableDescriptor);
        admin.close();
        connection.close();


    }
    private void initTableRelation() throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RELATION));
        //创建列描述器(attends,fans)
        HColumnDescriptor attends = new HColumnDescriptor("attends");
        attends.setBlockCacheEnabled(true);
        attends.setBlocksize(2*1024*1024);
        attends.setMinVersions(1);
        attends.setMaxVersions(1);
        HColumnDescriptor fans = new HColumnDescriptor("fans");
        fans.setBlockCacheEnabled(true);
        fans.setBlocksize(2*1024*1024);
        fans.setMinVersions(1);
        fans.setMaxVersions(1);
        //将列描述器添加到表描述器中
        tableDescriptor.addFamily(attends);
        tableDescriptor.addFamily(fans);
        admin.createTable(tableDescriptor);

        admin.close();
        connection.close();

    }
    //创建收件箱表
    private void initTableInbox() throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_INBOX));
        HColumnDescriptor info = new HColumnDescriptor("info");
        info.setBlockCacheEnabled(true);
        info.setBlocksize(2*1024*1024);
        info.setMinVersions(100);
        info.setMaxVersions(100);

        hTableDescriptor.addFamily(info);
        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }

    /**
     * 发布微博
     * 1.向微博内容表中添加刚发布的内容，多一个rowkey
     * 2.向发布微博人的粉丝的收件箱中，添加微博rowkey
     * @param uid
     * @param content
     * @throws IOException
     */
    public void publishContent(String uid,String content) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        //得到微博表对象
        Table table = connection.getTable(TableName.valueOf(TABLE_CONCENT));

        //组装rowkey
        long ts = System.currentTimeMillis();
        String rowkey = uid +"_"+ ts;
        //添加微博内容到微博表
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("content"),Bytes.toBytes(content));
        table.put(put);

        //查询用户关系表，得到当前用户的fans用户id
        Table table1 = connection.getTable(TableName.valueOf(TABLE_RELATION));
        //获取粉丝的用户id
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        //取出所有粉丝的用户id,存放于一个集合中
        List<byte[]> fans = new ArrayList<>();
        Result result = table1.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells){
            //取出当前用户所有粉丝uid
            fans.add(CellUtil.cloneValue(cell));
        }
        //如果没有粉丝，则不需要操作粉丝的收件箱表
        if(fans.size()<=0) return;


        //开始操作收件箱
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));

        //封装用于操作粉丝收件箱表的Put对象集合
        List<Put> puts = new ArrayList<>();
        for (byte[] fansRowKey : fans){
            Put put1 = new Put(fansRowKey);
            put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes(uid),ts,Bytes.toBytes(rowkey));
            puts.add(put1);
        }
        //向收件箱表放置数据
        inboxTable.put(puts);

        inboxTable.close();
        table.close();
        table1.close();
        connection.close();
    }

    /**添加关注
     * a.在用户关系表中，对当前主动操作的用户id进行添加关注的操作
     * b.在用户关系表中，对被关注的人的用户id,添加粉丝操作
     * c.对当前操作的用户的收件箱表中，添加他所关注人的最近的微博rowkey
     */
    public void addAttends(String uid,String... attends) throws Exception{
        //参数过滤，如果没有传递关注的人的uid，则直接返回
        if(attends == null || attends.length <= 0 || uid == null) return;

        //a,b
        Connection connection = ConnectionFactory.createConnection(conf);
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        List<Put> puts = new ArrayList<>();
        //在微博用户关系表中，添加新关注的好友
        Put attendPut = new Put(Bytes.toBytes(uid));
        for(String attend: attends){
            //为当前用户添加关注人
            attendPut.addColumn(Bytes.toBytes("attends"),Bytes.toBytes("attend"),Bytes.toBytes(attend));
            //被关注的人添加粉丝uid
            Put fansPut = new Put(Bytes.toBytes(attend));
            fansPut.addColumn(Bytes.toBytes("fans"),Bytes.toBytes("uid"),Bytes.toBytes(uid));
            puts.add(fansPut);
        }
        puts.add(attendPut);
        relationTable.put(puts);

        //c  取得微博内容表
        Table concentTable = connection.getTable(TableName.valueOf(TABLE_CONCENT));
        Scan scan = new Scan();
        //用于存放扫描出来的我所关注的人的微博rowkey
        List<byte[]> rowkeys = new ArrayList<>();
        for(String attend : attends){
            //扫描微博rowkey，使用rowfilter过滤器
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
            scan.setFilter(rowFilter);
            //通过该scan扫描结果
            ResultScanner resultScanner = concentTable.getScanner(scan);
            Iterator<Result> resultIterator = resultScanner.iterator();
            while (resultIterator.hasNext()){
                Result result = resultIterator.next();
                rowkeys.add(result.getRow());
            }
        }
        //将取出微博的rowkey放置于当前操作用户的收件箱
        //如果所关注的人，没有一条微博则直接返回
        if(rowkeys.size()<=0) return;

        //操作inboxTable
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        Put inboxPut = new Put(Bytes.toBytes(uid));
        for(byte[] rowkey : rowkeys){
            String s = Bytes.toString(rowkey);
            String attendUID = s.split("_")[0];
            String attendTs = s.split("_")[1];
            inboxPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(attendUID),Long.valueOf(attendTs),rowkey);
        }
        inboxTable.put(inboxPut);

        inboxTable.close();
        concentTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 取关操作
     * a.在用户关系表中，删除你要取关的那个人的用户id
     * b.在用户关系表中，删除被你取关的那个人的粉丝中的当前操作用户id
     * c.删除微博收件箱表中你取关的人所发布的微博的rowkey
     *
     */
    public void removeAttends(String uid,String... attends) throws IOException{
        //过滤数据
        if(uid == null || uid.length()<=0 || attends==null || attends.length<=0) return ;

        Connection connection = ConnectionFactory.createConnection(conf);
        //a.在用户关系表中，删除已关注好友
        Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
        //待删除的用户关系表中所有数据
        List<Delete> deletes = new ArrayList<>();
        //当前取关用户
        Delete attendDelete = new Delete(Bytes.toBytes(uid));

        for(String attend : attends){
            attendDelete.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(attend));
            //b
            Delete delete = new Delete(Bytes.toBytes(attend));
            delete.addColumn(Bytes.toBytes("fans"),Bytes.toBytes("uid"));
            deletes.add(delete);
        }
        deletes.add(attendDelete);
        relationTable.delete(deletes);

        //c
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        for(String attend : attends){
            inboxDelete.addColumns(Bytes.toBytes("info"),Bytes.toBytes(attend));
        }
        inboxTable.delete(inboxDelete);
        //释放资源
        inboxTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 查看微博内容
     * a.从微博收件箱中获取所有关注的人发布微博的rowkey
     * b.根据得到的微博rowkey，去微博哦内容中得到数据
     * c.将取出的数据解码然后封装到Message对象中
     */
    public List<Message> getAttendsContent(String uid) throws IOException{
        //a
        Connection connection = ConnectionFactory.createConnection(conf);
        Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.addFamily(Bytes.toBytes("info"));
        inboxGet.setMaxVersions(5);
        Result inboxResult = inboxTable.get(inboxGet);
        //准备一个存放所有微博rowkey的集合
        List<byte[]> rowkeys = new ArrayList<>();
        Cell[] cells = inboxResult.rawCells();
        for (Cell cell : cells){
            rowkeys.add(CellUtil.cloneValue(cell));
        }
        //b
        Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONCENT));
        //批量获取微博所有数据
        List<Get> contentGets = new ArrayList<>();
        for (byte[] rowkey : rowkeys){
            Get contentGet = new Get(rowkey);
            contentGets.add(contentGet);
        }
        //所有的结果数据
        List<Message> messages = new ArrayList<>();
        Result[] contentResults = contentTable.get(contentGets);
        for (Result r : contentResults){
            Cell[] cs = r.rawCells();
            for (Cell c : cs){
                String rk = Bytes.toString(r.getRow());
                String publishUID = rk.split("_")[0];
                Long publishTS = Long.valueOf(rk.split("_")[1]);
                Message message = new Message();
                message.setUid(publishUID);
                message.setTimestamp(publishTS);
                message.setContent(Bytes.toString(CellUtil.cloneValue(c)));
                messages.add(message);
            }
        }
        contentTable.close();
        inboxTable.close();
        connection.close();

        return messages;
    }









}
