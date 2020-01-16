package com.atguigu.hbase_syllabus;

import com.sun.javadoc.SourcePosition;
import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBaseDemo {

    public static Configuration conf;
    static {
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.101");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    //判断表是否存在
    public static boolean isExist(String tableName) throws Exception{
        //在HBase中管理，访问表需要先创建HBaseAdmin对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    //创建表
    public static void createTable(String tableName,String... columnFamily) throws Exception{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if(isExist(tableName)){
            System.out.println("表已存在");
        } else{
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
            //创建列族
            for (String cf : columnFamily){
                htd.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(htd);
            System.out.println("表创建成功");
        }
    }

    //删除表
    public static void dropTable(String tableName) throws Exception{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if(isExist(tableName)){
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("删除成功！");
        } else{
            System.out.println("不存在！");
        }
    }

    //向表中插入数据
    public static void addRowData(String tableName,String rowKey,String columnFamily,
                                  String column,String value) throws Exception{
        //创建HTable对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向put对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("插入数据成功");
    }

    //扫描数据
    public static void getAllRows(String tableName) throws Exception{
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region对象
        Scan scan = new Scan();
        //使用table得到resultcanner实现类的对象
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到rowkey
                System.out.println("行键："+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    //得到某一行所有数据
    public static void getRow(String tableName,String rowKey) throws Exception{
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //获取某一行指定"列族：列"的数据
        //get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        System.out.println(get.setMaxVersions());
        Result result = table.get(get);
        for(Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    //删除多行数据
    public static void deleteRow(String tableName,String... rows)throws Exception{
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deletes = new ArrayList<>();
        for (String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deletes.add(delete);
        }
        table.delete(deletes);
        System.out.println("删除成功");
        table.close();
    }


    //主函数中进行测试
    public static void main(String[] args) throws Exception
    {
        //System.out.println(isExist("student"));
        createTable("stuff","info1","info2");
        //dropTable("stuff");
        addRowData("stuff","1001","info1","name","张三");
        //deleteRow("stuff","1001");
        getAllRows("stuff");
        getRow("stuff","1001");
    }
}
