package com.atguigu.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class FruitMRRunner implements Tool {

    public static Configuration conf;
    static {
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.101");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }
    @Override
    public int run(String[] args) throws Exception {
        //得到 Configuration
        Configuration conf = this.getConf();
        //创建 Job 任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(FruitMRRunner.class);
        //配置 Job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);
        //设置 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                "fruit", //数据源的表名
                scan, //scan 扫描控制器
                ReadFruitMapper.class,//设置 Mapper 类
                ImmutableBytesWritable.class,//设置 Mapper 输出 key 类型
                Put.class,//设置 Mapper 输出 value 值类型
                job//设置给哪个 JOB
        );
        //设置 Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitMRReducer.class, job);
        //设置 Reduce 数量，最少 1 个
        job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw new IOException("Job running with error");
        }
        return isSuccess ? 0 : 1;
    }

    @Override
    public Configuration getConf(){
        return this.conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = HBaseConfiguration.create(conf);
    }

    public static void main(String[] args){
        try{
            Configuration conf = HBaseConfiguration.create();
            int status = ToolRunner.run(conf, new FruitMRRunner(), args);
            System.out.println(status);
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
