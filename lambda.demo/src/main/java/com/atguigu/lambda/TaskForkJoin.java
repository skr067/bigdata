package com.atguigu.lambda;

import org.junit.Test;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

public class TaskForkJoin {

    @Test
    public void test1(){
        ForkJoinPool pool = new ForkJoinPool();
        ForkJoinTask<Long> task = new ForkJoinCalculate(0, 1000000L);
        Long sum = pool.invoke(task);
        System.out.println(sum);

    }
}
