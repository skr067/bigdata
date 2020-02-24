package com.atguigu.lambda;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * 1.创建Stream
 *
 * 2.中间操作
 *
 * 3.终止操作
 */

public class StreamApi {

    @Test
    public void test1(){
        //1.通过Collection系列集合提供的stream()
        ArrayList<Object> list = new ArrayList<>();
        Stream<Object> stream1 = list.stream();

        //2.通过Arrays的静态方法stream()获取数组流
        Employee[] emps = new Employee[10];
        Stream<Employee> stream2 = Arrays.stream(emps);

        //3.通过Stream类的静态方法of()
        Stream<String> stream3 = Stream.of("aa", "bb", "cc");

        //4.创建无限流
        //迭代
        Stream<Integer> stream4 = Stream.iterate(0, x -> x + 2);
                stream4.limit(10).forEach(System.out::println);
        //生成
        Stream.generate(() -> Math.random()).limit(6).forEach(System.out::println);
    }

    /**
     * 给定一个数字列表，如何返回一个由每个数的平方构成的列表
     */
    @Test
    public void test2(){
        Integer[] nums = {1, 2, 3, 4, 5};
        Stream<Integer> stream = Arrays.stream(nums);
        stream.map(x -> x*x)
                .forEach(System.out::println);

    }
    List<Employee> emps = Arrays.asList(
            new Employee(104,"王五",18,1264.12),
            new Employee(101,"张三",18,1234.21),
            new Employee(103,"李思",18,1334.34),
            new Employee(10,"诸葛",18,1134.34)
    );
    /**
     * 怎样用map和reduce方法数一数流中有多少个employee
     */
    @Test
    public void test3(){
        Optional<Integer> count = emps.stream().map(e -> 1)
                .reduce(Integer::sum);
        System.out.println(count.get());

        emps.stream().filter(e -> e.getAge() == 18)
                .sorted((t1, t2) -> Integer.compare(t1.getId(), t2.getId()))
                .forEach(System.out::println);
        String reduce = emps.stream().map(e -> e.getName())
                .sorted().reduce("", String::concat);
        System.out.println(reduce);

    }

}
