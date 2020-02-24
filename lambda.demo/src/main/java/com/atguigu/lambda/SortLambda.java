package com.atguigu.lambda;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class SortLambda {

    List<Employee> emps = Arrays.asList(
            new Employee(104,"王五",18,1264.12),
            new Employee(101,"张三",18,1234.21),
            new Employee(103,"李思",18,1334.34),
            new Employee(10,"诸葛",18,1134.34)
    );
    @Test
    public void CollectSort(){
        Collections.sort(emps,(a,b) ->{
            if(a.getAge() == b.getAge()){
                return a.getName().compareTo(b.getName());
            } else {
                return Integer.compare(a.getAge(),b.getAge());
            }
        });
        for(Employee emp : emps){
            System.out.println(emp);
        }
    }
    //用于处理字符串
    public String strHandler(String str,CustomSort cs){
        return cs.getValue(str);
    }

    @Test
    public void SubString(){
        //String trimStr = strHandler("\t\t\t 尚硅谷威武 ",(a) -> a.trim());
        String strUp = strHandler("abcdj", (str) -> str.toUpperCase());
        String s = strHandler(strUp, (a) -> a.substring(2, 5));
        System.out.println(s);
    }

    @Test
    public void test3(){
        op(100L,200L,(x,y) -> x+y);
        op(100L,200L,(x,y) -> x*y);

        happy(1000,(m) -> System.out.println("爱你"));

        List<Integer> numList = getNumList(10, () -> (int) (Math.random() * 100));
        System.out.println(numList);
    }

    //对于两个long型数据进行运算
    public void op(Long l1 ,Long l2,Funation<Long,Long> mf){
        System.out.println(mf.getValue(l1,l2));
    }

    public  void happy(double money, Consumer<Double> con){
        con.accept(money);
    }

    //产生指定个数的整数，并放入集合中
    public List<Integer> getNumList(int num, Supplier<Integer> sup){
        ArrayList<Integer> list = new ArrayList<>();
        for(int i=0;i<num;i++){
            Integer n = sup.get();
            list.add(n);
        }
        return list;
    }





}
