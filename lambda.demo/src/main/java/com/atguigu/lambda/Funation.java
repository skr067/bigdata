package com.atguigu.lambda;

@FunctionalInterface
public interface Funation<T ,R> {

    public R getValue(T t1, T t2);

}
