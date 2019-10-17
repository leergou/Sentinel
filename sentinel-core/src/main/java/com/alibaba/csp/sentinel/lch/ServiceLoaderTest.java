package com.alibaba.csp.sentinel.lch;

import java.util.Date;
import java.util.ServiceLoader;

/**
 * 测试Java自带的ServiceLoader
 */
public class ServiceLoaderTest {

    public static void main(String[] args) {
        ServiceLoader<IBike> bikes = ServiceLoader.load(IBike.class);
        System.out.println("......"+new Date().toLocaleString());
        for(IBike bike:bikes) {
            System.out.println(bike.getClass().getName());
        }
    }
}
