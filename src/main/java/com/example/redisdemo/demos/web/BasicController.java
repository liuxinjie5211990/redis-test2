/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.redisdemo.demos.web;

import com.example.redisdemo.config.RedisCache;
import com.example.redisdemo.config.RedisConfig;
import com.example.redisdemo.service.XmlService;
import org.apache.lucene.util.RamUsageEstimator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author <a href="mailto:chenxilzx1@gmail.com">theonefx</a>
 */
@Controller
public class BasicController {

    /*当前时间: 2024-05-11 23:08:07
map size 100, value is 3200512
map size 100, value is 3.1 MB
结束时间: 2024-05-11 23:09:09
当前时间: 2024-05-11 23:09:34
结束时间: 2024-05-11 23:10:22
当前时间: 2024-05-11 23:17:25
结束时间: 2024-05-11 23:18:14*/
    @Autowired
    RedisCache redisCache;

    @Autowired
    XmlService xmlService;

    private static String formatSize(long size) {
        if (size < 1024) {
            return size + " bytes";
        } else if (size < 1024 * 1024) {
            return size / 1024 + " KB";
        } else {
            return size / (1024 * 1024) + " MB";
        }
    }

    // http://127.0.0.1:8080/hello?name=lisi
    @RequestMapping("/batch1")
    @ResponseBody
    public String batch1(@RequestParam(name = "name", defaultValue = "unknown user") String name) throws IOException {
        // 获取当前时间
        LocalDateTime now = LocalDateTime.now();

        String content = xmlService.loadXmlContent("demo.xml");

        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 格式化时间并打印
        String formattedDateTime = now.format(formatter);
        System.out.println("当前时间: " + formattedDateTime);
   /*     for (int i=1;i<=10000;i++) {
            redisCache.setCacheObject("cao"+i,content);
        }*/


        Map map = new HashMap<>();
        for (int i=1;i<=30000;i++) {
            map.put("cao"+i,content);
//            redisCache.setCacheList("cao"+i,content);
        }
        // 计算对象的大小（包括对象头）
        System.out.println("map size 100, value is " + RamUsageEstimator.sizeOf(map));

        System.out.println("map size 100, value is " + RamUsageEstimator.humanSizeOf(map));
        // 将Map的entry映射为Stream
        Stream<Map.Entry<String, String>> entryStream = map.entrySet().stream();

        // 将Map转换为列表
        List<Map.Entry<String, String>> entries = new ArrayList<>(map.entrySet());

        entries.stream()
                .collect(Collectors.groupingBy(
                        entry -> entries.indexOf(entry) / 1000,
                        Collectors.toList()))
                .values().parallelStream()
                .forEach(subList -> {
                    // 对每个子列表执行someMethod方法
                    redisCache.batchInsert(subList.stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                });
        now = LocalDateTime.now();
        System.out.println("结束时间: " + now.format(formatter));
//// 遍历每个batch,构建子Map并执行某方法
//        batchedEntries.forEach(batch -> {
//            batch.forEach(entry -> redisCache.batchInsert(entry));
//
//        });


       /* map.clear();

        for (int i=1;i<=3000;i++) {
            map.put("diu"+i,content);
//            redisCache.setCacheList("cao"+i,content);
        }
        redisCache.batchInsert(map);

        map.clear();

        for (int i=1;i<=3000;i++) {
            map.put("ya"+i,content);
//            redisCache.setCacheList("cao"+i,content);
        }
        redisCache.batchInsert(map);

        map.clear();

        for (int i=1;i<=3000;i++) {
            map.put("fa"+i,content);
//            redisCache.setCacheList("cao"+i,content);
        }
        redisCache.batchInsert(map);

        map.clear();

        for (int i=1;i<=3000;i++) {
            map.put("fa"+i,content);
//            redisCache.setCacheList("cao"+i,content);
        }
        redisCache.batchInsert(map);

        map.clear();

        for (int i=1;i<=3000;i++) {
            map.put("fa"+i,content);
//            redisCache.setCacheList("cao"+i,content);
        }
        redisCache.batchInsert(map);

        now = LocalDateTime.now();
        System.out.println("结束时间: " + now.format(formatter));*/

        return "Hello " + name;
    }

    @RequestMapping("/batch2")
    @ResponseBody
    public String batch2(@RequestParam(name = "name", defaultValue = "unknown user") String name) throws IOException {
        // 获取当前时间
        LocalDateTime now = LocalDateTime.now();

        String content = xmlService.loadXmlContent("demo.xml");

        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 格式化时间并打印
        String formattedDateTime = now.format(formatter);
        System.out.println("当前时间: " + formattedDateTime);
        /*for (int i=1;i<=10000;i++) {
            redisCache.setCacheObject("cao"+i,content);
        }

        now = LocalDateTime.now();*/
        for (int i=1;i<=30000;i++) {
            redisCache.setCacheObject("cao"+i,compressString(content));
        }
        now = LocalDateTime.now();
        System.out.println("结束时间: " + now.format(formatter));

        return "Hello " + name;
    }

    @RequestMapping("/batch3")
    @ResponseBody
    public String batch3(@RequestParam(name = "name", defaultValue = "unknown user") String name) throws IOException {
        // 获取当前时间
        LocalDateTime now = LocalDateTime.now();

        String content = xmlService.loadXmlContent("demo.xml");

        // 定义日期时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 格式化时间并打印
        String formattedDateTime = now.format(formatter);
        System.out.println("当前时间: " + formattedDateTime);
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        int chunk = 2000;
        for (int i=1;i<=30000;i++) {
            keys.add("cao"+i);
            values.add(content);
            if (i % chunk == 0) {
                redisCache.batchInsert(keys,values);
                keys.clear();
                values.clear();
            }
        }

        if (keys.size() > 0) {
            redisCache.batchInsert(keys,values);
        }

        now = LocalDateTime.now();
        System.out.println("结束时间: " + now.format(formatter));

        return "Hello " + name;
    }

    // http://127.0.0.1:8080/user
    @RequestMapping("/user")
    @ResponseBody
    public User user() throws IOException {
        User user = new User();
        user.setName("theonefx");
        user.setAge(666);

        String content = xmlService.loadXmlContent("demo.xml");
        // 计算对象的大小（包括对象头）
        System.out.println("content size 100, value is " + RamUsageEstimator.sizeOf(content));

        System.out.println("content size 100, value is " + RamUsageEstimator.humanSizeOf(content));

        String aaa = compressString(content);

        // 计算对象的大小（包括对象头）
        System.out.println("content size 100, value is " + RamUsageEstimator.sizeOf(aaa));

        System.out.println("content size 100, value is " + RamUsageEstimator.humanSizeOf(aaa));

        String ccc = decompressString(aaa);

        // 计算对象的大小（包括对象头）
        System.out.println("content size 100, value is " + RamUsageEstimator.sizeOf(ccc));

        System.out.println("content size 100, value is " + RamUsageEstimator.humanSizeOf(ccc));

        String bbb = redisCache.getCacheObject("cao1") == null ? "":redisCache.getCacheObject("cao1");

        System.out.println(bbb);

        String fff = decompressString(bbb);

        System.out.println(fff);

        return user;
    }

    // http://127.0.0.1:8080/save_user?name=newName&age=11
    @RequestMapping("/save_user")
    @ResponseBody
    public String saveUser(User u) {
        return "user will save: name=" + u.getName() + ", age=" + u.getAge();
    }

    // http://127.0.0.1:8080/html
    @RequestMapping("/html")
    public String html() {
        return "index.html";
    }

    @ModelAttribute
    public void parseUser(@RequestParam(name = "name", defaultValue = "unknown user") String name
            , @RequestParam(name = "age", defaultValue = "12") Integer age, User user) {
        user.setName("zhangsan");
        user.setAge(18);
    }

    // 压缩字符串
    public static String compressString(String str) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
                gzipOut.write(str.getBytes("UTF-8"));
            }
            return baos.toString("ISO-8859-1");
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // 解压缩字符串
    public static String decompressString(String compressedStr) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(compressedStr.getBytes("ISO-8859-1"));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPInputStream gzipIn = new GZIPInputStream(bais)) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = gzipIn.read(buffer)) > 0) {
                    baos.write(buffer, 0, len);
                }
            }
            return baos.toString("UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
