package com.example.redisdemo.service;

import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Service
public class XmlService {

    private final ResourceLoader resourceLoader;

    public XmlService(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    public String loadXmlContent(String filename) throws IOException {
        // 加载资源文件
        Resource resource = resourceLoader.getResource("classpath:" + filename);

        // 检查资源是否存在
        if (resource.exists()) {
            // 使用StreamUtils将文件内容读取为字符串
            return StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
        } else {
            throw new IOException("File not found: " + filename);
        }
    }
}
