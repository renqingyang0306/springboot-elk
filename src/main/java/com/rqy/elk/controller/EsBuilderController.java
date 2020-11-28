package com.rqy.elk.controller;

import com.alibaba.fastjson.JSONObject;
import com.rqy.elk.esDao.EsService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * @Author renqingyang
 * @create 2020/11/25 1:43 AM
 * 使用方式有两种：
 * 1.一种是经过 SpringData 封装过的，直接在 dao 接口继承 ElasticsearchRepository 即可
 * 2.一种是经过 Spring 封装过的，直接在 Service/Controller 中引入该 bean 即可 ElasticsearchTemplate
 */
@RestController
@RequestMapping("/es")
public class EsBuilderController {

    private static final Logger logger = LoggerFactory.getLogger(EsBuilderController.class);

    private static final String INDEX = "logstash";

    private static final String TYPE = "doc";

    private static final String TIMESTAMP = "createTime";

    @Autowired
    EsService esService;

    /**
     * 方式1
     * <p>
     * 单个保存索引
     *
     * @return
     */
    @RequestMapping(value = "/createIndex", produces = {"application/json;charset=UTF-8;"}, method = RequestMethod.GET)
    public ResponseEntity createIndex(@RequestParam String indexName) {
        try {
            esService.createIndex(indexName);
            logger.info("创建索引成功！索引名称为{}", indexName);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("创建索引失败！索引名称为{}", indexName);
        }
        return new ResponseEntity(indexName, HttpStatus.OK);

    }

    @RequestMapping(value = "/addDoc", produces = {"application/json;charset=UTF-8;"}, method = RequestMethod.GET)
    public ResponseEntity addDocByJson(@RequestParam(value = "message", defaultValue = "") String message,
                                       @RequestParam(value = "level", required = false, defaultValue = "") String level,
                                       @RequestParam(value = "createTime", required = false,  defaultValue = "") String createTime) {
        HashMap<String, String> map = new HashMap<>();
        String time= LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        map.put("message", message);
        map.put("level", level);
        map.put("createTime", time);
        String id = UUID.randomUUID().toString().substring(24);
        try {
            esService.addDocByJson(INDEX, TYPE, id, JSONObject.toJSONString(map));
            logger.info("doc成功！id名称为{}", id);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("doc创建失败!");
        }
        return new ResponseEntity(id, HttpStatus.OK);

    }

    @RequestMapping(value = "/getDoc", produces = {"application/json;charset=UTF-8;"}, method = RequestMethod.GET)
    public ResponseEntity getDoc(@RequestParam(value = "id", required = false,  defaultValue = "") String id) {
        Map<String, Object> document = new HashMap<>();
        try {
            document = esService.getDocument(INDEX, TYPE, id);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("查询doc失败！id={}",id);
        }
        return new ResponseEntity(document, HttpStatus.OK);

    }

}
