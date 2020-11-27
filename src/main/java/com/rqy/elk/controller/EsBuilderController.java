package com.rqy.elk.controller;

import com.rqy.elk.esDao.EsService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;



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

    @Autowired
    EsService esService;

    /**
     * 方式1
     *
     * 单个保存索引
     * @return
     */
    @RequestMapping(value = "/save", produces = {"application/json;charset=UTF-8;"}, method = RequestMethod.POST)
    public ResponseEntity save(@RequestParam String data){

        return new ResponseEntity(null, HttpStatus.OK);

    }

}
