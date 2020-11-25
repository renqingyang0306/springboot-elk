package com.rqy.elk.controller;

import com.rqy.elk.core.RestResponse;
import com.rqy.elk.domain.Builder;
import com.rqy.elk.esDao.BuilderDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


import java.util.Optional;

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
    BuilderDao builderDao;

    /**
     * 方式1
     *
     * 单个保存索引
     * @return
     */
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public RestResponse save(@RequestBody Builder builder){
        builder = builder == null ? new Builder() : builder;

        RestResponse res = new RestResponse();
        Builder builder2 = builderDao.save(builder);

        res.setResult(builder2);
        return res;
    }
    /**
     * 方式1
     *
     * 根据ID获取单个索引
     * @param id
     * @return
     */
    @RequestMapping(value = "/get", method = RequestMethod.GET)
    public RestResponse get(Long id){
        RestResponse res = new RestResponse();
        Optional<Builder> get = builderDao.findById(id);
        res.setResult(get.isPresent() == false ? null : get.get());
        return res;
    }

}
