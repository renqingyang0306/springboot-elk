package com.rqy.elk.core;

import com.alibaba.fastjson.JSON;
import org.springframework.http.HttpStatus;

import java.io.Serializable;

/**
 * Created by jiashiran on 2017/9/8.
 */
public class RestResponse implements Serializable {

    /**
     * 返回状态
     */
    private int status;

    /**
     * 返回对象
     */
    private Object result;

    /**
     * 描述信息
     */
    private String msg;
    /**
     * 当前页码，默认第一页
     */
    private int currentPageNo;

    /**
     * 每页数据条数
     */
    private int pageSize;

    /**
     * 数据总条数
     */
    private int totalCount;

    /**
     * 导出文件的url
     */
    private String url;

    private String tips;

    public String getTips() {
        return tips;
    }

    public void setTips(String tips) {
        this.tips = tips;
    }

    public RestResponse() {
        this.status = HttpStatus.OK.value();
    }

    public RestResponse(Object result) {
        this.result = result;
        this.status = HttpStatus.OK.value();
    }

    public RestResponse(Object result, Integer totalCount) {
        this.result = result;
        this.status = HttpStatus.OK.value();
        this.setTotalCount(totalCount);
    }

    public RestResponse(int status, String msg) {
        this.status = status;
        this.msg = msg;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public String getMsg() {
        return msg;
    }

    public int getCurrentPageNo() {
        return currentPageNo;
    }

    public void setCurrentPageNo(int currentPageNo) {
        this.currentPageNo = currentPageNo;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String toJSON(){
        return JSON.toJSONString(this);
    }

    public boolean isEmpty() {
        return result == null && msg == null;
    }

    public boolean isNotEmpty() {
        return !isEmpty();
    }
}
