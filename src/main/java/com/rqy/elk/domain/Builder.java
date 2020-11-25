package com.rqy.elk.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Mapping;
import org.springframework.data.elasticsearch.annotations.Setting;

import java.util.Date;

/**
 * @Author renqingyang
 * @create 2020/11/25 1:13 AM
 */
/**
 * es的index的settings 和 mapping 设置，是最初的第一次设置。后续即使更改，也不起作用。
 * 但是mapping中的属性名称以及属性个数如果更改了，会更新到ES中。这样会导致数据的丢失。需要注意。
 */


//ES的三个注解
//指定index索引名称为项目名   指定type类型名称为实体名
@Document(indexName = "elk")
//相当于ES中的mapping    注意对比文件中的json和原生json  最外层的key是没有的
@Mapping(mappingPath = "/esConfig/builder-mapping.json")
//相当于ES中的settings   注意对比文件中的json和原生json  最外层的key是没有的
@Setting(settingPath = "/esConfig/builder-setting.json")
public class Builder {

    //id  测试长整型数据   注意与es中索引本身id区分开
    @Id
    private Long id;


    //在创建初始化索引开始   就要去查看mapping是否ik分词创建成功   否则 需要进行索引数据的迁移操作


    //指定查询分词器 为ik分词器     存储分词器为 ik分词器
    //在@Field中指定的ik分词器没起作用，因此采用上面的两个注解 可以完全自定义类型Field的各个属性
    //@Field(searchAnalyzer = "ik_max_word",analyzer = "ik_max_word")


    //类型定义为text 可测试ik分词  繁简体转化   pinyin分词 查询效果
    //名称     测试字符串类型
    private String buildName;

    //类型定义为text  可测试大文本
    private String remark;

    //类型定义为keyword 可测试是否分词 以及查询效果
    private String email;

    //数量  测试整型数据
    private int buildNum;

    //时间也可以进行范围查询，但是查询传入参数，应该为mapping中定义的时间字段的 格式化字符串  或 时间戳 否则，ES无法解析格式会报错
    //时间    测试时间类型
    private Date buildDate;

    //积分比率  测试浮点型数据
    private Double integral;


    //分页大小
    private Integer pageNum = 0;
    //分页数量
    private Integer pageSize = 10;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBuildName() {
        return buildName;
    }

    public void setBuildName(String buildName) {
        this.buildName = buildName;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public int getBuildNum() {
        return buildNum;
    }

    public void setBuildNum(int buildNum) {
        this.buildNum = buildNum;
    }

    public Date getBuildDate() {
        return buildDate;
    }

    public void setBuildDate(Date buildDate) {
        this.buildDate = buildDate;
    }

    public Double getIntegral() {
        return integral;
    }

    public void setIntegral(Double integral) {
        this.integral = integral;
    }

    public Integer getPageNum() {
        return pageNum;
    }

    public void setPageNum(Integer pageNum) {
        this.pageNum = pageNum;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }
}
