package com.rqy.elk.core;

/**
 * @Author renqingyang
 * @create 2020/11/25 5:44 PM
 */
public class Constant {

 public static final int ES_BATCH_SCROLL_SIZE = 1000;

 public static final int ES_BATCH_SCROLL_ID_LIFE = 1000;

 public static final int ES_BATCH_SEARCH_SLEEP_MILLISECONDS = 1000;

 public static final int ES_BATCH_SEARCH_LIMIT = 100;

 public static final int ES_BATCH_OPEN_LIMIT = 100;

 public static final int ES_BATCH_CLOSE_LIMIT = 100;

 public static final String INDEX_METADATA_KEY = "es.index";

 public static final String ES_INDEX_MAP_OPEN_KEY = "es.index.map.open.key";

 public static final String ES_INDEX_MAP_CLOSE_KEY = "es.index.map.close.key";

 public static final String ES_INDEX_MAP_NOT_EXIST_KEY = "es.index.map.not.exist.key";
}
