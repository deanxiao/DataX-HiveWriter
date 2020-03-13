package com.alibaba.datax.plugin.writer.hivewriter;

/**
 * @author dean 2019/10/25.
 * @version v1.1
 */
public class Constants {

    public static final String TEMP_TABLE_NAME_PREFIX_DEFAULT="tmp_datax_hivewriter_";
    public final static String HIVE_CMD_DEFAULT = "hive";
    public final static String HIVE_SQL_SET_DEFAULT = ""; 
    public final static String HIVE_TARGET_TABLE_COMPRESS_SQL= "";
    public static final String WRITE_MODE_DEFAULT="insert";
    public final static String HIVE_PRESQL_DEFAULT = ""; 
    public final static String HIVE_POSTSQL_DEFAULT = ""; 
    public static final String INSERT_PRE_SQL="SET hive.exec.dynamic.partition=true;"
                                             +"SET hive.exec.dynamic.partition.mode=nonstrict;"
    		                                 +"SET hive.exec.max.dynamic.partitions.pernode=100000;"
                                             +"SET hive.exec.max.dynamic.partitions=100000;";
    public final static String FIELDDELIMITER_DEFAULT = "\\u0001";
    public final static String COMPRESS_DEFAULT="gzip";
    
	// 此默认值，暂无使用
	public static final String DEFAULT_NULL_FORMAT = "\\N";
   
}
