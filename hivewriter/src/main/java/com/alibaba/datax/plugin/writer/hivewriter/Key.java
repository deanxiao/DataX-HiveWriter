package com.alibaba.datax.plugin.writer.hivewriter;

/**
 * @author dean 2019/10/25.
 * @version v1.1
 */
public class Key {
    /**
     * 1.必选:defaultFS,databaseName,tableName,column,tmpDatabasePath,tmpDatabase
     * 2.可选(有缺省值):
     * 			writeMode(insert)
     *          hive_cmd(hive)
     *          fieldDelimiter(\u0001)
     *          compress(gzip)  
     *          tmpTableName(tmp_datax_hivewriter_)     
     * 3.可选(无缺省值):partition,fullFileName,encoding
     * */
	
	public final static String DEFAULT_FS = "defaultFS";
    public final static String DATABASE_NAME = "databaseName";//目标数据库名
    public final static String TABLE_NAME = "tableName";//目标表名
    public static final String WRITE_MODE = "writeMode";//表的写入方式insert、overwrite
    public static final String COLUMN = "column";//目标表的列
    public static final String NAME = "name";//目标表的字段名
    public static final String TYPE = "type";//目标表的字段类型
    public static final String PARTITION="partition";//分区字段
    public static final String HIVE_DATABASE_TMP_LOCATION="tmpDatabasePath";//临时hive表所在数据库的location路径
    public static final String HIVE_TMP_DATABASE="tmpDatabase";//临时HIVE表所在的数据库
    public static final String TEMP_TABLE_NAME_PREFIX="tmpTableName";//临时HIVE表名前缀
    public static final String TMP_FULL_NAME="fullFileName";//临时hive表  HDFS文件名称
    public static final String HIVE_CMD = "hive_cmd"; //hive
    public final static String HIVE_SQL_SET = "hive_sql_set"; 
    public static final String HIVE_PRESQL = "hive_preSql"; 
    public final static String HIVE_POSTSQL = "hive_postSql"; 
    public final static String HIVE_TARGET_TABLE_COMPRESS_SQL="hive_target_table_compress_sql";
    public static final String COMPRESS = "compress";//临时表压缩格式
    public static final String ENCODING="encoding";
    public static final String FIELD_DELIMITER="fieldDelimiter";
    public static final String NULL_FORMAT = "nullFormat";
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    public static final String HADOOP_CONFIG = "hadoopConfig";


}
