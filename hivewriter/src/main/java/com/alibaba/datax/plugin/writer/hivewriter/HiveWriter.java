package com.alibaba.datax.plugin.writer.hivewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.KeyUtil;
import com.alibaba.datax.common.util.ShellUtil;
import com.alibaba.datax.plugin.writer.hivewriter.Constants;
import com.alibaba.datax.plugin.writer.hivewriter.HiveWriterErrorCode;
import com.alibaba.datax.plugin.writer.hivewriter.Key;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @author dean 2019/10/25.
 * @version v1.1
 */
public class HiveWriter extends Writer{


    public static class Job extends Writer.Job {

        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;
        private String defaultFS;
        private String tmpPath;
        private String tmpTableName;
        private String tempHdfsLocation;
        @Override
        public void init() {
            this.conf = super.getPluginJobConf();//获取配置文件信息{parameter 里面的参数}
            log.info("hive writer params:{}", conf.toJSON());
            //校验 参数配置
            log.info("HiveWriter流程说明[1:创建hive临时表 ;2:Reader的数据导入到临时表HDFS路径(无分区);3:临时表数据插入到目标表;4:删除临时表]");
            this.validateParameter();
        }

        private void validateParameter() {
            this.conf.getNecessaryValue(Key.DATABASE_NAME,HiveWriterErrorCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(Key.TABLE_NAME,HiveWriterErrorCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(Key.DEFAULT_FS,HiveWriterErrorCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(Key.HIVE_DATABASE_TMP_LOCATION,HiveWriterErrorCode.REQUIRED_VALUE);
            //Kerberos check
            Boolean haveKerberos = this.conf.getBool(Key.HAVE_KERBEROS, false);
            if(haveKerberos) {
                this.conf.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HiveWriterErrorCode.REQUIRED_VALUE);
                this.conf.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HiveWriterErrorCode.REQUIRED_VALUE);
            }

        }

        @Override
        public void prepare() {
        	
        	  this.tempHdfsLocation=this.conf.getString(Key.HIVE_DATABASE_TMP_LOCATION);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            this.defaultFS = this.conf.getString(Key.DEFAULT_FS);
            //按照reader 切分的情况来组织相同个数的writer配置文件  (reader channel writer)
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration splitedTaskConfig = this.conf.clone();
                this.tmpTableName=hiveTableName();
              //判断set语句的结尾是否是/，不是给加一个
                if (!this.tempHdfsLocation.trim().endsWith("/")) {
                	this.tempHdfsLocation=this.tempHdfsLocation + "/";}
              //创建临时Hive表,指定hive表在hdfs上的存储路径
                this.tmpPath=this.tempHdfsLocation+this.tmpTableName.toLowerCase();
                //后面需要指定写入的文件名称
                String fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                String fullFileName=String.format("%s%s/%s__%s", defaultFS, this.tmpPath,this.tmpTableName,fileSuffix);// 临时存储的文件路径

                splitedTaskConfig.set(Key.HIVE_DATABASE_TMP_LOCATION,tmpPath);
                splitedTaskConfig.set(Key.TMP_FULL_NAME,fullFileName);
                splitedTaskConfig.set(Key.TEMP_TABLE_NAME_PREFIX,this.tmpTableName);
                //分区字段解析 "dt","type"
                List<String> partitions = this.conf.getList(Key.PARTITION, String.class);
                String partitionInfo=StringUtils.join(partitions,",");
                splitedTaskConfig.set(Key.PARTITION,partitionInfo);

                configurations.add(splitedTaskConfig);
            }
            return configurations;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        private String hiveTableName() {
            StringBuilder str = new StringBuilder();
            FastDateFormat fdf = FastDateFormat.getInstance("yyyyMMdd");
            str.append(Constants.TEMP_TABLE_NAME_PREFIX_DEFAULT).append(fdf.format(new Date()))
                    .append("_").append(KeyUtil.genUniqueKey());
            return str.toString();
        }

    }


    public static class Task extends Writer.Task {

        //写入hive步骤 (1)创建临时表  (2)读取数据写入临时表  (3) 从临时表写出数据

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration conf;
        private String defaultFS;
        private String databaseName;
        private String tableName;//目标表名称
        private String writeMode;
        private String partition;
        
        private String tmpDataBase;
        private String tmpTableName;
        private boolean alreadyDel=false;
        private String hive_cmd;
        private String hive_sql_set;
        private HdfsHelper hdfsHelper = null;//工具类
        private String fieldDelimiter;
        private String hive_fieldDelimiter;
        private String compress;
        private String hive_target_table_compress_sql;
        private String hive_preSql;
        private String hive_postSql;
        @Override
        public void init() {

            this.conf = super.getPluginJobConf();
            //初始化每个task参数
            this.defaultFS = this.conf.getString(Key.DEFAULT_FS);
            this.databaseName=this.conf.getString(Key.DATABASE_NAME);
            this.tableName=this.conf.getString(Key.TABLE_NAME);
            this.partition=this.conf.getString(Key.PARTITION);
            this.writeMode=this.conf.getString(Key.WRITE_MODE,Constants.WRITE_MODE_DEFAULT);
            this.tmpDataBase=this.conf.getString(Key.HIVE_TMP_DATABASE,this.databaseName);
            this.tmpTableName =this.conf.getString(Key.TEMP_TABLE_NAME_PREFIX);
            this.hive_cmd=this.conf.getString(Key.HIVE_CMD, Constants.HIVE_CMD_DEFAULT);
            this.hive_sql_set = this.conf.getString(Key.HIVE_SQL_SET,Constants.HIVE_SQL_SET_DEFAULT);
            this.fieldDelimiter=this.conf.getString(Key.FIELD_DELIMITER,Constants.FIELDDELIMITER_DEFAULT);
            this.compress=this.conf.getString(Key.COMPRESS,Constants.COMPRESS_DEFAULT);
            this.hive_preSql=this.conf.getString(Key.HIVE_PRESQL,Constants.HIVE_PRESQL_DEFAULT);
            this.hive_postSql=this.conf.getString(Key.HIVE_POSTSQL,Constants.HIVE_POSTSQL_DEFAULT);
            this.hive_fieldDelimiter=this.fieldDelimiter;
            this.fieldDelimiter = StringEscapeUtils.unescapeJava(this.fieldDelimiter);  
            this.conf.set(Key.FIELD_DELIMITER, this.fieldDelimiter);//设置hive 存储文件 hdfs默认的分隔符,传输时候会分隔
            this.conf.set(Key.COMPRESS, this.compress);
            this.hive_target_table_compress_sql=this.conf.getString(Key.HIVE_TARGET_TABLE_COMPRESS_SQL,Constants.HIVE_TARGET_TABLE_COMPRESS_SQL);
            //判断set语句的结尾是否是分号，不是给加一个
            if (!this.hive_sql_set.trim().endsWith(";")) {
            	this.hive_sql_set=this.hive_sql_set + ";";}
            if (!this.hive_preSql.trim().endsWith(";")) {
            	this.hive_preSql=this.hive_preSql + ";";}
            if (!this.hive_postSql.trim().endsWith(";")) {
            	this.hive_postSql=this.hive_postSql + ";";}
            hdfsHelper = new HdfsHelper();
            hdfsHelper.getFileSystem(defaultFS, conf);
            
            

        }


        @Override
        public void prepare() {
            //创建临时表
        	
            List<Configuration>  columns = this.conf.getListConfiguration(Key.COLUMN);
            String columnsInfo=hdfsHelper.getColumnInfo(columns);
            
            String hive_presql_str="";
            if(this.hive_preSql.equals("select 1;")) {
            	hive_presql_str="";
            }else if(StringUtils.isNotBlank(this.hive_preSql)) {
            	String hivepresql_Info=this.hive_preSql;
            	hive_presql_str=hivepresql_Info;
            }
            
            String hiveCmd =this.hive_sql_set+hive_presql_str+" use "+this.tmpDataBase+"; " +
                            "create table " + this.tmpTableName + "("+columnsInfo+") " +
                            " ROW FORMAT DELIMITED FIELDS TERMINATED BY '"+this.hive_fieldDelimiter+"' stored as TEXTFILE ";
            
            LOG.info("创建hive临时表 ----> :" + hiveCmd);
            //执行脚本,创建临时表
            if (!ShellUtil.exec(new String[]{this.hive_cmd, "-e", "\"" + hiveCmd + "\""})) {
                throw DataXException.asDataXException(
                        HiveWriterErrorCode.SHELL_ERROR,
                        "创建hive临时表脚本执行失败");
            }
            addHook();
            LOG.info("创建hive 临时表结束 end!!!");

        }


        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            //(2)读取数据写入临时表,默认创建的临时表是textfile格式
            LOG.info("begin do write...");
            String fullFileName=this.conf.getString(Key.TMP_FULL_NAME);// 临时存储的文件路径
            LOG.info(String.format("write to file : [%s]", fullFileName));
            //写TEXT FILE
            hdfsHelper.textFileStartWrite(lineReceiver, this.conf, fullFileName, this.getTaskPluginCollector());
            LOG.info("end do write tmp text table");
            
            String writeModeSql=null;
            if(this.writeMode.equals("overwrite")) {
            	writeModeSql="overwrite";
            }else {
            	writeModeSql="into";
            }
            
            String partition_str="";
           
            if( StringUtils.isNotBlank(this.partition)) {
            	//获取分区字段
            	String partitionInfo=this.partition;
            	partition_str=" partition("+partitionInfo+") ";
            }
            //从临时表写入到目标表
            String insertCmd=this.hive_sql_set+" use "+this.databaseName+";" +
            		          Constants.INSERT_PRE_SQL+this.hive_target_table_compress_sql+
                             " insert "+writeModeSql+ " table "+this.tableName+partition_str +
                             " select * from "+this.tmpDataBase+"."+this.tmpTableName+" DISTRIBUTE BY rand();";
            LOG.info("insertCmd ----> :" + insertCmd);

            //执行脚本,创建临时表
            if (!ShellUtil.exec(new String[]{this.hive_cmd, "-e", "\"" + insertCmd + "\""})) {
                throw DataXException.asDataXException(
                        HiveWriterErrorCode.SHELL_ERROR,
                        "导入数据到目标hive表失败");
            }

            LOG.info("end do write");
        }

        @Override
        public void post() {
            LOG.info("one task hive write post...end");
            deleteTmpTable();
        }

        @Override
        public void destroy() {

        }


        private void addHook(){
            if(!alreadyDel){
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        deleteTmpTable();
                    }
                }));
            }
        }


        private void deleteTmpTable() {
            
            String hive_postsql_str="";
            if(this.hive_postSql.equals("select 1;")) {
            	hive_postsql_str="";
            }else if( StringUtils.isNotBlank(this.hive_postSql)) {
            	//获取分区字段
            	String hivepostsql_Info=this.hive_postSql;
            	hive_postsql_str=hivepostsql_Info;
            }
            
            String hiveCmd =this.hive_sql_set+" use "+this.tmpDataBase+";" +
                             "drop table " + tmpTableName +";" +hive_postsql_str;//注意要删除的是临时表
            LOG.info("hiveCmd ----> :" + hiveCmd);
            //执行脚本,创建临时表
            if (!ShellUtil.exec(new String[]{this.hive_cmd, "-e", "\"" + hiveCmd + "\""})) {
                throw DataXException.asDataXException(
                        HiveWriterErrorCode.SHELL_ERROR,
                        "删除hive临时表脚本执行失败");
            }
            alreadyDel=true;
        }

    }

}
