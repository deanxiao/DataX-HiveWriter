# DataX HiveWriter

---

## 1 快速介绍

实现写数据到Hive表或分区



## 2 实现原理

- 主要实现步骤
  - 将源数据到一个临时HDFS目录
  - 设计一个临时表映射表1）中的HDFS目录
  - 通过SQL语句方式，将临时表的数据，写入到目标Hive表
  - 删除临时表与临时HDFS目录



## 3 功能说明

Hivewriter插件：写数据到Hive目标表

- 支持insert,overwrite写数模式
- 支持写数据到Hive表(无分区)或分区表；支持多个分区列的表的写入，支持动态分区数据写入(多份分区数据)
- 支持任意存储格式的Hive目标表
- 支持指定压缩格式的Hive目标表写入
- Reader的字段顺序和数量需跟Hive目标表中完全一致，且包括分区列(此处与HdfsWriter不同)



### 3.1 配置样例

#### job.json

```
{
  "job": {
    "setting": {
      "speed": {
        "channel":1
      }
    },
    "content": [
      {
        "reader": {
          ...
        },
        "writer": {
          "name": "hivewriter",
          "parameter": {
            "databaseName":"default",
            "tableName": "test_topic",
            "defaultFS": "hdfs://xxx:port",
            "writeMode": "insert",
            "tmpDatabase":"tmp",
            "tmpDatabasePath":"/user/hive/warehouse/tmp.db/",
            "partition": ["dt"],
            "column": [
                          {
                            "name": "id",
                            "type": "INT"
                          },
                          {
                            "name": "username",
                            "type": "STRING"
                          },
                          {
                            "name": "telephone",
                            "type": "STRING"
                          },
                          {
                            "name": "mail",
                            "type": "STRING"
                          },
                          {
                             "name": "dt",
                             "type": "STRING"
                          }
                        ]
          }
        }
        }
    ]
  }

}
```

#### 3.2 参数说明
* **databaseName**
  * 描述：目标hive表所属库，默认是default

  * 必选：是 

  * 默认值：无 

* **tableName**

  * 描述：目标hive表名称

  * 必选：是 

  * 默认值：无 

* **defaultFS**

  * 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:port；例如：hdfs://127.0.0.1:9000

  * 必选：是 

  * 默认值：无 

* **writeMode**
  * 描述：数据写入模式(支持insert/overwrite)

  * 必选：否 

  * 默认值：insert 

* **partition**
  * 描述：分区字段  ,  逗号分隔；示例 dt 或 dt1,dt2

  * 必选：否 

  * 默认值：无 

* **column**
  * 描述：手动指定hive表列信息,按照建表的字段顺序列出来,包括分区列信息
  * 必选：是 
  * 默认值：无 

- **tmpDatabase**
  - 描述：hive临时表存放的database(需写权限); **建议根据实际场景设置**
  - 必选：否
  - 默认值：与“**databaseName**”值一致

- **tmpDatabasePath**
  - 描述：hive临时表所在数据库的hdfs路径(需写权限)；**建议根据实际场景设置**
  - 必选：是
  - 默认值：无 

- **hive_cmd**
  - 描述：执行的hive命令
  - 必选：否
  - 默认值：hive 

- **hive_sql_set**
  - 描述：hive临时表执行的前置hive set语句；示例，设置队列set tez.queue.name=root.xxxxxxx；
  - 必选：否
  - 默认值：无 

- **fieldDelimiter**
  - 描述：字段分隔符，hive临时表使用，建议**不设置**
  - 必选：否
  - 默认值：\\\u0001，（json里面需要写成'\\u0001'），可选示例'\\\t'  , ',' 等；注意，这与HdfsReader不同

- **nullFormat**

  - 描述：文本文件中无法使用标准字符串定义null(空指针)，nullFormat可定义哪些字符串可以表示为null。

    例如如果用户配置: nullFormat:"\\\N"，那么如果源头数据是null，写入HDFS时用\N替代。

  - 必选：否

  - 默认值：\\\\N

- **compress**

  - 描述：临时表HDFS文件压缩格式，建议**不设置**
  - 必选：否
  - 默认值：gzip 

- **haveKerberos**
  - 描述：是否进行Kerberos认证(true/false)
  - 必选：否
  - 默认值：无 

- **kerberosKeytabFilePath**
  - 描述：Kerberos认证 keytab文件路径，绝对路径,如/etc/security/keytabs/xxxx_user.keytab
  - 必选：否
  - 默认值：无 

- **kerberosPrincipal**
  - 描述：Kerberos认证Principal名，如xxxx/hadoopclient@xxx.xxx 
  - 必选：否
  - 默认值：无 

- **hadoopConfig**

  - 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。
  - 必选：否
  - 默认值：无 

- **preSql**
  - 描述：写入数据到目的表前，会先执行这里的标准HiveQL语句。
  - 必选：否
  - 默认值：无 

- **postSql**
  - 描述：写入数据到目的表后，会执行这里的标准HiveQL语句。
  - 必选：否
  - 默认值：无 

#### 3.3 环境准备

- 执行datax任务的机器要安装好hive命令,并且配置好环境变量
- tempDatabase需要有“**写**”权限


## 4 性能报告

### 4.1测试环境
已在以下测试环境测试通过:

- **测试环境1**

  CDH 5.7.0 (hive 1.1.1 , hadoop 2.7.1)

- **测试环境2**

  HDP 3.1.4 (hive 3.1.0 , hadoop 3.1.1 )

### 4.2 测试报告

- 场景一：从mysql导入数据到hive的非分区表，json只传必选参数，其他为默认参数值

  目标表:stage.stage_md_test_new  非分区表

  ```json
  {
    "job": {
      "setting": {
        "speed": {
          "channel":1
        }
      },
      "content": [
        {
          "reader": {
            "name":"mysqlreader",
                                  "parameter":{
                                          "connection":[
                                                  {
                                                          "jdbcUrl":[
   "jdbc:mysql://localhost:3306/stage?useUnicode=true&characterEncoding=UTF-8"
                                                          ],
                                                          "querySql":[
                                                                  "select id,pay_decimal,pay_str,pay_double,etl_date,dt from stage.stage_md_test_new"
                                                          ]
                                                  }
                                          ],
                                          "password":"cloudera",
                                          "username":"root"
                                  }
          },
          "writer": {
            "name": "hivewriter",
            "parameter": {
              "databaseName":"stage",
              "tableName": "stage_md_test_no_partition",
              "defaultFS": "hdfs://quickstart.cloudera:8020",
  			"tmpDatabase":"stage",
  			"tmpDatabasePath":"/user/hive/warehouse/stage.db/",
              "column": [ {
                              "name": "id",
                              "type": "INT"
                            },
                            {
                              "name": "pay_decimal",
                              "type": "string"
                            },
                            {
                              "name": "pay_str",
                              "type": "STRING"
                            },
                            {
                              "name": "pay_double",
                              "type": "double"
                            },
                            {
                              "name": "etl_date",
                              "type": "string"
                            },
                            {
                              "name": "dt",
                              "type": "string"
                            }
                            
                          ]
            }
          }
          }
      ]
    }
  
  }
  ```

- 场景二：从mysql导入数据到hive的单分区表，json只传必选参数，其他为默认参数值

  目标表:stage.stage_md_test1 单分区表，分区字段：etl_date

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel":1
      }
    },
    "content": [
      {
        "reader": {
          "name":"mysqlreader",
                                "parameter":{
                                        "connection":[
                                                {
                                                        "jdbcUrl":[
                              "jdbc:mysql://localhost:3306/stage?            useUnicode=true&characterEncoding=UTF-8"
                                                        ],
                                                        "querySql":[
                                                            "select id,pay_decimal,pay_str,pay_double,etl_date dt from stage.stage_md_test1 ;"
                                                        ]
                                                }
                                        ],
                                        "password":"cloudera",
                                        "username":"root"
                                }
        },
        "writer": {
          "name": "hivewriter",
          "parameter": {
            "databaseName":"stage",
            "tableName": "stage.stage_md_test1",
            "defaultFS": "hdfs://quickstart.cloudera:8020",
            "tmpDatabase":"stage",
			"tmpDatabasePath":"/user/hive/warehouse/stage.db/",
            "partition": ["etl_date"],
            "column": [ {
                            "name": "id",
                            "type": "INT"
                          },
                          {
                            "name": "pay_decimal",
                            "type": "string"
                          },
                          {
                            "name": "pay_str",
                            "type": "STRING"
                          },
                          {
                            "name": "pay_double",
                            "type": "double"
                          },
                          {
                            "name": "etl_date",
                            "type": "string"
                          }
                          
                        ]
          }
        }
        }
    ]
  }

}



```

- 场景三：从mysql导入数据到hive的多分区表，json传入所有可选参数

  目标表：stage.stage_md_test2，多分区表，分区字段：type,etl_date

  ```json
  {
    "job": {
      "setting": {
        "speed": {
          "channel":1
        }
      },
      "content": [
        {
          "reader": {
            "name":"mysqlreader",
                                  "parameter":{
                                          "connection":[
                                                  {
                                                          "jdbcUrl":[
                                "jdbc:mysql://localhost:3306/stage?            useUnicode=true&characterEncoding=UTF-8"
                                                          ],
                                                          "querySql":[
                                                              "select id,pay_decimal,pay_str,pay_double,type,etl_date dt from stage.stage_md_test2 ;"
                                                          ]
                                                  }
                                          ],
                                          "password":"cloudera",
                                          "username":"root"
                                  }
          },
          "writer": {
            "name": "hivewriter",
            "parameter": {
              "databaseName":"stage",
              "tableName": "stage.stage_md_test2",
              "defaultFS": "hdfs://quickstart.cloudera:8020",
              "writeMode": "overwrite",
              "tmpDatabase":"stage",
  			"tmpDatabasePath":"/user/hive/warehouse/stage.db/",
              "partition": ["type,etl_date"],
              "hive_cmd":"hive",
              "hive_sql_set":"set tez.queue.name=edw;",
              "preSql":"select 1;",
              "postSql":"select 1;",
              "fieldDelimiter":"\\u0001",
              "nullFormat":"\\N",
              "compress":"gzip",
              "haveKerberos":"true",
              "kerberosKeytabFilePath":"/etc/security/keytabs/xxxx_user.keytab",
              "kerberosPrincipal":"xxxx_user@HADOOP_CLUSTER_XXXXX.COM",
              "hadoopConfig":{
                 "dfs.nameservices": "hadoop_cluster",
                 "dfs.ha.namenodes.hadoop_cluster": "nn1,nn2",
                 "dfs.namenode.rpc-address.hadoop_cluster.nn1": "IPXXXXXXX01:8020",
                 "dfs.namenode.rpc-address.hadoop_cluster.nn2": "IPXXXXXXX02:8020",
                 "dfs.client.failover.proxy.provider.hadoop_cluster":     "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"},
              "column": [ {
                              "name": "id",
                              "type": "INT"
                            },
                            {
                              "name": "pay_decimal",
                              "type": "string"
                            },
                            {
                              "name": "pay_str",
                              "type": "STRING"
                            },
                            {
                              "name": "pay_double",
                              "type": "double"
                            },
                            {
                             "name": "type",
                              "type": "string"
                            },
                            {
                              "name": "etl_date",
                              "type": "string"
                            }
                            
                          ]
            }
          }
          }
      ]
    }
  
  }
  
  ```
