---
layout: post
comments: false
categories: grafana
---

### grafana

使用 grafana 來監控一些數據．


先下載 grafana-6.2.0-beta1、prometheus-2.9.2.darwin-amd64、pushgateway-0.8.0.darwin-amd64

```
> mygrafana > ll
total 202904
drwxr-xr-x  12 daniel  staff   384B May  9 10:49 grafana-6.2.0-beta1
drwxr-xr-x@ 10 daniel  staff   320B May  9 18:46 prometheus-2.9.2.darwin-amd64
drwxr-xr-x@  7 daniel  staff   224B May  9 18:03 pushgateway-0.8.0.darwin-amd64
```

#### 啟動 grafana

```
./grafana-server web
INFO[05-15|10:28:13] Starting Grafana                         logger=server version=6.2.0-beta1 commit=9d877d6 branch=HEAD compiled=2019-05-07T22:08:46+0800
INFO[05-15|10:28:13] Config loaded from                       logger=settings file=/Users/daniel/1-project/2-ght/mygrafana/grafana-6.2.0-beta1/conf/defaults.ini
INFO[05-15|10:28:13] Config loaded from                       logger=settings file=/Users/daniel/1-project/2-ght/mygrafana/grafana-6.2.0-beta1/conf/custom.ini
INFO[05-15|10:28:13] Path Home                                logger=settings path=/Users/daniel/1-project/2-ght/mygrafana/grafana-6.2.0-beta1
INFO[05-15|10:28:13] Path Data                                logger=settings path=/Users/daniel/1-project/2-ght/mygrafana/grafana-6.2.0-beta1/data
INFO[05-15|10:28:13] Path Logs                                logger=settings path=/Users/daniel/1-project/2-ght/mygrafana/grafana-6.2.0-beta1/data/log
INFO[05-15|10:28:13] Path Plugins                             logger=settings path=/Users/daniel/1-project/2-ght/mygrafana/grafana-6.2.0-beta1/data/plugins
INFO[05-15|10:28:13] Path Provisioning                        logger=settings path=/Users/daniel/1-project/2-ght/mygrafana/grafana-6.2.0-beta1/conf/provisioning
INFO[05-15|10:28:13] App mode production                      logger=settings
INFO[05-15|10:28:13] Initializing SqlStore                    logger=server
INFO[05-15|10:28:13] Connecting to DB                         logger=sqlstore dbtype=sqlite3
INFO[05-15|10:28:13] Starting DB migration                    logger=migrator
INFO[05-15|10:28:13] Initializing HTTPServer                  logger=server
INFO[05-15|10:28:13] Initializing InternalMetricsService      logger=server
INFO[05-15|10:28:13] Initializing RemoteCache                 logger=server
INFO[05-15|10:28:13] Initializing QuotaService                logger=server
INFO[05-15|10:28:13] Initializing LoginService                logger=server
INFO[05-15|10:28:13] Initializing PluginManager               logger=server
INFO[05-15|10:28:13] Starting plugin search                   logger=plugins
INFO[05-15|10:28:13] Initializing RenderingService            logger=server
INFO[05-15|10:28:13] Initializing AlertingService             logger=server
INFO[05-15|10:28:13] Initializing DatasourceCacheService      logger=server
INFO[05-15|10:28:13] Initializing HooksService                logger=server
INFO[05-15|10:28:13] Initializing SearchService               logger=server
INFO[05-15|10:28:13] Initializing ServerLockService           logger=server
INFO[05-15|10:28:13] Initializing TracingService              logger=server
INFO[05-15|10:28:13] Initializing UsageStatsService           logger=server
INFO[05-15|10:28:13] Initializing UserAuthTokenService        logger=server
INFO[05-15|10:28:13] Initializing CleanUpService              logger=server
INFO[05-15|10:28:13] Initializing NotificationService         logger=server
INFO[05-15|10:28:13] Initializing provisioningServiceImpl     logger=server
INFO[05-15|10:28:13] Initializing Stream Manager
INFO[05-15|10:28:13] HTTP Server Listen                       logger=http.server address=0.0.0.0:3000 protocol=http subUrl= socket=

```

![grafana-day1_1.jpg](/static/img/grafana/grafana-day1_1.jpg){:height="80%" width="80%"}

一開始會有預設一組帳密 : admin / admin  

#### 啟動 prometheus :  

```
prometheus-2.9.2.darwin-amd64 > ./prometheus --config.file=prometheus.yml
level=info ts=2019-05-15T02:37:38.200Z caller=main.go:285 msg="no time or size retention was set so using the default time retention" duration=15d
level=info ts=2019-05-15T02:37:38.200Z caller=main.go:321 msg="Starting Prometheus" version="(version=2.9.2, branch=HEAD, revision=d3245f15022551c6fc8281766ea62db4d71e2747)"
level=info ts=2019-05-15T02:37:38.200Z caller=main.go:322 build_context="(go=go1.12.4, user=root@1d43b6951e8f, date=20190424-15:38:47)"
level=info ts=2019-05-15T02:37:38.200Z caller=main.go:323 host_details=(darwin)
level=info ts=2019-05-15T02:37:38.200Z caller=main.go:324 fd_limits="(soft=4864, hard=unlimited)"
level=info ts=2019-05-15T02:37:38.200Z caller=main.go:325 vm_limits="(soft=unlimited, hard=unlimited)"
level=info ts=2019-05-15T02:37:38.202Z caller=main.go:640 msg="Starting TSDB ..."
level=info ts=2019-05-15T02:37:38.202Z caller=web.go:416 component=web msg="Start listening for connections" address=0.0.0.0:9090
level=info ts=2019-05-15T02:37:38.204Z caller=repair.go:47 component=tsdb msg="found healthy block" mint=1557371288998 maxt=1557374400000 ulid=01DADKA0ME61GBCXZZ0YVWQKMG
level=info ts=2019-05-15T02:37:38.205Z caller=repair.go:47 component=tsdb msg="found healthy block" mint=1557374400000 maxt=1557381600000 ulid=01DADP8RS3Z27R79HFDRWZX31Y
level=info ts=2019-05-15T02:37:38.205Z caller=repair.go:47 component=tsdb msg="found healthy block" mint=1557381600000 maxt=1557388800000 ulid=01DADX48WVR4ZAX2N09TQ634V9
level=info ts=2019-05-15T02:37:38.362Z caller=main.go:655 msg="TSDB started"
level=info ts=2019-05-15T02:37:38.362Z caller=main.go:724 msg="Loading configuration file" filename=prometheus.yml
level=info ts=2019-05-15T02:37:38.383Z caller=main.go:751 msg="Completed loading of configuration file" filename=prometheus.yml
level=info ts=2019-05-15T02:37:38.384Z caller=main.go:609 msg="Server is ready to receive web requests."
level=info ts=2019-05-15T02:37:44.781Z caller=compact.go:499 component=tsdb msg="write block" mint=1557388800000 maxt=1557396000000 ulid=01DAWNMK7NWPY77XXGBE31759Q duration=728.109783ms
level=info ts=2019-05-15T02:37:44.812Z caller=head.go:540 component=tsdb msg="head GC completed" duration=2.494261ms
level=info ts=2019-05-15T02:37:45.645Z caller=compact.go:499 component=tsdb msg="write block" mint=1557396000000 maxt=1557403200000 ulid=01DAWNMKZG1HEZM796YENH8N3V duration=829.050418ms
level=info ts=2019-05-15T02:37:45.674Z caller=head.go:540 component=tsdb msg="head GC completed" duration=1.907243ms
level=info ts=2019-05-15T02:37:48.836Z caller=compact.go:444 component=tsdb msg="compact blocks" count=2 mint=1557371288998 maxt=1557381600000 ulid=01DAWNMQ6HRM85GWCQ2KFHP58B sources="[01DADKA0ME61GBCXZZ0YVWQKMG 01DADP8RS3Z27R79HFDRWZX31Y]" duration=722.930613ms

```

![grafana-day1_2.jpg](/static/img/grafana/grafana-day1_2.jpg){:height="80%" width="80%"}

設定 grafana 的 data source :  
![grafana-day1_3.jpg](/static/img/grafana/grafana-day1_3.jpg){:height="80%" width="80%"}

輸入 Prometheus 的 URL :  
![grafana-day1_4.jpg](/static/img/grafana/grafana-day1_4.jpg){:height="80%" width="80%"}

建立新的 dashboard :  
![grafana-day1_5.jpg](/static/img/grafana/grafana-day1_5.jpg){:height="80%" width="80%"}

建立新的 query :  
![grafana-day1_6.jpg](/static/img/grafana/grafana-day1_6.jpg){:height="80%" width="80%"}

接著在 query 的條件下 go_gc_duration_seconds，grafana 會定時從 prometheus 讀取數值並呈現圖表 :  
![grafana-day1_7.jpg](/static/img/grafana/grafana-day1_7.jpg){:height="80%" width="80%"}

也可以在 prometheus 查詢資料 :  
![grafana-day1_8.jpg](/static/img/grafana/grafana-day1_8.jpg){:height="80%" width="80%"}

#### 啟動 pushgateway

```
./pushgateway  --web.listen-address ":9091" --persistence.file /Users/daniel/1-project/2-ght/mygrafana/pushgateway-0.8.0.darwin-amd64/data/file
INFO[0000] Starting pushgateway (version=0.8.0, branch=HEAD, revision=d90bf3239c5ca08d72ccc9e2e2ff3a62b99a122e)  source="main.go:65"
INFO[0000] Build context (go=go1.11.8, user=root@00855c3ed64f, date=20190413-11:29:57)  source="main.go:66"
INFO[0000] Listening on :9091.                           source="main.go:108"
```

![grafana-day1_9.jpg](/static/img/grafana/grafana-day1_9.jpg){:height="80%" width="80%"}

接著修改 prometheus 的 prometheus.yml 檔案，新增來源，prometheus 就會去 pushgateway 讀取資料 : 
```
  - job_name: pushgateway
    static_configs:
    - targets: ['localhost:9091']
```

prometheus.yml :  

```
# my global config
global:
  scrape_interval:     15s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
  evaluation_interval: 15s # Evaluate rules every 15 seconds. The default is every 1 minute.
  # scrape_timeout is set to the global default (10s).

# Alertmanager configuration
alerting:
  alertmanagers:
  - static_configs:
    - targets:
      # - alertmanager:9093

# Load rules once and periodically evaluate them according to the global 'evaluation_interval'.
rule_files:
  # - "first_rules.yml"
  # - "second_rules.yml"

# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'prometheus'

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

    static_configs:
    - targets: ['localhost:9090']

  - job_name: pushgateway
    static_configs:
    - targets: ['localhost:9091']
```


可以看到 target 多一個 pushgateway :  
![grafana-day1_10.jpg](/static/img/grafana/grafana-day1_10.jpg){:height="80%" width="80%"}


post 資料給 pushgateway : 
```
echo "mappingcount 500" | curl --data-binary @- http://localhost:9091/metrics/job/idmapping_report
echo "personsCount 5000" | curl --data-binary @- http://localhost:9091/metrics/job/idmapping_report
```

可以在 pushgateway 上看到 post 的資料 : 
![grafana-day1_11.jpg](/static/img/grafana/grafana-day1_11.jpg){:height="80%" width="80%"}

可以在 prometheus 查到到 pushgateway 的資料 : 
![grafana-day1_12.jpg](/static/img/grafana/grafana-day1_12.jpg){:height="80%" width="80%"}

接著就可以在 grafana 拉想要的圖表 :  
![grafana-day1_13.jpg](/static/img/grafana/grafana-day1_13.jpg){:height="80%" width="80%"}

dashboard 畫面 :  
![grafana-day1_14.jpg](/static/img/grafana/grafana-day1_14.jpg){:height="80%" width="80%"}

流程圖 :  
![grafana-day1_15.jpg](/static/img/grafana/grafana-day1_15.jpg){:height="60%" width="60%"}

### 使用 node_exporter

下載 node_exporter :  
```
git clone https://github.com/prometheus/node_exporter.git
```
下 make 時遇到下面錯誤 :  

```
> make
xcrun: error: invalid active developer path (/Library/Developer/CommandLineTools), missing xcrun at: /Library/Developer/CommandLineTools/usr/bin/xcrun
```

安裝 xcode  
```
xcode-select --install
sudo xcode-select -switch /
```
安裝完 make 還是遇到很多問題，在 Mac 上 make 不成功．


所以先用測試環境直接執行 :  

```
/usr/sbin/node_exporter --web.listen-address=:4100 --collector.textfile.directory /Users/daniel/1-project/2-ght/mygrafana/nodeExporter/textfile_collector
```

修改 prometheus.yml 檔案，新增來源，prometheus 就會去 node-exporter 讀取資料 :  
```
- job_name: 'node-exporter'
    static_configs:
            - targets: ['HDFS1:4100', 'HDFS2:4100', 'HDFS3:4100', 'Rediscluster1:4100', 'Rediscluster2:4100', 'Rediscluster3:4100']
```
可以在很多台機器上執行 node-exporter．


只要在 /Users/daniel/1-project/2-ght/mygrafana/nodeExporter/textfile_collector 路徑產生 prom 檔案，按照格式寫入就可以讓 prometheus 抓到 node-exporter 的資料 :  

```
/Users/daniel/1-project/2-ght/mygrafana/nodeExporter/textfile_collector/chtWeeklyReport.prom
```

chtWeeklyReport.prom 檔案內容格式如下 :  
```
labelcount_report{content="totalLabelCount"} 50000
labelcount_report{content="clientLabelCount"} 1000
idmapping_report{content="mappingcount"} 500
idmapping_report{content="personsCount"} 5000
idmapping_report{content="mappingPercentage"} 0.1
```






