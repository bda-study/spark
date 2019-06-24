# Chapter 17. 스파크 배포 환경

### - 스파크 애플리케이션 실행에 필요한 인프라 구조를 알아보자
  - 클러스터 배포 시 선택사항
  - 스파크가 지원하는 클러스터 매니저
  - 배포 시 고려사항과 배포 환경 설정

---

### - 스파크 공식 지원 클러스터 매니저
  - 스파크, 스탠드얼론 모드
  - 하둡, YARN
  - 메소스
  - 쿠버네티스 (2.3.x ~)

### - 클러스터 매니저의 역할
  - 스파크 애플리케이션을 배포해 실행할 수 있는 클러스터의 머신을 유지하고 관리
  - 클러스 매니저마다 추구하는 방향 다름

## 클러스터 환경

### - 설치형 on-premise
  - 장/단점
    - +) 사용 중인 하드웨어를 온전히 제어 => 특정 워크로드 성능 최적화 가능
    - -) 클러스터 크기 제한 : 작아도 문제, 커도 문제
    - -) 자체 저장소 시스템(하둡 파일시스템, 분산 키-값 저장소 등)을 선택/운영 직접
    - -) 지리적 복제(geo replication) 및 재해 복구(disaster recovery) 체계 직접
  - 특징
    - 자체 데이터 센터 운영 조직에 적합
    - 클러스터 매니저를 사용하여 다수의 스파크 애플리케이션을 실행하고, 자원을 동적으로 할당 가능
    - YARN : 동적 자원 공유를 스탠드얼론보다 잘 지원. 스파크 외의 애플리케이션도 실행 가능
    - 여러 종류의 저장소 선택 가능
  
### - 클라우드형 public cloud
  - 초기 빅데이터 시스템은 설치형에 적합하게 설계. 지금은 클라우드에도 좋음
  - 장/단점
    - +) 자원을 탄력적으로 사용 가능
    - +) 비용 저렴
    - +) 지리적 복제 기능 등을 지원하는 저장소 제공
  - 대표적 클라우드 서비스 : AWS, Azure, GCP
    - S3, Azure Blob, GCS 등 분리된 글로벌 저장소 시스템을 사용 하는 게 좋음<br>
      > 연산 클러스터, 저장소 클러스터를 분리하면 연산이 필요한 경우에만 <br>
     클러스터 비용 지불 가능
  - Spark 전용 클라우드 서비스 : 데이터브릭스
    - 클러스터 크기 자동 조절
    - 클러스터 자동 종료
    - 클라우드 스토리지에 최적화된 커넥터
    - 노트북 제공
    - 스탠드얼론 잡을 위한 공동 작업 환경


## 클러스터 매니저

### - 스탠드얼론 모드
- 스파크 전용 경량화 플랫폼
- -) 타 클러스터 매니저 대비 제한적 기능
- +) 빠르게 구축 가능

#### - 클러스터 시작하기
- 준비 : 노드끼리 통신할 수 있는 네트워크 환경
- 스파크 바이너리를 내려 받아 전체 노드에 설치
- 실행하기
  ```bash
  $SPARK_HOME/sbin/start-master.sh
  # 클러스터 매니저의 마스터 프로세스 시작
  # spark://HOST:PORT 형식의 URI, 워커노드 실행시 사용
  # 마스터 프로세스 UI 에서 확인 가능 : http://master-ip-address:8080
  ```
  ```bash
  $SPARK_HOME/sbin/start-slave.sh spark://MASTER_HOST:MASTER_PORT
  ```
  
#### - 스크립트를 이용한 클러스터 시작하기
- conf/slave 파일 생성 후 스파크 워커로 사용할 모든 머신의 호스트명을 한 줄에 하나씩 기록
- ssh 로 접근하기 때문에 password-less로 접근 가능하게 처리 <br>
  또는 SPARK_SSH_FOREGROUND 환경변수를 설정해 비밀번호 입력
  ```bash
  # in sbin/slaves.sh
  for slave in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  if [ -n "${SPARK_SSH_FOREGROUND}" ]; then
    ssh $SPARK_SSH_OPTS "$slave" $"${@// /\\ }" \
      2>&1 | sed "s/^/$slave: /"
  else
  ```
- 스크립트
  ```bash
  $SPARK_HOME/sbin/start[stop]-master.sh # 현재 머신에서 마스터 실행[정지]
  $SPARK_HOME/sbin/start[stop]-slaves.sh # slaves에 정의된 머신들에서 워커 실행[정지]
  $SPARK_HOME/sbin/start[stop]-slave.sh # 현재 머신에서 워커 실행[정지]
  $SPARK_HOME/sbin/start[stop]-all.sh # start[stop]-master.sh + start[stop-slaves.sh
  ```
- 참고
  https://spark.apache.org/docs/latest/spark-standalone.html

#### - 애플리케이션 제출하기
```bash
$SPARK_HOME/bin/spark_submit \
   --class ... \
   --master spark://...:... \
   --deploy-mode ... \
   ... .jar
```

### - YARN
- 잡 스케쥴링, 클러스터 자원 관리용 프레임워크 <br>
  ※ 스파크는 하둡과 거의 관련 없음. 단지 스파크가 YARN에서 실행 가능할 뿐
- 다양한 실행 프레임워크를 지원하는 통합 스케줄러

#### - 애플리케이션 제출하기
```bash
export HADOOP_CONF_DIR # or export YARN_CONF_DIR

$SPARK_HOME/bin/spark_submit \
   --class ... \
   --master yarn \
   --deploy-mode ... \
   ... .jar
```

- client 모드 : 드라이버가 클라이언트 프로세스에서 실행
- cluster 모드 : 클러스터에서 드라이버 프로세스 관리. 클라이언트는 애플리케이션 생성 후 종료 <br>
  다른 곳에서 스파크 잡이 실행되기 때문에 jar를 사전 배포하거나 --jars 인자 추가해야.
  
#### - YARN 환경의 스파크 애플리케이션 설정하기
- 하둡 설정
  - 스파크를 이용해 HDFS 파일을 읽고 쓰려면, 스파크 클래스패스에 두개의 설정 필요
  - hdfs 클라이언트의 동작 방식을 결정하는 hdfs-site.xml
  - 기본 파일 시스템의 이름을 설정하는 core-site.xml
  - HADOOP_CONF_DIR 변수 값을 spark-env.sh에 설정해서 사용하면 자동 참조됨
- 참고
  https://spark.apache.org/docs/latest/running-on-yarn.html

### - 메소스
- CPU, 메모리, 저장소 그리고 다른 연산 자원을 머신에서 추상화 <br>
  => 내고장성 (fault-tolerant), 탄력적 분산 시스템 (elastic distributed system) 가능
- 스파크처럼 짧게 실행되는 애플리케이션 관리 가능
- 웹 애플리케이션이나 다른 자원 인터페이스 등 오래 실행되는 애플리케이션 관리 가능
- 가장 무거운 클러스터 매니저
- Spark 2.x.x 부터 fine-grained 모드는 deprecated됨. coarse-grained 모드만 가능 <br>
  => 스파크 익스큐터를 단일 메소스 태스크로 실행
- cluster 방식 추천. client 모드 사용시 클러스터 분산 자원 관리와 관련된 추가 설정 필요 <br>
  => 드라이버와 메소스 클러스터가 통신할 수 있도록 spark-env.sh 파일에 설정 추가해야 함.
  ```bash
  export MESOS_NATIVE_JAVA_LIBRARY=libmesos.so파일 경로
  ```

#### - 애플리케이션 제출하기
- spark 바이너리 파일 위치 지정
  ```bash
  export SPARK_EXECUTOR_URI=spark-2.x.x.tar.gz파일의 URL
  ```
- 실행하기
```bash
$SPARK_HOME/bin/spark_submit \
   --class ... \
   --master mesos://...:... \
   --deploy-mode ... \
   ... .jar
```
- 참고
  http://spark.apache.org/docs/latest/running-on-mesos.html


### - (네이티브) 쿠버네티스
- 컨테이너 오케스트레이션 도구
- Spark 2.3.x 부터 지원
- Spark 2.4.x 부터 Client 모드 지원
- 컨테이너 환경에서 클러스터 매니저를 사용하게 되면, 하나의 컨테이너 내에 여러 Executor가 생성되는 문제가 발생. <br>
  네이티브 쿠버네티스를 사용하면 Executor가 독립된 pod에 생성

#### - 애플리케이션 제출하기
```bash
$SPARK_HOME/bin/spark-submit \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=<spark-image> \
    local:///path/to/examples.jar
```
- 참고
http://spark.apache.org/docs/latest/running-on-kubernetes.html



### 보안 관련 설정
- 보안 관련 설정은 통신 방식과 관련. 인증, 네트워크 구간 암호화, SSL/TLS 설정 관련.
- 참고
http://spark.apache.org/docs/latest/security.html

### 클러스터 네트워크 설정
- 서비스 포트, network timeout 등 설정 가능
- 참고
http://spark.apache.org/docs/latest/configuration.html#networking

### 애플리케이션 스케줄링
- 클러스터 매니저에 의한 애플리케이션 스케줄링 : 정적 할당 (default), 동적 할당
- 스파크 애플리케이션 내에서 잡 간의 스케줄링 : FIFO 스케줄러 (default), FAIR 스케줄러

### 동적 할당
- 모든 coarse-grained 클러스터 매니저에서 사용 가능 : 스탠드얼론, Yarn, 메소스 coarse-grained 모드
- 쿠버네티스에서는 HPA (Horizontal Pod Autoscaler) 기능을 이용해 유사한 효과 볼 수 있을 것 같음.
- 설정
  ```bash
  spark.dynamicAllocation.enabled = true
  spark.shuffle.service.enabled = true # 외부 셔플 서비스를 이용하면 Executor가 제거될 때 셔플 파일을 유지
  ```
- 참고
  https://spark.apache.org/docs/2.3.0/job-scheduling.html#dynamic-resource-allocation


## 기타 고려사항
### - 클러스터 매니저
  - 애플리케이션 개수와 유형에 의존
  - YARN은 다양한 애플리케이션 지원 가능. HDFS를 사용하는 애플리케이션에 적합. 연산과 저장소가 강하게 결합. 함께 확장해야 함.
  - 메소스는 다양한 애플리케이션 지원, 더 큰 규모의 클러스터에 적합
  - 스탠드얼론은 가장 가벼움. 이해와 활용 쉬움. 많은 애플리케이션 관리를 해야 한다면 YARN이나 메소스가 좋은 선택

### - Version
  -  스파크 애플리케이션이 다양한 버전으로 되어있다면, 설정 스크립트를 관리 잘해야 함.
  -  제한하는 것도 방법

### - Log
  - YARN, 메소스는 로그 기록 기능이 기본. 스탠드얼론 모드에서는 약간의 수정 필요.

### - 메타스토어
  - 데이터 카탈로그 같은 데이터셋의 메타데이터 관리를 위해 메타스토어 사용 고려
  - 하이브 메타스토어를 사용하면 같은 데이터셋을 참조하는 여러 애플리케이션의 생산성을 높일 수 있음

### - 외부 셔플 서비스
  - 스파크는 셔플 블록을 특정 노드의 로컬 디스크 저장
  - 외부 셔플 서비스르 이용하면 모든 익스큐터가 외부 셔플 서비스에 셔플 블록을 저장 <br>
    => 익스큐터 제거해도 셔플 결과 사용 가능

