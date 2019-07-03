# Chapter 23. 운영 환경에서의 구조적 스트리밍

---
## 23.1 내고장성과 체크포인팅

스파크 자체적으로 장애를 극복하는 기본 기능 있음
 - CheckPointing
 - WAL (Write Ahead Logs)

==>

- 쿼리에 체크포인트 경로 설정 가능
- 경로는 HDFS, S3 등 호환 가능한 신뢰도 높은 파일 시스템으로 해야.

```scala
val static = spark.read.json("/data/activity-data")

val streaming = spark
  .readStream
  .schema(static.schema)
  .option("maxFilesPerTrigger", 10)
  .json("/data/activity-data")
  .groupBy("gt")
  .count()

val query = streaming
  .writeStream
  .outputMode("complete")
  .option("checkpointLocation", "/some/location/")
  .queryName("test_stream")
  .format("memory")
  .start()

```

---
## 23.2 애플리케이션 변경하기

체크포인팅 : 현재까지 처리한 스트림과 모든 중간 상태를 저장

=> 애플리케이션 업데이트시 **"중대한 변화 (breaking change)"** 가 있는지 확인 필요
  - 애플리케이션 코드 업데이트
  - 스파크 버전 업데이트

### 코드 업데이트
- 사용자 정의 함수 시그니처가 같은 경우에만 코드 변경 가능.
- **"새로운 컬럼 추가"** 나 **"사용자 정의 함수 변경"** 등의 작은 수정은 문제 없음.
- 중대한 변화시에는 새로운 체크포인트 디렉토리 사용해야.


### 버전 업데이트
```text
Semantic Versioning : major.minor.patch
```

- 패치 버전 업데이트시 : 이전 체크포인트 디렉터리를 사용해 재시작 가능.
- 마이너 버전 업데이트시 : 포맷 호환성 유지 노력. 릴리스 노트에 확인 필요.

### 규모 재조정 (규모 변경시의 안정적인 운영)

"유입률 >> 처리율" 애플리케이션 크기 늘려야.
 - 방법 1) 동적 자원 할당 (리노스 매니저와 배포 방식에 따라)
 - 방법 2) 익스큐터를 제거하거나 설정을 줄인 후 재시작

"인프라 구조 변경" 또는 "설정 적용"으로 재시작
 - 변경된 설정을 위해 스트림만 재시작 (ex. spark.sql.shuffle.partitions)
 - 중대한 변경시 애플리케이션 재시작

---
## 23.3 메트릭과 모니터링

### 쿼리 상태

- 특정 쿼리의 현재 상태 확인
- "지금 스트림에서 어떤 처리를 하고 있지?"
- spark-shell 등에서 사용 가능. spark-submit 애플리케이션의 경우 Listener 사용해야.

```scala
scala> query.status

{
  "message" : "Waiting for data to arrive",
  "isDataAvailable" : false,
  "isTriggerActive" : false
}
```

### 최근 진행 상환

- 처리율과 배치 주기 등 시간 기반 정보 확인
- "튜플을 얼마나 처리하고 있지?"
- "소스에서 이벤트가 얼마나 빠르게 들어오지?"

```scala
query.recentProgress
```

- 유입률 : 입력 소스에서 스트리밍 내부로 유입되는 데이터 양
- 처리율 : 유입된 데이터를 처리하는 속도
- 배치 주기 : 구조적 스트리밍은 데이터를 연산할 때 다양한 양의 이벤트를 처리하기 때문에 배치 주기가 변함

### 스파크 UI

- 트리거마다 생성된 짧은 잡이 누적된 형태로 나타남.
- 메트릭, 쿼리 실행 계획, 태스크 주기, 로그 정보 제공
- DStream API는 Streaming 탭에서 정보 확인 가능.

---
## 23.4 알림

- 대시보드의 메트릭 확인하여 잠재적 문제 발견 필요.
- 유입률이 처리율보다 떨어지는 경우 자동 알림 기능 가능. (recentProgress)
- Coda Hale, 프로메테우스, 로깅 등 활용

---
## 23.5 스트리밍 리스너

- StreamingQueryLister 클래스를 이용해 비동기 방식으로 스트리밍 쿼리 정보를 수신
- 다른 시스템과 연계하여 모니터링 가능
- StreamingQueryListener 구현 -> SparkSession에 등록

```scala
val spark: SparkSession = ...

spark.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
    }
    override def onQueryTerminated(
      queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
    }
    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
    }
})
```

- kafka에 쿼리 진행 상황을 전달하는 것도 가능

```scala
...
  override def onQueryProgress(event:
    StreamingQueryListener.QueryProgressEvent): Unit = {
    producer.send(new ProducerRecord("streaming-metrics",
      event.progress.json))
  }
...
```