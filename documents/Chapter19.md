# Chapter 19. 성능 튜닝

잡의 실행 속도를 높이기 위한 성능 튜닝 방법과 요소를 알아보자

## 간접적인 성능 향상 기법

- 속성 값, 런타임 환경 조정을 통한 성능 향상
- 일반적인 성능 향상 기법

### 1. 설계 방안

#### 1.1. 스칼라 vs 자바 vs 파이썬 vs R

- 사용환경에 따라 언어 달라지므로 명확한 해답은 없음
    - ex) 대규모 ETL 후 단일 머신에서 SparkR을 통한 머신러닝 작업
- 구조적 API 사용 가능한 경우: 가장 편안하고 상황에 적합한 언어 사용
- 구조적 API 사용 불가능한 경우: 파이썬을 주 언어로, RDD 트랜스포메이션/UDF는 스칼라 사용
    - R이나 파이썬으로 UDF 정의할 경우 프로세스간 직렬화 cost 발생
- [apache-spark-scala-vs-java-v-python-vs-r-vs-sql26](https://mindfulmachines.io/blog/2018/6/apache-spark-scala-vs-java-v-python-vs-r-vs-sql26)

#### 1.2. DataFrame vs SQL vs Dataset vs RDD

- 근본적으로는 UDF 대신 DataFrame, SQL 사용
    - 구조적 API가 더욱 최적화된 RDD로 변환해줌
- RDD를 직접 사용할 경우 스칼라나 자바를 사용하고, 불가피하게 파이썬 등으로 RDD를 사용할 경우 사용 영역을 최소한으로 제한

### 2. RDD 객체 직렬화

- 기본 직렬화 기능 대신 [Kryo](https://github.com/EsotericSoftware/kryo) 직렬화 사용
    - 장점: 자바 직렬화에 비해 약 10배 이상 좋은 성능
    - 단점: 사용자가 직접 클래스 등록해야 하는 번거로움 (Kryo가 기본값이 아닌 이유), 모든 직렬화 지원하지 않음
- Kyro 클래스 등록
    ```scala
    val conf = new SparkConf().setMaster(...).setAppName(...)
    conf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2]))
    val sc = new SparkContext(conf)
    ```

### 3. 클러스터 설정

#### 3.1. 클러스터/애플리케이션 규모 산정과 자원 공유

- 하드웨어나 사용 환경에 따라 변화하므로 꾸준한 모니터링을 통한 최적화 필요
- 자원 공유와 스케쥴링 문제로 요약 가능

#### 3.2. 동적 할당

- 워크로드에 따라 애플리케이션이 차지하는 자원을 동적으로 조절
- 다수의 애플리케이션이 클러스터 자원 공유하는 환경에서 유용
- `spark.dynamicAllocation.enabled` 옵션을 `true`로 설정
- [스파크 공식 문서](https://spark.apache.org/docs/latest/job-scheduling.html#configuration-and-setup)

### 4. 스케쥴링

- `spark.scheduler.mode` 값을 `FAIR`로 설정
- `--max-executor-cores` 값 조절해 모든 자원 점유하는 것 방지
- "스케쥴링 최적화는 연구와 실험이 필요한 영역" -> 휴리스틱하게 풀어야 한다? or 정답이 없다?

### 5. 보관용 데이터

#### 5.1 파일 기반 장기 데이터 저장소

- 각 사례별 데이터 저장의 best practice를 따르자!
- 바이너리 형태 저장 시 구조적 API 사용
- csv 보다는 파케이 사용하는 것이 효율적

#### 5.2 분할 가능한 파일 포맷과 압축

- 분할 가능하다 -> 여러 태스크가 동시에 여러 파일 조각을 읽을 수 있다
- 분할 시 장점
    1. 쿼리에 필요한 부분만 읽어올 수 있다
    2. HDFS 등의 분산 파일 시스템에 최적화 가능
- 분할 불가능 포맷: json, zip, tar
- 분할 가능한 포맷: gzip, bzip2, lz4

#### 5.3 테이블 파티셔닝

- 파티션 기준으로 디렉터리 경로 저장됨
- 스파크는 키를 기준으로 상관 없는 데이터를 건너뛰고 파일 읽어들임
- 즉, 키로 자주 사용되는 필드를 파티션으로 설정하면 필터링에 효율적
- 파티션 단위를 너무 작게 잘라 파일개수가 커지면 파일 목록을 읽어들이는 오버헤드 증가
- 파티셔닝 예제
    ```scala
    csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")
        .save("/tmp/partitioned-files.parquet")
    ```
    ```bash
    ls /tmp/partitioned-files.parquet
    DEST_COUNTRY_NAME=Costa Rica/
    DEST_COUNTRY_NAME=Egypt/
    DEST_COUNTRY_NAME=Equatorial Guinea/
    DEST_COUNTRY_NAME=Senegal/
    DEST_COUNTRY_NAME=United States/
    ```

#### 5.4 버켓팅

- 조인, 집계를 수행할 때 버켓팅 되어있으면 사전 분할 가능
- 버켓팅을 통해 전체 파티션에 균등 분산
- 버켓 단위로 데이터를 모아 일정 수의 파일로 저장하는 예제
    ```scala
    val numberBuckets = 10
    val columnToBucketBy = "count"

    csvFile.write.format("parquet").mode("overwrite")
        .bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")
    ```
    ```bash
    ls /user/hive/wharehouse/bucketedfiles/
    part-00000-tid-1020575097626332666-8....parquet
    part-00000-tid-1020575097626332666-8....parquet
    part-00000-tid-1020575097626332666-8....parquet
    ```

#### 5.5 파일 수

- 작은 파일이 많으면 스케쥴러 부하 증가, 저장 효율 떨어짐
    - ex) HDFS 블록이 최대 128MB 저장 가능한데, 5MB 파일 30개를 저장하면 30개의 블록을 사용
- 적은 수의 대용량 파일이 있으면 스케쥴러 부하 줄어들지만 태스크 수행시간 길어짐
- 파일 크기와 개수는 trade off를 생각하고 상황에 맞춰 설정
- 효율적으로 저장하려면 파일 크기를 최소 수십 MB를 갖도록 조정

#### 5.6 데이터 지역성

- 데이터를 동일 노드의 로컬 저장소에서 찾을 경우 더욱 빠른 접근 가능
- 데이터 저장 시스템이 지역성 정보 제공할 경우에만 가능

#### 5.7 통계 수집

- 비용 기반 쿼리 옵티마이저 동작 시 필요한 통계 정보 수집
- 빠르게 성장하는 기능 중 하나
- 테이블 수준, 컬럼 수준 두 가지 종류
    ```sql
    // 테이블 수준
    ANALYZE TABLE table_name COMPUTE STATISTICS

    // 컬럼 수준
    ANALYZE TABLE table_name COMPUTE STATISTICS FOR
    COLUMNS column_name1, column_name2, ...
    ```

### 6. 셔플 설정

- 외부 셔플 서비스를 통한 성능 향상 가능 but 유지보수 어려움
- 다양한 설정값 있지만 기본값으로도 충분함

### 7. 메모리 부족과 가비지 컬렉션

#### 7.1 가비지 컬렉션 영향도 측정

- GC의 발생 빈도, 소요 시간에 대한 통계 데이터 축적하기
    - `spark.executor.extraJavaOptions` 옵션에 `-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps` 추가
- 스파크 잡 실행하고 워커 로그 축적

#### 7.2 가비지 컬렉션 튜닝

- JVM 메모리 기초지식 필요
    - 자바 힙공간은 Young/Old 영역으로 나눔
    - Young 영역은 다시 Eden,Survivor1,Survivor2 영역으로 나눔
- GC 수행절차
    1. Eden 영역이 가득차면 마이너 가비지 컬렉션 수행
    2. Eden에 살아남은 객체와 Survivor1 객체를 Survivor2 영역으로 복제
    3. 두 Survivor 영역 교체
    4. 객체가 아주 오래되었거나 Survivor2 영역이 가득차면 Old 영역으로 복제
    5. Old 영역이 가득차면 풀 가비지 컬렉션 수행, 힙 공간의 모든 객체를 추적해 레퍼런스 정보가 없는 객체들 제거. 가장 비싼 cost
- GC 튜닝의 목표: 풀 가비지 컬렉션을 피하자!
    1. 수명이 긴 캐시 데이터를 Old 영역에 저장
    2. Young 영역에 충분한 공간을 유지
- 태스크가 완료되기 전 풀 GC 자주 발생하면 캐싱 메모리를 줄이자 (태스크용 메모리 부족)
- 마이너 GC는 자주 발생하고 풀 GC가 별로 없다면 Eden 영역의 메모리를 늘리자

## 직접적인 성능 향상 기법

- 개별 잡, 스테이지, 태스크 성능 튜닝, 코드 변경을 통한 성능 향상
- 특정 스테이지나 잡에 대한 개별적인 해결책

### 1. 병렬화

- 스테이지의 속도 향상시키려면 병렬성부터 높인다
- `spark.default.parallelism` 값과 `spark.sql.shuffle.partitions` 값을 클러스터 코어 수에 맞게 설정
- 스테이지에서 처리할 데이터 양이 많다면 클러스터 코어 당 2~3개 이상 태스크를 할당

### 2. 향상된 필터링

- 최종 결과와 무관한 데이터 필터링을 가급적 먼저 수행하는 것이 좋다

### 3. 파티션 재분배와 병합

- 파티션 재분배는 셔플을 수반하나, 데이터가 균등 분배되므로 이후 전체 실행단계에서의 조인, cache 메서드 등에 최적화
- 가능한 적은 양의 데이터를 셔플 수행
- 셔플하기 전에 `coalesce` 메서드를 통해 파티션 수를 줄여주는 것이 좋음

#### 3.1 사용자 정의 파티셔닝

- 잡이 여전히 느리다면 RDD를 이용한 사용자 정의 파티셔닝 기법 사용
- DataFrame보다 더 정밀한 데이터 제어 가능

### 4. 사용자 정의 함수(UDF)

- UDF를 최대한 피하는 것도 최적화 방법 중 하나
- UDF는 데이터를 JVM 객체로 변환하여 쿼리의 레코드 당 여러번 수행되는 고비용 함수
- 대량의 데이터를 한번에 처리하는 vectorized UDF 개발 중 (파이썬은 사용 가능)

### 5. 임시 데이터 저장소(캐싱)

- 여러번 자주 사용하는 데이터셋은 캐싱을 통해 최적화 가능
- 임시 저장소에 DataFrame, RDD 등으로 저장
- 지연 연산: 데이터에 실제로 접근해야 캐싱이 일어남
- RDD 캐싱 vs 구조적 API 캐싱 차이?
    - RDD 캐싱: 물리적 데이터를 캐시 저장
    - 구조적 API 캐싱: 물리적 실행 계획을 키로 캐시 저장
- [데이터 캐시 저장소 레벨 공식 문서](https://spark.apache.org/docs/latest/api/java/org/apache/spark/storage/StorageLevel.html)
- 캐시된 DataFrame 적용 예제
    ```scala
    val tax = spark.read.format("csv")
        .option("header", "true")
        .load("data/seoul_tax.csv")

    tax.cache() // persist도 사용 가능
    tax.count() // 캐싱은 지연처리. 즉시 캐싱위해 이처럼 사용

    val taxByIndividual = tax.groupBy("개인").count().show()
    val taxByCorporation = tax.groupBy("법인").count().show()
    val taxByTotal = tax.groupBy("전체").count().show()
    ```

### 6. 조인

- 조인 순서를 변경하는 것만으로도 효율성 향상
- 동등 조인은 최적화하기 쉬우므로 우선적으로 사용
- 카테시안 조인, 전체 외부 조인은 필터링 형식의 조인으로 최대한 변경
- 테이블 통계를 수집하면 스파크가 조인 타입 결정할 때 수월함
- 적절한 버켓팅을 하면 조인 수행시 거대한 셔플 발생하지 않도록 방지 가능

### 7. 집계

- 집계하기 전 최대한 필터링 하는 것이 최선의 방법

### 8. 브로드캐스트 변수

- 모든 태스크마다 직렬화 하지 않고 클러스터 내의 모든 머신에 캐시하는 불변성 공유 변수
- 어플리케이션 내 다수의 UDF에서 큰 데이터 조각을 사용할 시 각 데이터 조각을 읽기 전용으로 개별 노드에 저장
- 모든 워커 노드에 큰 값을 저장하여 잡 마다 재전송 없이 재사용 가능
- 룩업테이블, 머신러닝 모델 저장시 사용 가능
- 브로드캐스트 변수 사용 예제
    ```scala
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val words = spark.sparkContext.parallelize(myCollection, 2)
    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200, "Big" -> 300, "Simple" -> 100)

    val suppBroadcast = spark.sparkContext.broadcast(supplementalData) // 브로드캐스트 불변객체 생성
    suppBroadcast.value // 액션을 실행할 때 클러스터의 모든 노드에 지연 처리 방식으로 복제

    words
      .map(word => (word, suppBroadcast.value.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2)
      .collect()
    ```

## 정리

- 파티셔닝, 버켓팅 사용하기
- 바이너리 포맷을 사용하고 가능한 작은 데이터 읽기
- 병렬성을 고려하고 파티셔닝을 통한 skew 현상 방지하기
- 구조적 API를 통해 최적화된 코드 사용하기