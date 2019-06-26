# Chapter 12 RDD (319~, 23)

12.1 저수준 API란

- 저수준 API의 종류
    1. 분산 데이터 처리를 위한 RDD
    2. 브로트캐스트, 어큐뮬레이터와 같은 분산형 공유 변수

- 언제 사용하는가?
    1. 고수준 API 에서 제공하지 않는 기능이 필요한 경우. 물리적 데이터 배치를 아주 세밀하게 제어할 때.
    2. RDD 활용한 기존 코드 유지 보수
    3. 공유 변수다룰 때

- 구조적 API 권장
    1. 스파크의 모든 워크로드는 저수준 기능을 사용하는 기초적인 형태로 컴파일됨
    2. 숙련된 개발자라 하더라도 구조적 API 위주로 사용하는 것 권함

- 사용방법

        spark.sparkContext // SparkSession.SparkContext

12.2 RDD 개요

- RDD는 Spark 1.x의 핵심 API. 2.x에서는 구조적 API 중심.
- 최종적으로 DataFrame, Dataset 코드는 RDD로 컴파일됨.
- 구조적 API는 자동으로 데이터를 최적화하고, 압축된 바이너리 포맷으로 저장.

- RDD 특징 : 불변성, 파티셔닝된 레코드의 모음

- RDD 종류
    1. 제네릭 RDD (일반적인 RDD)
    2. 키밸류 RDD (Pair RDD) : key를 이용한 사용자 지정 파티셔닝 개념과 특수 연산 가능

- RDD 속성
    1. 파티션 목록 (partitions)
    2. 각 조각 연산 함수 (iterator)
    3. 다른 RDD와의 의존성 목록 (dependencies)
    4. PairRDD를 위한 Partitioner (partitioner)
    5. 조각 연산을 위한 기본 위치 (preferredLocations)

12.3 RDD 생성하기

- Dataset에서 RDD 생성방법

        // import org.apache.spark.sql.Dataset
        // import java.lang.Long
        val ds: Dataset[Long] = spark.range(500)
        
        // import org.apache.spark.rdd.RDD
        val rdd: RDD[Long] = ds.rdd

- DataFrame에서 RDD 생성 및 변환

        // import org.apache.spark.sql.DataFrame
        val df: DataFrame = spark.range(10).toDF()
        
        // import org.apache.spark.sql.Row
        val rdd: RDD[Row] = df.rdd   // Row 타입을 가진 RDD를 생성
        val mappedRdd: RDD[Long] = rdd.map(r => r.getLong(0))

- RDD에서 DataFrame 생성

        val rdd: RDD[Long] = spark.range(10).rdd
        val df: DataFrame = rdd.toDF()

- 로컬 컬렉션에서 RDD 생성

        val myCollection: Array[String] = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
        val words: RDD[String] = spark.sparkContext.parallelize(myCollection, 2)

- RDD 이름

        words.setName("myWords") // Spark Application Web UI, Stroage 탭에서 확인 가능 (저장시)
        val name: String = words.name

- 데이터소스에서 RDD 생성

        > // show file list
        > import sys.process._
        > "ls -l conf".!
        
        total 8
        -rw-r--r-- 1 zepl zepl  400 Jun 12  2018 core-site.xml
        -rw-r--r-- 1 zepl zepl 1031 Jan 18 02:07 log4j.xml
        import sys.process._
        res9: Int = 0
        
        
        
        > // textFile
        
        > spark.sparkContext.textFile("conf/log4j.xml").count
        res12: Long = 27
        
        > spark.sparkContext.textFile("conf").count
        res40: Long = 40
        
        
        
        > // whoeTextFiles
        
        > spark.sparkContext.wholeTextFiles("conf/log4j.xml").count
        res17: Long = 1
        
        > spark.sparkContext.wholeTextFiles("conf").count
        res18: Long = 2
        
        
        > val rdd: RDD[(String, String)] = spark.sparkContext.wholeTextFiles("conf")
        rdd: org.apache.spark.rdd.RDD[(String, String)] = conf MapPartitionsRDD[42] at wholeTextFiles at <console>:34
        
        > rdd.keys.collect
        res22: Array[String] = Array(file:/usr/zepl/interpreter/conf/log4j.xml, file:/usr/zepl/interpreter/conf/core-site.xml)

12.4 RDD 다루기

- RDD는 스파크 데이터 타입 대신 자바나 스칼라의 객체를 다룬다는 사실이 DataFrame과 다르다

12.5 트랜스포메이션

1. distinct
    - 중복 제거

        words.distinct().count()

2. filter

- where과 유사. 조건 함수는 불리언 타입 반환.

    > def startsWithS(s: String) = s.startsWith("S")
    > words.filter(startsWithS).collect()
    res23: Array[String] = Array(Spark, Simple)

3. map

- 단순 변환 map (functor)

    > val words2: RDD[(String, Char, Boolean)] = words.map( w => (w, w(0), w.startsWith("S)) )
    > words2.filter( _._3 ).collect
    res26: Array[(String, Char, Boolean)] = Array((Spark,S,true), (Simple,S,true))
    

- flatMap (monad)

    > words.flatMap( _.toSeq ).collect
    res27: Array[Char] = Array(S, p, a, r, k, T, h, e, D, e, f, i, n, i, t, i, v, e, G, u, i, d, e, :, B, i, g, D, a, t, a, P, r, o, c, e, s, s, i, n, g, M, a, d, e, S, i, m, p, l, e)

4. sortBy

- 정렬

    > words.sortBy( _.length() * -1 ).take(5) //기본 ASC, -1곱해서 DESC
    res30: Array[String] = Array(Definitive, Processing, Simple, Spark, Guide)

5. randomSplit

- 임의로 RDD 분할

    > words.randomSplit(Array[Double](0.5, 0.5))
    res44: Array[org.apache.spark.rdd.RDD[String]] = Array(MapPartitionsRDD[108] at randomSplit at <console>:39, MapPartitionsRDD[109] at randomSplit at <console>:39)

- 데이터 확인

    > // 결과가 매번 바뀜뀜
    > words.randomSplit(Array(0.5, 0.5)).foreach(r => {println("---"); println(r.collect mkString "\n")})
    ---
    Spark
    Definitive
    Data
    Made
    ---
    The
    Guide
    :
    Big
    Processing
    Simple
    
    > words.randomSplit(Array(0.2, 0.1)).foreach(r => {println("---"); println(r.collect mkString "\n")})
    ---
    Definitive
    :
    Big
    Data
    Processing
    Simple
    ---
    Spark
    The
    Guide
    Made
    
    > words.randomSplit(Array(100, 0.1)).foreach(r => {println("---"); println(r.collect mkString "\n")})
    ---
    Spark
    The
    Definitive
    Guide
    :
    Big
    Data
    Processing
    Made
    Simple
    ---
    
    > words.randomSplit(Array(0,5, 0.5)).map(_.count).sum
    res60: Long = 10
    
    > words.randomSplit(Array(0.2, 0.1)).map(_.count).sum
    res61: Long = 10
    
    > words.randomSplit(Array(100, 0.1)).map(_.count).sum
    res62: Long = 10
    

12.6 액션

- 데이터를 드라이버로 모으거나 외부 데이터소르로 보내는 작업

1. reduce
    - RDD의 모든 값을 하나의 값으로 만든다.
    - 두 개의 입력값을 하나로 줄이는 함수를 사용

        sc.parallelize(1 to 20).reduce(_+_) // 210
        
        
        // 파티션에 대한 리듀스 연산은 비결정적 : 결과가 다를 수 있음
        words.reduce( (l,r) => if (l.length > r.length) l else r ) // Definitive or Processing

2. count
    - count

        : 로우 수 확인

        words.count()  // 10

    - countApprox

        : 제한된 시간 내에 근사치 계산

        : 지정된 제한 시간 내에 처리해야 하며, 제한 시간 초과시 불완전한 결과 반환

        : 신뢰도(confidence)는 실제로 연산한 결과와의 오차율. [0, 1] 범위의 값

        > words.countApprox(400, 0.95) // 반복적으로 호출하면 실제 값과 동일한 값이 95%이상 포함될 것으로 기대
        res77: org.apache.spark.partial.PartialResult[org.apache.spark.partial.BoundedDouble] = (final: [10.000, 10.000])
        
        > words.countApprox(10, 0.33)
        res76: org.apache.spark.partial.PartialResult[org.apache.spark.partial.BoundedDouble] = (partial: [8.000, 11.000])
        
        
        > words.countApprox(400, 1)
        res26: org.apache.spark.partial.PartialResult[org.apache.spark.partial.BoundedDouble] = (final: [10.000, 10.000])
        
        > words.countApprox(400, 1.1)
        java.lang.IllegalArgumentException: requirement failed: confidence (1.1) must be in [0,1]
        at scala.Predef$.require(Predef.scala:224)
          at org.apache.spark.rdd.RDD$$anonfun$countApprox$1.apply(RDD.scala:1177)
          at org.apache.spark.rdd.RDD$$anonfun$countApprox$1.apply(RDD.scala:1176)
          ...

    - countApproxDistinct

        i) 상대 정확도 (relative accuracy)를 사용. 이 값이 작으면 더 많은 메모리 공간을 사용하는 카운터가 생성됨. 최소 0.000017

            words.countApproxDistinct(0.05) // 10
            
            words.countApproxDistinct(0.50) // 9

        ii) 상대 정확도를 세부적으로 제어 가능 : 일반 데이터 위한 파라미터, 희소 표현 위한 파라미터

            p : 정밀도 (정수)   ※ 상대정확도 = 1.054 / sqrt(2^p)

            sp : 희소 정밀도 (정수)

           : 이를 잘 사용하면 메모리 소비를 줄이면서 정확도를 증가시킬 수 있다.

            words.countApproxDistinct(4, 10) // 10

    - countByValue

        : RDD 값의 개수

        : 결과 데이터셋을 드라이버의 메모리로 읽어 들이기 때문에 조심히 사용해야 함

        > words.countByValue()
        res83: scala.collection.Map[String,Long] = Map(Definitive -> 1, Simple -> 1, Processing -> 1, The -> 1, Spark -> 1, Made -> 1, Guide -> 1, Big -> 1, : -> 1, Data -> 1)
        
        > words.flatMap(_.toSeq).countByValue()
        res85: scala.collection.Map[Char,Long] = Map(e -> 7, s -> 2, n -> 2, T -> 1, t -> 2, u -> 1, f -> 1, a -> 4, m -> 1, M -> 1, i -> 7, v -> 1, G -> 1, g -> 2, B -> 1, l -> 1, P -> 1, p -> 2, c -> 1, h -> 1, r -> 2, : -> 1, k -> 1, D -> 2, o -> 1, S -> 2, d -> 2)

    - countByValueApprox

        : countApprox와 유사.

        > words.countByValueApprox(1000, 0.95)
        res87: org.apache.spark.partial.PartialResult[scala.collection.Map[String,org.apache.spark.partial.BoundedDouble]] = (final: Map(Definitive -> [1.000, 1.000], Simple -> [1.000, 1.000], Processing -> [1.000, 1.000], The -> [1.000, 1.000], Spark -> [1.000, 1.000], Made -> [1.000, 1.000], Guide -> [1.000, 1.000], Big -> [1.000, 1.000], : -> [1.000, 1.000], Data -> [1.000, 1.000]))

3. first

        words.first()  // Spark

4. max, min

        sc.parallelize(1 to 20).max()  // 20
        
        sc.parallelize(1 to 20).min()  //  1

5. take
    - 하나의 파티션 스캔 ⇒ 결과 수 확인 ⇒ 추가 파티션 수 예측

        > words.take(5) // 그냥
        res91: Array[String] = Array(Spark, The, Definitive, Guide, :)
        
        > words.takeOrdered(5) // 순서대로
        res92: Array[String] = Array(:, Big, Data, Definitive, Guide)
        
        > words.top(5) // takeOrdered와 반대 순서로
        res93: Array[String] = Array(The, Spark, Simple, Processing, Made)
        
        > words.takeSample(true, 6, 100L) // 랜덤 추출
        res94: Array[String] = Array(Guide, Spark, :, Simple, Simple, Spark)

12.7 파일 저장하기

- 처리 결과를 텍스트 파일로 쓰기

1. saveAsTextFile
    - 경로 지정 필요

        > words.saveAsTextFile("file:/tmp/bookTitle")
        > "ls -lR /tmp/bookTitle".!
        /tmp/bookTitle:
        total 8
        -rw-r--r-- 1 zepl zepl  0 Mar 12 19:14 _SUCCESS
        -rw-r--r-- 1 zepl zepl 29 Mar 12 19:14 part-00000
        -rw-r--r-- 1 zepl zepl 32 Mar 12 19:14 part-00001
        res100: Int = 0
        
        > "cat /tmp/bookTitle/part-00000".!
        Spark
        The
        Definitive
        Guide
        :
        res101: Int = 0

    - 압축 코덱 설정 가능

        > import org.apache.hadoop.io.compress.BZip2Codec
        > words.saveAsTextFile("file:/tmp/bookTitleCompressed", classOf[BZip2Codec])
        > "ls -lR /tmp/bookTitleCompressed".!
        /tmp/bookTitleCompressed:
        total 8
        -rw-r--r-- 1 zepl zepl  0 Mar 12 19:17 _SUCCESS
        -rw-r--r-- 1 zepl zepl 69 Mar 12 19:17 part-00000.bz2
        -rw-r--r-- 1 zepl zepl 71 Mar 12 19:17 part-00001.bz2
        res103: Int = 0
        
        > "cat /tmp/bookTitleCompressed/part-00000.bz2".!
        BZh91AY&SYӧ��O��'iW 1L��F���z!ة�h
        �;���d]��BCN�j�res104: Int = 0

2. 시퀀스 파일
    - 바이너리 key-value 쌍으로 구성된 플랫 파일
    - 맵리듀스의 입출력 포맷으로 사용

        > words.saveAsObjectFile("/tmp/my/sequenceFilePath")
        > "ls -lR /tmp/my/sequenceFilePath".!
        /tmp/my/sequenceFilePath:
        total 8
        -rw-r--r-- 1 zepl zepl   0 Mar 12 19:22 _SUCCESS
        -rw-r--r-- 1 zepl zepl 190 Mar 12 19:22 part-00000
        -rw-r--r-- 1 zepl zepl 193 Mar 12 19:22 part-00001
        res108: Int = 0
        
        > "cat /tmp/my/sequenceFilePath/part-00000".!
        SEQ!org.apache.hadoop.io.NullWritable"org.apache.hadoop.io.BytesWritable�G
        �fQ�zuؿd\�WS��ur[Ljava.lang.String;��V��{GxptSparktThet
        DefinitivetGuidet:res109: Int = 0

3. 하둡 파일
    - 여러 가지 하둡 파일 포맷이 있다.
    - 하둡 파일 포맷을 사용하면 클래스, 출력 포맷, 하둡 설정 그리고 압축 방식을 지정할 수 있다.

12.8 캐싱

- DataFrame, Datase의 캐싱과 동일
- 메모리에 있는 데이터를 대상
- setName으로 이름 지정 가능
- StorageLevel : 메모리, 디스크, 둘의 조합, 오프힙

    > words.getStorageLevel
    res110: org.apache.spark.storage.StorageLevel = StorageLevel(1 replicas)
    
    > words.cache
    
    > words.getStorageLevel
    res112: org.apache.spark.storage.StorageLevel = StorageLevel(memory, deserialized, 1 replicas)
    
    > words.persist
    
    > words.getStorageLevel
    res114: org.apache.spark.storage.StorageLevel = StorageLevel(memory, deserialized, 1 replicas)

12.9 체크포인팅

- DataFrame 에서 사용할 수 없는 기능
- RDD를 디스크에 저장하는 방식
- 원본 데이터소스를 계산하지 않고, 디스크에 저장된 중간 결과 파티션을 참조해서 사용 가능
- 캐싱의 디스크는 호스트의 특정 위치에 저장. 체크포인팅은 원하는 위치 지정 가능
- SparkContext가 사라져도 재사용 가능

    > sc.setCheckpointDir("/tmp/checkpointing")
    > words.checkpoint()
    > "ls -lR /tmp/checkpointing".!
    
    /tmp/checkpointing:
    total 4
    drwxr-xr-x 2 zepl zepl 4096 Mar 12 19:32 74ff595e-0d01-4aba-84b1-d2312375e70a
    
    /tmp/checkpointing/74ff595e-0d01-4aba-84b1-d2312375e70a:
    total 0
    res118: Int = 0
    
    > words.collect
    > "ls -lR /tmp/checkpointing".!
    /tmp/checkpointing:
    total 4
    drwxr-xr-x 2 zepl zepl 4096 Mar 12 19:32 74ff595e-0d01-4aba-84b1-d2312375e70a
    
    /tmp/checkpointing/74ff595e-0d01-4aba-84b1-d2312375e70a:
    total 0
    res124: Int = 0
    

12.10 RDD를 시스템 명령으로 전송하기

 0. pipe

- RDD를 외부 프로세스로 전달
- 파티션마다 한번씩 프로세스에서 처리 후 표준 출력을 파티션으로 반환

    > words.pipe("wc -l").collect()
    res2: Array[String] = Array(5, 5)

1. mapPartitions
    - 스파크는 실제 코드 실행시 파티션 단위로 동작
    - map함수는 mapPartitions의 로우 단위 처리를 위한 별칭

        // https://github.com/apache/spark/blob/v2.4.0/core/src/main/scala/org/apache/spark/rdd/RDD.scala
        // RDD.scala : 370 line
        def map[U: ClassTag](f: T => U): RDD[U] = withScope {
          val cleanF = sc.clean(f)
          new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
        }

    - mapPartitions 예제

        words.count()  // 10
        words.getNumPartitions  // 2
        words.mapPartitions(p => Iterator[Int](1)).sum()  // 2.0

    - mapPartitionsWithIndex  예제

        > def indexedFunc(i: Int, it: Iterator[String]) = it.toList.map( v => s"Partitions: $i => $v").iterator
        > words.mapPartitionsWithIndex( indexedFunc ).collect()
        indexedFunc: (i: Int, it: Iterator[String])Iterator[String]
        res8: Array[String] = Array(Partitions: 0 => Spark, Partitions: 0 => The, Partitions: 0 => Definitive, Partitions: 0 => Guide, Partitions: 0 => :, Partitions: 1 => Big, Partitions: 1 => Data, Partitions: 1 => Processing, Partitions: 1 => Made, Partitions: 1 => Simple)

2. foreachPartition
    - 순회할 뿐 결과 반환 없음
    - DB 저장 등에 활용. 실제로 많은 데이터소스 커넥터에서 이 함수 사용

        > words.foreachPartition { it =>
        >   import java.io._
        >   import scala.util.Random
        
        >   val randomFileName = new Random().nextInt()
        >   val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
        >   while (it.hasNext) pw.write(it.next())
        >   pw.close()
        > }
        total 88
        drwxr-xr-x 12 zepl zepl  4096 Mar 13 00:39 blockmgr-ad68123a-64f8-4d92-826e-3bf14d8734fb
        drwxr-xr-x  2 root root  4096 Jan 31 17:05 hsperfdata_root
        drwxr-xr-x  2 zepl zepl  4096 Mar 13 00:22 hsperfdata_zepl
        -rw-r--r--  1 zepl zepl 59545 Mar 13 00:24 liblz4-java4293336766757213133.so
        -rw-r--r--  1 zepl zepl    24 Mar 13 00:39 random-file-1384920955.txt
        -rw-r--r--  1 zepl zepl    27 Mar 13 00:39 random-file-1389598015.txt
        drwx------  3 zepl zepl  4096 Mar 13 00:22 spark-64aab8f6-5bb7-4cfb-aafd-b3b2e11fc3be
        drwx------ 42 zepl zepl  4096 Mar 13 00:41 spark1384707065281968447
        -rw-r--r--  1 zepl zepl     0 Mar 13 00:22 zeppelin_pyspark-7186326342731061003.py
        res14: Int = 0
        
        > "cat /tmp/random-file-1384920955.txt".!
        SparkTheDefinitiveGuide:res18: Int = 0
        
        > "cat /tmp/random-file-1389598015.txt".!
        BigDataProcessingMadeSimpleres19: Int = 0

3. glom
    - 모든 파티션을 배열로 반환

        > sc.parallelize( Seq("Hello", "World"), 2 ).glom().collect()
        res21: Array[Array[String]] = Array(Array(Hello), Array(World))
        
        > words.glom().collect()
        res22: Array[Array[String]] = Array(Array(Spark, The, Definitive, Guide, :), Array(Big, Data, Processing, Made, Simple))