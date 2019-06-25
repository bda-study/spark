# Chapter 25. 데이터 전처리 및 피처 엔지니어링

이 장에서는 데이터 전처리, 피처 엔지니어링 방법, 데이터 구조, 모델 학습의 핵심 요구 등을 알아보고 스파크가 제공하는 도구를 알아보자

## 사용 목적에 따라 모델 서식 지정하기

- 데이터를 통해 얻을 최종 목표를 우선 검토하기
    - 회귀 알고리즘: Double 타입의 컬럼을 레이블로, Vector 타입의 컬럼을 특징으로 설정
    - 추천 알고리즘: 사용자 컬럼, 아이템 컬럼, 등급 컬럼으로 데이터를 표현
    - 비지도 학습 알고리즘: Vector 타입의 컬럼을 입력 데이터로 사용
    - 그래프 분석: 정점과 에지를 각각 DataFrame으로 구성
- 데이터를 다양한 형태로 확보하는 방법 = **변환자**를 사용한다 (DataFrame to DataFrame)
- 예제 데이터 불러오기
    ```scala
    // 데이터셋: sales, fakeIntDF, simpleDF, scaleDF
    // 코드에서 NULL 값을 걸러낸 것은 MLlib에서 이를 처리하는 로직이 아직 없기 때문

    val sales = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/data/retail-data/by-day/*.csv")
      .coalesce(5)
      .where("Description IS NOT NULL")
    val fakeIntDF = spark.read.parquet("/data/simple-ml-integers")
    var simpleDF = spark.read.json("/data/simple-ml")
    val scaleDF = spark.read.parquet("/data/simple-ml-scaling")

    sales.cache()
    sales.show()
    ```

## 변환자 (Transformers)

![](https://learning.oreilly.com/library/view/spark-the-definitive/9781491912201/assets/spdg_2403.png)

- 원시 데이터를 변환시키는 함수: 새로운 상호작용 변수 생성, 컬럼 정규화, 변수 Double 타입 변환 등
- 주로 데이터 전처리 혹은 특징 생성을 위해 사용
- ex) Tokenizer
    ```scala
    import org.apache.spark.ml.feature.Tokenizer

    val tkn = new Tokenizer()
      .setInputCol("Description")
      .setOutputCol("TokenizedDescription")

    tkn
      .transform(sales.select("Description"))
      .show(false)
    ```
    ```text
    +-----------------------------------+------------------------------------------+
    |Description                        |TokenizedDescription                      |
    +-----------------------------------+------------------------------------------+
    |RABBIT NIGHT LIGHT                 |[rabbit, night, light]                    |
    |DOUGHNUT LIP GLOSS                 |[doughnut, lip, gloss]                    |
    |12 MESSAGE CARDS WITH ENVELOPES    |[12, message, cards, with, envelopes]     |
    |BLUE HARMONICA IN BOX              |[blue, harmonica, in, box]                |
    |GUMBALL COAT RACK                  |[gumball, coat, rack]                     |
    |SKULLS  WATER TRANSFER TATTOOS     |[skulls, , water, transfer, tattoos]      |
    |FELTCRAFT GIRL AMELIE KIT          |[feltcraft, girl, amelie, kit]            |
    |CAMOUFLAGE LED TORCH               |[camouflage, led, torch]                  |
    |WHITE SKULL HOT WATER BOTTLE       |[white, skull, hot, water, bottle]        |
    |ENGLISH ROSE HOT WATER BOTTLE      |[english, rose, hot, water, bottle]       |
    |HOT WATER BOTTLE KEEP CALM         |[hot, water, bottle, keep, calm]          |
    |SCOTTIE DOG HOT WATER BOTTLE       |[scottie, dog, hot, water, bottle]        |
    |ROSE CARAVAN DOORSTOP              |[rose, caravan, doorstop]                 |
    |GINGHAM HEART  DOORSTOP RED        |[gingham, heart, , doorstop, red]         |
    |STORAGE TIN VINTAGE LEAF           |[storage, tin, vintage, leaf]             |
    |SET OF 4 KNICK KNACK TINS POPPIES  |[set, of, 4, knick, knack, tins, poppies] |
    |POPCORN HOLDER                     |[popcorn, holder]                         |
    |GROW A FLYTRAP OR SUNFLOWER IN TIN |[grow, a, flytrap, or, sunflower, in, tin]|
    |AIRLINE BAG VINTAGE WORLD CHAMPION |[airline, bag, vintage, world, champion]  |
    |AIRLINE BAG VINTAGE JET SET BROWN  |[airline, bag, vintage, jet, set, brown]  |
    +-----------------------------------+------------------------------------------+
    ```

## 전처리 추정자 (Estimators)

![](https://learning.oreilly.com/library/view/spark-the-definitive/9781491912201/assets/spdg_2502.png)

- 입력 데이터에 따라 변환자를 적합시킨 후 변환 수행
- ex) StandardScaler
    ```scala
    import org.apache.spark.ml.feature.StandardScaler

    val ss = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("output")

    ss
      .fit(scaleDF)
      .transform(scaleDF)
      .show(false)
    ```
    ```text
    +---+--------------+------------------------------------------------------------+
    |id |features      |output                                                      |
    +---+--------------+------------------------------------------------------------+
    |0  |[1.0,0.1,-1.0]|[1.1952286093343936,0.02337622911060922,-0.5976143046671968]|
    |1  |[2.0,1.1,1.0] |[2.390457218668787,0.2571385202167014,0.5976143046671968]   |
    |0  |[1.0,0.1,-1.0]|[1.1952286093343936,0.02337622911060922,-0.5976143046671968]|
    |1  |[2.0,1.1,1.0] |[2.390457218668787,0.2571385202167014,0.5976143046671968]   |
    |1  |[3.0,10.1,3.0]|[3.5856858280031805,2.3609991401715313,1.7928429140015902]  |
    +---+--------------+------------------------------------------------------------+
    ```

### 1. 변환자 속성 정의하기

- 모든 변환자는 inputCol, outputCol 지정 필수 -> 지정 안할 시? inputCol은 에러 발생, outputCol은 해시값 조합으로 기본 지정됨
- setInputCol, setOutputCol 함수를 통해 지정
- inputCol, outputCol 이외에도 변환자를 조정할 수 있는 다양한 파라미터 존재

## 고수준 변환자

고수준 변환자는 하나의 변환에서 여러 가지 변환을 간결하게 지정 가능하므로 일반적인 오류 위험을 최소화할 수 있다.

### 1. RFormula

- R 언어에서 빌려온 변환자. 선언적으로 간편하게 변환 가능
- **one-hot encoding**을 통해 범주형(categorical) 변수 자동 처리
    - n개의 고유값을 가지는 범주형 변수를 0과 1로 변환하여 n개의 숫자형 변수로 다시 표현하는 방법
    - 더미변수화 or 가변수화
    - 범주형 변수: 이산적인 형태의 값을 가지는 변수, 범주 간에 순서가 있을 수도있고 없을 수도 있음 (https://debuglog.tistory.com/22)
- 숫자를 Double 타입으로 변환
- 레이블 컬럼이 String 타입일 경우 StringIndexer를 통해 Double 타입으로 변환
- 기본 연산자
    |연산자|설명|
    |:---|:---|
    |~|함수에서 타깃과 항을 분리|
    |+|연결기호. '+0'은 절편 제거를 의미|
    |-|삭제기호. '-1'은 절편 제거를 의미|
    |:|상호작용(수치형 값이나 이진화된 범주 값에 대한 곱셈)|
    |.|타깃/종속변수를 제외한 모든 컬럼|
- RFormula 사용 예제
    ```scala
    import org.apache.spark.ml.feature.RFormula

    val supervised = (new RFormula()
      .setFormula("lab ~ . + color:value1 + color:value2"))

    supervised
      .fit(simpleDF)
      .transform(simpleDF)
      .show(false)
    ```
    ```text
    +-----+----+------+------------------+----------------------------------------------------------------------+-----+
    |color|lab |value1|value2            |features                                                              |label|
    +-----+----+------+------------------+----------------------------------------------------------------------+-----+
    |green|good|1     |14.386294994851129|(10,[1,2,3,5,8],[1.0,1.0,14.386294994851129,1.0,14.386294994851129])  |1.0  |
    |blue |bad |8     |14.386294994851129|(10,[2,3,6,9],[8.0,14.386294994851129,8.0,14.386294994851129])        |0.0  |
    |blue |bad |12    |14.386294994851129|(10,[2,3,6,9],[12.0,14.386294994851129,12.0,14.386294994851129])      |0.0  |
    |green|good|15    |38.97187133755819 |(10,[1,2,3,5,8],[1.0,15.0,38.97187133755819,15.0,38.97187133755819])  |1.0  |
    |green|good|12    |14.386294994851129|(10,[1,2,3,5,8],[1.0,12.0,14.386294994851129,12.0,14.386294994851129])|1.0  |
    |green|bad |16    |14.386294994851129|(10,[1,2,3,5,8],[1.0,16.0,14.386294994851129,16.0,14.386294994851129])|0.0  |
    |red  |good|35    |14.386294994851129|(10,[0,2,3,4,7],[1.0,35.0,14.386294994851129,35.0,14.386294994851129])|1.0  |
    |red  |bad |1     |38.97187133755819 |(10,[0,2,3,4,7],[1.0,1.0,38.97187133755819,1.0,38.97187133755819])    |0.0  |
    |red  |bad |2     |14.386294994851129|(10,[0,2,3,4,7],[1.0,2.0,14.386294994851129,2.0,14.386294994851129])  |0.0  |
    |red  |bad |16    |14.386294994851129|(10,[0,2,3,4,7],[1.0,16.0,14.386294994851129,16.0,14.386294994851129])|0.0  |
    |red  |good|45    |38.97187133755819 |(10,[0,2,3,4,7],[1.0,45.0,38.97187133755819,45.0,38.97187133755819])  |1.0  |
    |green|good|1     |14.386294994851129|(10,[1,2,3,5,8],[1.0,1.0,14.386294994851129,1.0,14.386294994851129])  |1.0  |
    |blue |bad |8     |14.386294994851129|(10,[2,3,6,9],[8.0,14.386294994851129,8.0,14.386294994851129])        |0.0  |
    |blue |bad |12    |14.386294994851129|(10,[2,3,6,9],[12.0,14.386294994851129,12.0,14.386294994851129])      |0.0  |
    |green|good|15    |38.97187133755819 |(10,[1,2,3,5,8],[1.0,15.0,38.97187133755819,15.0,38.97187133755819])  |1.0  |
    |green|good|12    |14.386294994851129|(10,[1,2,3,5,8],[1.0,12.0,14.386294994851129,12.0,14.386294994851129])|1.0  |
    |green|bad |16    |14.386294994851129|(10,[1,2,3,5,8],[1.0,16.0,14.386294994851129,16.0,14.386294994851129])|0.0  |
    |red  |good|35    |14.386294994851129|(10,[0,2,3,4,7],[1.0,35.0,14.386294994851129,35.0,14.386294994851129])|1.0  |
    |red  |bad |1     |38.97187133755819 |(10,[0,2,3,4,7],[1.0,1.0,38.97187133755819,1.0,38.97187133755819])    |0.0  |
    |red  |bad |2     |14.386294994851129|(10,[0,2,3,4,7],[1.0,2.0,14.386294994851129,2.0,14.386294994851129])  |0.0  |
    +-----+----+------+------------------+----------------------------------------------------------------------+-----+
    ```

### 2. SQL 변환자 (SQLTransformer)

- 스파크의 SQL 처리 라이브러리 활용 가능
- SQL의 모든 SELECT 문과 동일하나, 테이블 이름 대신 `THIS` 키워드를 사용하는 차이점
- 하이퍼 파라미터 튜닝 시 특징에 서로 다른 SQL 적용하고자 할 때 유용
- 원시 데이터로부터 현재 상태까지의 조작을 변환자로 버전화 식별 가능
- SQLTransformer 사용 예제
    ```scala
    import org.apache.spark.ml.feature.SQLTransformer

    val basicTransformation = new SQLTransformer()
      .setStatement("""
          SELECT sum(Quantity), count(*), CustomerID
          FROM __THIS__
          GROUP BY CustomerID
      """)

    basicTransformation
      .transform(sales)
      .show()
    ```
    ```text
    +-------------+--------+----------+
    |sum(Quantity)|count(1)|CustomerID|
    +-------------+--------+----------+
    |          119|      62|   14452.0|
    |          440|     143|   16916.0|
    |          630|      72|   17633.0|
    |           34|       6|   14768.0|
    |         1542|      30|   13094.0|
    |          854|     117|   17884.0|
    |           97|      12|   16596.0|
    |          290|      98|   13607.0|
    |          541|      27|   14285.0|
    |          244|      31|   16561.0|
    |          491|     152|   13956.0|
    |          204|      76|   13533.0|
    |          493|      64|   16629.0|
    |          159|      38|   17267.0|
    |         1140|      30|   13918.0|
    |           55|      28|   18114.0|
    |           88|       7|   14473.0|
    |          150|      16|   14024.0|
    |          206|      23|   12493.0|
    |          138|      18|   15776.0|
    +-------------+--------+----------+
    ```

### 3. 벡터 조합기 (VectorAssembler)

- 모든 특징을 하나의 벡터로 연결하여 추정자에 전달
- 보통 머신러닝 파이프라인의 마지막 단계에서 사용
- Boolean, Double, Vector와 같은 컬럼을 입력으로 사용
- VectorAssembler 사용 예제
    ```scala
    import org.apache.spark.ml.feature.VectorAssembler

    val va = new VectorAssembler()
      .setInputCols(Array("int1", "int2", "int3"))
      .setOutputCol("output")

    va
      .transform(fakeIntDF)
      .show()
    ```
    ```text
    +----+----+----+-------------+
    |int1|int2|int3|       output|
    +----+----+----+-------------+
    |   7|   8|   9|[7.0,8.0,9.0]|
    |   1|   2|   3|[1.0,2.0,3.0]|
    |   4|   5|   6|[4.0,5.0,6.0]|
    +----+----+----+-------------+
    ```

## 연속형 특징 처리하기

- 양의 무한대부터 음의 무한대까지의 숫자값
- 버켓팅을 통한 연속형 -> 범주형 특징 변환 가능
- Double 타입의 특징 스케일링 or 정규화 (Double 타입으로 숫자 맞춰줄 것)
    ```scala
    val confDF = spark.range(20).selectrExpr("case(id as double)")
    ```

### 1. 버켓팅

- 구간화(binning) -> 히스토그램
- 기본적으로 Bucketizer를 사용
- 버켓팅은 가능한 모든 입력 범위를 수용하지 못하므로 경계를 설정해야함
- 버켓 포인트(분할 배열) 지정할 시 요구사항
    1. min(분할 배열) < min(DataFrame)
    2. max(분할 배열) > max(DataFrame)
    3. count(분할 배열) >= 3
- 가능한 모든 범위를 포함하기 위한 방법: `scala.Double.NegativeInfinity`와 `scala.Double.positiveInfinity` 사용
- null, NaN 값 처리하기 위한 방법: handleInvalid 파라미터 지정 (keep, error, skip 가능)
- Bucketizer 사용 예제
    ```scala
    import org.apache.spark.ml.feature.Bucketizer

    val bucketBorders = Array(-1.0, 5.0, 10.0, 250.0, 600.0)
    val bucketer = new Bucketizer()
      .setSplits(bucketBorders)
      .setInputCol("id")
      .setOutputCol("output")

    bucketer
      .transform(contDF)
      .show()
    ```
    ```text
    +----+------+
    |  id|output|
    +----+------+
    | 0.0|   0.0|
    | 1.0|   0.0|
    | 2.0|   0.0|
    | 3.0|   0.0|
    | 4.0|   0.0|
    | 5.0|   1.0|
    | 6.0|   1.0|
    | 7.0|   1.0|
    | 8.0|   1.0|
    | 9.0|   1.0|
    |10.0|   2.0|
    |11.0|   2.0|
    |12.0|   2.0|
    |13.0|   2.0|
    |14.0|   2.0|
    |15.0|   2.0|
    |16.0|   2.0|
    |17.0|   2.0|
    |18.0|   2.0|
    |19.0|   2.0|
    +----+------+
    ```
- QuantileDiscretizer: 백분위수를 기준으로 분할 ex) 90번째 백분위수는 전체 데이터의 90%가 해당 값보다 작은 데이터의 지점
- setRelativeError로 근사치 계산의 상대적 오류를 설정할 수 있음
- QuantileDiscretizer 사용 예제
    ```scala
    import org.apache.spark.ml.feature.QuantileDiscretizer

    val bucketer = new QuantileDiscretizer()
      .setNumBuckets(5)
      .setInputCol("id")
      .setOutputCol("output")

    bucketer
      .fit(contDF)
      .transform(contDF)
      .show()
    ```
    ```text
    +----+------+
    |  id|output|
    +----+------+
    | 0.0|   0.0|
    | 1.0|   0.0|
    | 2.0|   0.0|
    | 3.0|   1.0|
    | 4.0|   1.0|
    | 5.0|   1.0|
    | 6.0|   1.0|
    | 7.0|   2.0|
    | 8.0|   2.0|
    | 9.0|   2.0|
    |10.0|   2.0|
    |11.0|   2.0|
    |12.0|   3.0|
    |13.0|   3.0|
    |14.0|   3.0|
    |15.0|   4.0|
    |16.0|   4.0|
    |17.0|   4.0|
    |18.0|   4.0|
    |19.0|   4.0|
    +----+------+
    ```

#### 1.1. 고급 버켓팅 기술

- 이외에도 버켓팅 계산 알고리즘에 따라 다양한 방법 존재
- 지역성 기반 해싱 (locality sensitivityhasing, LSH)

### 2. 스케일링과 정규화

- 버켓팅과 함께 주로 사용하는 작업으로 스케일링, 정규화
- 스케일링: 각 컬럼의 단위가 다를 경우 이를 맞추는 작업 -> 히스토그램 y축 조정
- 정규화를 통해 컬럼의 평균값 기준으로 조정 -> 히스토그램 수평이동

### 3. StandardScaler

- 특징들이 평균=0, 표준편차=1인 분포를 갖도록 만듬
- 표준정규분포
- withStd: 표준편차를 1로 조정 (기본값: true)
- withMean: 평균을 0으로 조정 (기본값: false)
- StandardScaler 사용 예제
    ```scala
    import org.apache.spark.ml.feature.StandardScaler

    val sScaler = new StandardScaler()
      //.setWithMean(true)
      .setInputCol("features")
      .setOutputCol("output")

    sScaler
      .fit(scaleDF)
      .transform(scaleDF)
      .show(false)
    ```
    ```text
    +---+--------------+------------------------------------------------------------+
    |id |features      |output                                                      |
    +---+--------------+------------------------------------------------------------+
    |0  |[1.0,0.1,-1.0]|[1.1952286093343936,0.02337622911060922,-0.5976143046671968]|
    |1  |[2.0,1.1,1.0] |[2.390457218668787,0.2571385202167014,0.5976143046671968]   |
    |0  |[1.0,0.1,-1.0]|[1.1952286093343936,0.02337622911060922,-0.5976143046671968]|
    |1  |[2.0,1.1,1.0] |[2.390457218668787,0.2571385202167014,0.5976143046671968]   |
    |1  |[3.0,10.1,3.0]|[3.5856858280031805,2.3609991401715313,1.7928429140015902]  |
    +---+--------------+------------------------------------------------------------+
    ```

#### 3.1. MinMaxScaler

- 최솟값(Min)과 최댓값(Max)를 지정하여 그 사이의 값으로 비례하여 스케일링
- MinMaxScaler 사용 예제
    ```scala
    import org.apache.spark.ml.feature.MinMaxScaler

    val minMax = new MinMaxScaler()
      .setMin(5)
      .setMax(10)
      .setInputCol("features")
      .setOutputCol("output")

    minMax
      .fit(scaleDF)
      .transform(scaleDF)
      .show()
    ```
    ```text
    +---+--------------+----------------+
    | id|      features|          output|
    +---+--------------+----------------+
    |  0|[1.0,0.1,-1.0]|   [5.0,5.0,5.0]|
    |  1| [2.0,1.1,1.0]|   [7.5,5.5,7.5]|
    |  0|[1.0,0.1,-1.0]|   [5.0,5.0,5.0]|
    |  1| [2.0,1.1,1.0]|   [7.5,5.5,7.5]|
    |  1|[3.0,10.1,3.0]|[10.0,10.0,10.0]|
    +---+--------------+----------------+
    ```

#### 3.2. MaxAbsScaler

- 컬럼의 최대 절댓값으로 각 데이터를 나누어서 스케일링
- 모든 값은 -1과 1 사이에 위치하게 됨
- MaxAbsScaler 사용 예제
    ```scala
    import org.apache.spark.ml.feature.MaxAbsScaler

    val maScaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("output")

    maScaler
      .fit(scaleDF)
      .transform(scaleDF)
      .show(false)
    ```
    ```text
    +---+--------------+-------------------------------------------------------------+
    |id |features      |output                                                       |
    +---+--------------+-------------------------------------------------------------+
    |0  |[1.0,0.1,-1.0]|[0.3333333333333333,0.009900990099009901,-0.3333333333333333]|
    |1  |[2.0,1.1,1.0] |[0.6666666666666666,0.10891089108910892,0.3333333333333333]  |
    |0  |[1.0,0.1,-1.0]|[0.3333333333333333,0.009900990099009901,-0.3333333333333333]|
    |1  |[2.0,1.1,1.0] |[0.6666666666666666,0.10891089108910892,0.3333333333333333]  |
    |1  |[3.0,10.1,3.0]|[1.0,1.0,1.0]                                                |
    +---+--------------+-------------------------------------------------------------+
    ```

#### 3.3. ElementwiseProduct

- 벡터의 각 값을 임의의 값으로 조정 (곱셈연산)
- 스케일링을 적용할 대상 벡터와 조정값 벡터의 차원은 일치해야 함
- ElementwiseProduct 사용 예제
    ```scala
    import org.apache.spark.ml.feature.ElementwiseProduct
    import org.apache.spark.ml.linalg.Vectors

    val scaleUpVec = Vectors.dense(10.0, 15.0, 20.0)
    val scalingUp = new ElementwiseProduct()
      .setScalingVec(scaleUpVec)
      .setInputCol("features")
      .setOutputCol("output")

    scalingUp
      .transform(scaleDF)
      .show()
    ```
    ```text
    +---+--------------+-----------------+
    | id|      features|           output|
    +---+--------------+-----------------+
    |  0|[1.0,0.1,-1.0]| [10.0,1.5,-20.0]|
    |  1| [2.0,1.1,1.0]| [20.0,16.5,20.0]|
    |  0|[1.0,0.1,-1.0]| [10.0,1.5,-20.0]|
    |  1| [2.0,1.1,1.0]| [20.0,16.5,20.0]|
    |  1|[3.0,10.1,3.0]|[30.0,151.5,60.0]|
    +---+--------------+-----------------+
    ```

#### 3.4. Normalizer

- 여러 가지 거리 표준 중 하나를 사용해 다차원 벡터 스케일링
- setP: p 파라미터로 표준 지정
    1. p=1: 맨해튼 표준 (맨해튼 거리)
    2. p=2: 유클리드 표준 (유클리드 거리), 기본값
- Normalizer 사용 예제
    ```scala
    import org.apache.spark.ml.feature.Normalizer

    val manhattanDistance = new Normalizer()
      .setP(1)
      .setInputCol("features")
      .setOutputCol("output")

    manhattanDistance
      .transform(scaleDF)
      .show()
    ```
    ```text
    +---+--------------+---------------------------------------------------------------+
    |id |features      |output                                                         |
    +---+--------------+---------------------------------------------------------------+
    |0  |[1.0,0.1,-1.0]|[0.47619047619047616,0.047619047619047616,-0.47619047619047616]|
    |1  |[2.0,1.1,1.0] |[0.48780487804878053,0.26829268292682934,0.24390243902439027]  |
    |0  |[1.0,0.1,-1.0]|[0.47619047619047616,0.047619047619047616,-0.47619047619047616]|
    |1  |[2.0,1.1,1.0] |[0.48780487804878053,0.26829268292682934,0.24390243902439027]  |
    |1  |[3.0,10.1,3.0]|[0.18633540372670807,0.6273291925465838,0.18633540372670807]   |
    +---+--------------+---------------------------------------------------------------+
    ```

## 범주형 특징 처리하기

- 범주형 특징의 가장 일반적인 작업은 인덱싱하는 것: 범주형 변수 -> 숫자형 변수 (머신러닝 적용을 위해)
- 인코딩 변경 이슈 등 일관성을 유지하기 위해서는 모든 범주형 변수의 색인을 다시 생성하는 것이 좋다

### 1. StringIndexer

- 문자열을 숫자 ID에 매핑
- DataFrame의 메타데이터 통해 입력과 출력 정보 저장하여 나중에 색인값에서 입력값을 다시 가져올 수 있음
- fit 과정에서 관측되지 않은 값이 입력으로 들어올 경우 처리 케이스
    1. handleInvalid=error: 예외 처리
    2. handleInvalid=skip: 해당 로우 건너뛰고 처리
    3. handleInvalid=keep: 해당 입력값에 대해 추가적인 index 매핑
- 인덱스 순서: 빈도가 높은 순서대로 0 부터 증가
- StringIndexer 사용 예제
    ```scala
    import org.apache.spark.ml.feature.StringIndexer

    val indexer = new StringIndexer()
      .setInputCol("lab")
    //  .setInputCol("value1")      // 문자열이 아닌 컬럼에도 적용 가능. 이 경우 문자열로 변환 후 색인 생성
      .setOutputCol("labelInd")
    //  .setHnadleInvalid("skip")     // keep, error, skip

    val indexRes = indexer
      .fit(simpleDF)
      .transform(simpleDF)

    indexRes.show()
    ```
    ```text
    +-----+----+------+------------------+--------+
    |color| lab|value1|            value2|labelInd|
    +-----+----+------+------------------+--------+
    |green|good|     1|14.386294994851129|     1.0|
    | blue| bad|     8|14.386294994851129|     0.0|
    | blue| bad|    12|14.386294994851129|     0.0|
    |green|good|    15| 38.97187133755819|     1.0|
    |green|good|    12|14.386294994851129|     1.0|
    |green| bad|    16|14.386294994851129|     0.0|
    |  red|good|    35|14.386294994851129|     1.0|
    |  red| bad|     1| 38.97187133755819|     0.0|
    |  red| bad|     2|14.386294994851129|     0.0|
    |  red| bad|    16|14.386294994851129|     0.0|
    |  red|good|    45| 38.97187133755819|     1.0|
    |green|good|     1|14.386294994851129|     1.0|
    | blue| bad|     8|14.386294994851129|     0.0|
    | blue| bad|    12|14.386294994851129|     0.0|
    |green|good|    15| 38.97187133755819|     1.0|
    |green|good|    12|14.386294994851129|     1.0|
    |green| bad|    16|14.386294994851129|     0.0|
    |  red|good|    35|14.386294994851129|     1.0|
    |  red| bad|     1| 38.97187133755819|     0.0|
    |  red| bad|     2|14.386294994851129|     0.0|
    +-----+----+------+------------------+--------+
    ```

### 2. 색인된 값을 텍스트로 변환하기 (IndexToString)

- StringIndexer의 반대 동작을 수행 (색인 -> 문자열 변환)
- DataFrame에 메타데이터를 유지하므로 별도의 키 값 필요 없음
- IndexToString 사용 예제
    ```scala
    import org.apache.spark.ml.feature.IndexToString

    val labelReverse = new IndexToString()
      .setInputCol("labelInd")
      .setOutputCol("labelIndToString")

    labelReverse
      .transform(indexRes)
      .show()
    ```
    ```text
    +-----+----+------+------------------+--------+----------------+
    |color| lab|value1|            value2|labelInd|labelIndToString|
    +-----+----+------+------------------+--------+----------------+
    |green|good|     1|14.386294994851129|     1.0|            good|
    | blue| bad|     8|14.386294994851129|     0.0|             bad|
    | blue| bad|    12|14.386294994851129|     0.0|             bad|
    |green|good|    15| 38.97187133755819|     1.0|            good|
    |green|good|    12|14.386294994851129|     1.0|            good|
    |green| bad|    16|14.386294994851129|     0.0|             bad|
    |  red|good|    35|14.386294994851129|     1.0|            good|
    |  red| bad|     1| 38.97187133755819|     0.0|             bad|
    |  red| bad|     2|14.386294994851129|     0.0|             bad|
    |  red| bad|    16|14.386294994851129|     0.0|             bad|
    |  red|good|    45| 38.97187133755819|     1.0|            good|
    |green|good|     1|14.386294994851129|     1.0|            good|
    | blue| bad|     8|14.386294994851129|     0.0|             bad|
    | blue| bad|    12|14.386294994851129|     0.0|             bad|
    |green|good|    15| 38.97187133755819|     1.0|            good|
    |green|good|    12|14.386294994851129|     1.0|            good|
    |green| bad|    16|14.386294994851129|     0.0|             bad|
    |  red|good|    35|14.386294994851129|     1.0|            good|
    |  red| bad|     1| 38.97187133755819|     0.0|             bad|
    |  red| bad|     2|14.386294994851129|     0.0|             bad|
    +-----+----+------+------------------+--------+----------------+
    ```

### 3. 벡터 인덱싱하기 (VectorIndexer)

- 벡터 내에 존재하는 범주형 변수를 인덱싱하는 도구
- maxCategories: 설정값 이하의 고유한 값을 가진 벡터의 모든 컬럼을 범주형 변수로 변환
- 특정 값을 지정하면 그에 따라 자동으로 색인하는 방식이므로 사전에 데이터 내 가장 큰 카테고리가 무엇인지, 고유 값이 몇 개인지 알고 있을 경우 유용함
- VectorIndexer 사용 예제
    ```scala
    import org.apache.spark.ml.feature.VectorIndexer
    import org.apache.spark.ml.linalg.Vectors

    val seq = Seq(
      (Vectors.dense(1, 2, 3), 1),
      (Vectors.dense(2, 5, 6), 2),
      (Vectors.dense(1, 8, 9), 3)
    )
    val idxIn = spark.createDataFrame(seq).toDF("features", "label")
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("featuresInd")
      .setMaxCategories(2)

    indexer
      .fit(idxIn)
      .transform(idxIn)
      .show()
    ```
    ```text
    +-------------+-----+-------------+
    |     features|label|  featuresInd|
    +-------------+-----+-------------+
    |[1.0,2.0,3.0]|    1|[0.0,2.0,3.0]|
    |[2.0,5.0,6.0]|    2|[1.0,5.0,6.0]|
    |[1.0,8.0,9.0]|    3|[0.0,8.0,9.0]|
    +-------------+-----+-------------+
    ```

### 4. 원-핫 인코딩 (OneHotEncoder)

- 범주형 변수를 인덱싱한 후 추가적으로 수행하는 보편적 기법
- 인덱스의 크기가 의미를 가지지 않을 경우, 인덱스를 boolean 타입의 벡터 구성 요소로 변환
- 예를 들어, 색상 값을 인코딩하면 더이상 정렬되지 않음 -> 다운스트림 모델 (ex. 선형 모델)이 처리하기 쉬움
- OneHotEncoder 사용 예제
    ```scala
    import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder}

    val indexer = new StringIndexer()
      .setInputCol("color")
      .setOutputCol("colorInd")

    val colorLab = indexer
      .fit(simpleDF)
      .transform(simpleDF.select("color"))

    val ohe = new OneHotEncoder()
      .setInputCol("colorInd")
      .setOutputCol("colorIndOneHotEncoded")

    ohe
      .transform(colorLab)
      .show()
    ```
    ```text
    +-----+--------+---------------------+
    |color|colorInd|colorIndOneHotEncoded|
    +-----+--------+---------------------+
    |green|     1.0|        (2,[1],[1.0])|
    | blue|     2.0|            (2,[],[])|
    | blue|     2.0|            (2,[],[])|
    |green|     1.0|        (2,[1],[1.0])|
    |green|     1.0|        (2,[1],[1.0])|
    |green|     1.0|        (2,[1],[1.0])|
    |  red|     0.0|        (2,[0],[1.0])|
    |  red|     0.0|        (2,[0],[1.0])|
    |  red|     0.0|        (2,[0],[1.0])|
    |  red|     0.0|        (2,[0],[1.0])|
    |  red|     0.0|        (2,[0],[1.0])|
    |green|     1.0|        (2,[1],[1.0])|
    | blue|     2.0|            (2,[],[])|
    | blue|     2.0|            (2,[],[])|
    |green|     1.0|        (2,[1],[1.0])|
    |green|     1.0|        (2,[1],[1.0])|
    |green|     1.0|        (2,[1],[1.0])|
    |  red|     0.0|        (2,[0],[1.0])|
    |  red|     0.0|        (2,[0],[1.0])|
    |  red|     0.0|        (2,[0],[1.0])|
    +-----+--------+---------------------+
    ```

## 텍스트 데이터 변환자

- 텍스트 데이터 유형: 자유형 텍스트, 문자열 범주형 변수

### 1. 텍스트 토큰화하기

- 토큰화: 자유형 텍스트를 토큰 단위로 변환
- Tokenizer, RegexTokenizer 사용
- Tokenizer: 단어 사이의 공백을 기준으로 구분하여 토큰 배열 변환
- RegexTokenizer: 정규 표현식을 기준으로 구분하여 토큰 배열 변환, Java의 정규 표현식 구문 사용
- Tokenizer 사용 예제
    ```scala
    import org.apache.spark.ml.feature.Tokenizer

    val tkn = new Tokenizer()
      .setInputCol("Description")
      .setOutputCol("DescOut")
    val tokenized = tkn.transform(sales.select("Description"))

    tokenized.show(false)
    ```
    ```text
    +-----------------------------------+------------------------------------------+
    |Description                        |DescOut                                   |
    +-----------------------------------+------------------------------------------+
    |RABBIT NIGHT LIGHT                 |[rabbit, night, light]                    |
    |DOUGHNUT LIP GLOSS                 |[doughnut, lip, gloss]                    |
    |12 MESSAGE CARDS WITH ENVELOPES    |[12, message, cards, with, envelopes]     |
    |BLUE HARMONICA IN BOX              |[blue, harmonica, in, box]                |
    |GUMBALL COAT RACK                  |[gumball, coat, rack]                     |
    |SKULLS  WATER TRANSFER TATTOOS     |[skulls, , water, transfer, tattoos]      |
    |FELTCRAFT GIRL AMELIE KIT          |[feltcraft, girl, amelie, kit]            |
    |CAMOUFLAGE LED TORCH               |[camouflage, led, torch]                  |
    |WHITE SKULL HOT WATER BOTTLE       |[white, skull, hot, water, bottle]        |
    |ENGLISH ROSE HOT WATER BOTTLE      |[english, rose, hot, water, bottle]       |
    |HOT WATER BOTTLE KEEP CALM         |[hot, water, bottle, keep, calm]          |
    |SCOTTIE DOG HOT WATER BOTTLE       |[scottie, dog, hot, water, bottle]        |
    |ROSE CARAVAN DOORSTOP              |[rose, caravan, doorstop]                 |
    |GINGHAM HEART  DOORSTOP RED        |[gingham, heart, , doorstop, red]         |
    |STORAGE TIN VINTAGE LEAF           |[storage, tin, vintage, leaf]             |
    |SET OF 4 KNICK KNACK TINS POPPIES  |[set, of, 4, knick, knack, tins, poppies] |
    |POPCORN HOLDER                     |[popcorn, holder]                         |
    |GROW A FLYTRAP OR SUNFLOWER IN TIN |[grow, a, flytrap, or, sunflower, in, tin]|
    |AIRLINE BAG VINTAGE WORLD CHAMPION |[airline, bag, vintage, world, champion]  |
    |AIRLINE BAG VINTAGE JET SET BROWN  |[airline, bag, vintage, jet, set, brown]  |
    +-----------------------------------+------------------------------------------+
    ```
- RegexTokenizer 사용 예제
    ```scala
    import org.apache.spark.ml.feature.RegexTokenizer

    val rtkn = new RegexTokenizer()
      .setInputCol("Description")
      .setOutputCol("DescOut")
      .setPattern(" ") // 가장 단순한 표현, 정규 표현식 패턴 입력 가능
      //.setPattern("a")
      //.setGaps(false)
      .setToLowercase(true)

    rtkn
      .transform(sales.select("Description"))
      .show(false)
    ```
    ```text
    +-----------------------------------+------------------------------------------+
    |Description                        |DescOut                                   |
    +-----------------------------------+------------------------------------------+
    |RABBIT NIGHT LIGHT                 |[rabbit, night, light]                    |
    |DOUGHNUT LIP GLOSS                 |[doughnut, lip, gloss]                    |
    |12 MESSAGE CARDS WITH ENVELOPES    |[12, message, cards, with, envelopes]     |
    |BLUE HARMONICA IN BOX              |[blue, harmonica, in, box]                |
    |GUMBALL COAT RACK                  |[gumball, coat, rack]                     |
    |SKULLS  WATER TRANSFER TATTOOS     |[skulls, water, transfer, tattoos]        |
    |FELTCRAFT GIRL AMELIE KIT          |[feltcraft, girl, amelie, kit]            |
    |CAMOUFLAGE LED TORCH               |[camouflage, led, torch]                  |
    |WHITE SKULL HOT WATER BOTTLE       |[white, skull, hot, water, bottle]        |
    |ENGLISH ROSE HOT WATER BOTTLE      |[english, rose, hot, water, bottle]       |
    |HOT WATER BOTTLE KEEP CALM         |[hot, water, bottle, keep, calm]          |
    |SCOTTIE DOG HOT WATER BOTTLE       |[scottie, dog, hot, water, bottle]        |
    |ROSE CARAVAN DOORSTOP              |[rose, caravan, doorstop]                 |
    |GINGHAM HEART  DOORSTOP RED        |[gingham, heart, doorstop, red]           |
    |STORAGE TIN VINTAGE LEAF           |[storage, tin, vintage, leaf]             |
    |SET OF 4 KNICK KNACK TINS POPPIES  |[set, of, 4, knick, knack, tins, poppies] |
    |POPCORN HOLDER                     |[popcorn, holder]                         |
    |GROW A FLYTRAP OR SUNFLOWER IN TIN |[grow, a, flytrap, or, sunflower, in, tin]|
    |AIRLINE BAG VINTAGE WORLD CHAMPION |[airline, bag, vintage, world, champion]  |
    |AIRLINE BAG VINTAGE JET SET BROWN  |[airline, bag, vintage, jet, set, brown]  |
    +-----------------------------------+------------------------------------------+
    ```

### 2. 일반적인 단어 제어하기 (StopWordsRemover)

- 토큰화 이후 일반적으로 불용어, 무의미한 용어 필터링 작업 수행
- ex) the, and, but, ...
- StopWordsRemover 사용 예제
    ```scala
    import org.apache.spark.ml.feature.StopWordsRemover

    // 불용어 지원 목록 (spark 2.4 기준)
    // "danish", "dutch", "english", "finnish", "french", "german", "hungarian", "italian", "norwegian", "portuguese", "russian", "spanish", "swedish" and "turkish"
    // korean은 따로 없는 듯
    val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
    val stops = new StopWordsRemover()
      .setStopWords(englishStopWords)
      .setInputCol("DescOut")
      .setOutputCol("DescOutStopWordsRemoved")

    stops
      .transform(tokenized)
      .show(false)
    ```
    ```text
    +-----------------------------------+------------------------------------------+----------------------------------------+
    |Description                        |DescOut                                   |DescOutStopWordsRemoved                 |
    +-----------------------------------+------------------------------------------+----------------------------------------+
    |RABBIT NIGHT LIGHT                 |[rabbit, night, light]                    |[rabbit, night, light]                  |
    |DOUGHNUT LIP GLOSS                 |[doughnut, lip, gloss]                    |[doughnut, lip, gloss]                  |
    |12 MESSAGE CARDS WITH ENVELOPES    |[12, message, cards, with, envelopes]     |[12, message, cards, envelopes]         |
    |BLUE HARMONICA IN BOX              |[blue, harmonica, in, box]                |[blue, harmonica, box]                  |
    |GUMBALL COAT RACK                  |[gumball, coat, rack]                     |[gumball, coat, rack]                   |
    |SKULLS  WATER TRANSFER TATTOOS     |[skulls, , water, transfer, tattoos]      |[skulls, , water, transfer, tattoos]    |
    |FELTCRAFT GIRL AMELIE KIT          |[feltcraft, girl, amelie, kit]            |[feltcraft, girl, amelie, kit]          |
    |CAMOUFLAGE LED TORCH               |[camouflage, led, torch]                  |[camouflage, led, torch]                |
    |WHITE SKULL HOT WATER BOTTLE       |[white, skull, hot, water, bottle]        |[white, skull, hot, water, bottle]      |
    |ENGLISH ROSE HOT WATER BOTTLE      |[english, rose, hot, water, bottle]       |[english, rose, hot, water, bottle]     |
    |HOT WATER BOTTLE KEEP CALM         |[hot, water, bottle, keep, calm]          |[hot, water, bottle, keep, calm]        |
    |SCOTTIE DOG HOT WATER BOTTLE       |[scottie, dog, hot, water, bottle]        |[scottie, dog, hot, water, bottle]      |
    |ROSE CARAVAN DOORSTOP              |[rose, caravan, doorstop]                 |[rose, caravan, doorstop]               |
    |GINGHAM HEART  DOORSTOP RED        |[gingham, heart, , doorstop, red]         |[gingham, heart, , doorstop, red]       |
    |STORAGE TIN VINTAGE LEAF           |[storage, tin, vintage, leaf]             |[storage, tin, vintage, leaf]           |
    |SET OF 4 KNICK KNACK TINS POPPIES  |[set, of, 4, knick, knack, tins, poppies] |[set, 4, knick, knack, tins, poppies]   |
    |POPCORN HOLDER                     |[popcorn, holder]                         |[popcorn, holder]                       |
    |GROW A FLYTRAP OR SUNFLOWER IN TIN |[grow, a, flytrap, or, sunflower, in, tin]|[grow, flytrap, sunflower, tin]         |
    |AIRLINE BAG VINTAGE WORLD CHAMPION |[airline, bag, vintage, world, champion]  |[airline, bag, vintage, world, champion]|
    |AIRLINE BAG VINTAGE JET SET BROWN  |[airline, bag, vintage, jet, set, brown]  |[airline, bag, vintage, jet, set, brown]|
    +-----------------------------------+------------------------------------------+----------------------------------------+
    ```

### 3. 단어 조합 만들기

- 토큰화, 불용어 제거가 끝난 단어 배열에서 조합을 통해 n-gram 생성 가능
- n-gram: 길이가 n인 단어의 시퀀스 (unigram, bigram, ...)
- ex) 'Big Data Processing Made Simple'
    - bigram: 'Big Data', 'Data Processing', 'Processing Made', 'Made Simple'
    - trigram: 'Big Data Processing', 'Data Processing Made', 'Processing Made Simple'
- n-gram을 통해 문장의 구조와 정보를 더 정확히 포착 가능함 -> 머신러닝 성능 향상
- NGram 사용 예제
    ```scala
    import org.apache.spark.ml.feature.NGram

    val unigram = new NGram().setInputCol("DescOut").setOutputCol("DescOutUnigram").setN(1)
    val bigram = new NGram().setInputCol("DescOut").setOutputCol("DescOutTrigram").setN(2)

    unigram.transform(tokenized.select("DescOut")).show(false)
    bigram.transform(tokenized.select("DescOut")).show(false)
    ```
    ```text
    +------------------------------------------+------------------------------------------+
    |DescOut                                   |DescOutUnigram                            |
    +------------------------------------------+------------------------------------------+
    |[rabbit, night, light]                    |[rabbit, night, light]                    |
    |[doughnut, lip, gloss]                    |[doughnut, lip, gloss]                    |
    |[12, message, cards, with, envelopes]     |[12, message, cards, with, envelopes]     |
    |[blue, harmonica, in, box]                |[blue, harmonica, in, box]                |
    |[gumball, coat, rack]                     |[gumball, coat, rack]                     |
    |[skulls, , water, transfer, tattoos]      |[skulls, , water, transfer, tattoos]      |
    |[feltcraft, girl, amelie, kit]            |[feltcraft, girl, amelie, kit]            |
    |[camouflage, led, torch]                  |[camouflage, led, torch]                  |
    |[white, skull, hot, water, bottle]        |[white, skull, hot, water, bottle]        |
    |[english, rose, hot, water, bottle]       |[english, rose, hot, water, bottle]       |
    |[hot, water, bottle, keep, calm]          |[hot, water, bottle, keep, calm]          |
    |[scottie, dog, hot, water, bottle]        |[scottie, dog, hot, water, bottle]        |
    |[rose, caravan, doorstop]                 |[rose, caravan, doorstop]                 |
    |[gingham, heart, , doorstop, red]         |[gingham, heart, , doorstop, red]         |
    |[storage, tin, vintage, leaf]             |[storage, tin, vintage, leaf]             |
    |[set, of, 4, knick, knack, tins, poppies] |[set, of, 4, knick, knack, tins, poppies] |
    |[popcorn, holder]                         |[popcorn, holder]                         |
    |[grow, a, flytrap, or, sunflower, in, tin]|[grow, a, flytrap, or, sunflower, in, tin]|
    |[airline, bag, vintage, world, champion]  |[airline, bag, vintage, world, champion]  |
    |[airline, bag, vintage, jet, set, brown]  |[airline, bag, vintage, jet, set, brown]  |
    +------------------------------------------+------------------------------------------+
    ```
    ```text
    +------------------------------------------+-------------------------------------------------------------------+
    |DescOut                                   |DescOutTrigram                                                     |
    +------------------------------------------+-------------------------------------------------------------------+
    |[rabbit, night, light]                    |[rabbit night, night light]                                        |
    |[doughnut, lip, gloss]                    |[doughnut lip, lip gloss]                                          |
    |[12, message, cards, with, envelopes]     |[12 message, message cards, cards with, with envelopes]            |
    |[blue, harmonica, in, box]                |[blue harmonica, harmonica in, in box]                             |
    |[gumball, coat, rack]                     |[gumball coat, coat rack]                                          |
    |[skulls, , water, transfer, tattoos]      |[skulls ,  water, water transfer, transfer tattoos]                |
    |[feltcraft, girl, amelie, kit]            |[feltcraft girl, girl amelie, amelie kit]                          |
    |[camouflage, led, torch]                  |[camouflage led, led torch]                                        |
    |[white, skull, hot, water, bottle]        |[white skull, skull hot, hot water, water bottle]                  |
    |[english, rose, hot, water, bottle]       |[english rose, rose hot, hot water, water bottle]                  |
    |[hot, water, bottle, keep, calm]          |[hot water, water bottle, bottle keep, keep calm]                  |
    |[scottie, dog, hot, water, bottle]        |[scottie dog, dog hot, hot water, water bottle]                    |
    |[rose, caravan, doorstop]                 |[rose caravan, caravan doorstop]                                   |
    |[gingham, heart, , doorstop, red]         |[gingham heart, heart ,  doorstop, doorstop red]                   |
    |[storage, tin, vintage, leaf]             |[storage tin, tin vintage, vintage leaf]                           |
    |[set, of, 4, knick, knack, tins, poppies] |[set of, of 4, 4 knick, knick knack, knack tins, tins poppies]     |
    |[popcorn, holder]                         |[popcorn holder]                                                   |
    |[grow, a, flytrap, or, sunflower, in, tin]|[grow a, a flytrap, flytrap or, or sunflower, sunflower in, in tin]|
    |[airline, bag, vintage, world, champion]  |[airline bag, bag vintage, vintage world, world champion]          |
    |[airline, bag, vintage, jet, set, brown]  |[airline bag, bag vintage, vintage jet, jet set, set brown]        |
    +------------------------------------------+-------------------------------------------------------------------+
    ```

### 4. 단어를 숫자로 변환하기

- 단어 특징을 모델에서 사용하기 위해서는 갯수, 발생 빈도 등을 알아야 함 -> CountVectorizer 사용
- CountVectorizer 과정
    1. 모델을 적합하여 모든 문서에서 출현한 단어 집합 및 출현 빈도를 계산
    2. 변환 과정에 각 로우(문서)에서 주어진 단어의 발생 빈도를 계산하고 해당 로우에 포함된 용어를 벡터 형태로 반환
- CountVectorizer 개념
    - 모든 로우를 문서(document)로 취급
    - 모든 단어를 용어(term)으로 취급
    - 모든 단어의 집합을 어휘집(vocabulary)로 취급
    - 각 요소는 조정 가능한 파라미터 -> 즉, minTF, minDF, vocabSize 설정 가능하다
- CountVectorizer 사용 예제
    ```scala
    import org.apache.spark.ml.feature.CountVectorizer

    val cv = new CountVectorizer()
      .setInputCol("DescOut")
      .setOutputCol("countVec")
      .setVocabSize(500)
      .setMinTF(1)
      .setMinDF(2)
    val fittedCV = cv.fit(tokenized)

    // countVec: 순서대로 총 어휘 크기, 어휘에 포함된 단어 색인, 단어의 출현 빈도
    fittedCV.transform(tokenized).show(false)
    ```
    ```text
    +-----------------------------------+------------------------------------------+---------------------------------------------------+
    |Description                        |DescOut                                   |countVec                                           |
    +-----------------------------------+------------------------------------------+---------------------------------------------------+
    |RABBIT NIGHT LIGHT                 |[rabbit, night, light]                    |(500,[149,185,212],[1.0,1.0,1.0])                  |
    |DOUGHNUT LIP GLOSS                 |[doughnut, lip, gloss]                    |(500,[462,463,491],[1.0,1.0,1.0])                  |
    |12 MESSAGE CARDS WITH ENVELOPES    |[12, message, cards, with, envelopes]     |(500,[35,41,166],[1.0,1.0,1.0])                    |
    |BLUE HARMONICA IN BOX              |[blue, harmonica, in, box]                |(500,[10,16,36,352],[1.0,1.0,1.0,1.0])             |
    |GUMBALL COAT RACK                  |[gumball, coat, rack]                     |(500,[228,281,408],[1.0,1.0,1.0])                  |
    |SKULLS  WATER TRANSFER TATTOOS     |[skulls, , water, transfer, tattoos]      |(500,[11,40,133],[1.0,1.0,1.0])                    |
    |FELTCRAFT GIRL AMELIE KIT          |[feltcraft, girl, amelie, kit]            |(500,[60,64,69],[1.0,1.0,1.0])                     |
    |CAMOUFLAGE LED TORCH               |[camouflage, led, torch]                  |(500,[264],[1.0])                                  |
    |WHITE SKULL HOT WATER BOTTLE       |[white, skull, hot, water, bottle]        |(500,[15,34,39,40,118],[1.0,1.0,1.0,1.0,1.0])      |
    |ENGLISH ROSE HOT WATER BOTTLE      |[english, rose, hot, water, bottle]       |(500,[34,39,40,46,169],[1.0,1.0,1.0,1.0,1.0])      |
    |HOT WATER BOTTLE KEEP CALM         |[hot, water, bottle, keep, calm]          |(500,[34,39,40,147,148],[1.0,1.0,1.0,1.0,1.0])     |
    |SCOTTIE DOG HOT WATER BOTTLE       |[scottie, dog, hot, water, bottle]        |(500,[34,39,40,146,386],[1.0,1.0,1.0,1.0,1.0])     |
    |ROSE CARAVAN DOORSTOP              |[rose, caravan, doorstop]                 |(500,[46,297],[1.0,1.0])                           |
    |GINGHAM HEART  DOORSTOP RED        |[gingham, heart, , doorstop, red]         |(500,[3,4,11,143,297],[1.0,1.0,1.0,1.0,1.0])       |
    |STORAGE TIN VINTAGE LEAF           |[storage, tin, vintage, leaf]             |(500,[6,45,109,162],[1.0,1.0,1.0,1.0])             |
    |SET OF 4 KNICK KNACK TINS POPPIES  |[set, of, 4, knick, knack, tins, poppies] |(500,[0,1,49,70,365,366],[1.0,1.0,1.0,1.0,1.0,1.0])|
    |POPCORN HOLDER                     |[popcorn, holder]                         |(500,[21,296],[1.0,1.0])                           |
    |GROW A FLYTRAP OR SUNFLOWER IN TIN |[grow, a, flytrap, or, sunflower, in, tin]|(500,[36,45,378],[1.0,1.0,1.0])                    |
    |AIRLINE BAG VINTAGE WORLD CHAMPION |[airline, bag, vintage, world, champion]  |(500,[2,6,328],[1.0,1.0,1.0])                      |
    |AIRLINE BAG VINTAGE JET SET BROWN  |[airline, bag, vintage, jet, set, brown]  |(500,[0,2,6,328,405],[1.0,1.0,1.0,1.0,1.0])        |
    +-----------------------------------+------------------------------------------+---------------------------------------------------+
    ```

#### 4.1. TF-IDF

- 텍스트를 숫자로 변환하기 위한 또 다른 방법
- 얼마나 많은 문서가 그 용어를 포함하고 있는지를 가중치로 표현
    - 여러 문서에서 자주 발생 -> 가중치 낮음
    - 적은 문서에서 적게 발생 -> 가중치 높음
- HashingTF, IDF 사용 예제
    1. 'red'라는 단어가 포함된 문서 선택
        ```scala
        val tfIdfIn = tokenized
          .where("array_contains(DescOut, 'red')")
          .select("DescOut")
          .limit(10)
        tfIdfIn.show(false)
        ```
        ```text
        +---------------------------------------+
        |DescOut                                |
        +---------------------------------------+
        |[gingham, heart, , doorstop, red]      |
        |[red, floral, feltcraft, shoulder, bag]|
        |[alarm, clock, bakelike, red]          |
        |[pin, cushion, babushka, red]          |
        |[red, retrospot, mini, cases]          |
        |[red, kitchen, scales]                 |
        |[gingham, heart, , doorstop, red]      |
        |[large, red, babushka, notebook]       |
        |[red, retrospot, oven, glove]          |
        |[red, retrospot, plate]                |
        +---------------------------------------+
        ```
    2. TF-IDF 입력을 위해 각 단어를 해싱하여 수치화 및 빈도 가중치 부여 (여기서 해싱은 반대 동작 불가능)
        ```scala
        import org.apache.spark.ml.feature.{HashingTF, IDF}

        val tf = new HashingTF()
          .setInputCol("DescOut")
          .setOutputCol("TFOut")
          .setNumFeatures(10000)
        val idf = new IDF()
          .setInputCol("TFOut")
          .setOutputCol("IDFOut")
          .setMinDocFreq(2)
          ```
    3. TF-IDF 수행
        ```scala
        // 결과: 총 어휘 크기, 문서에 나타나는 모든 단어의 해시, 각 용어의 가중치
        idf
          .fit(tf.transform(tfIdfIn))
          .transform(tf.transform(tfIdfIn))
          .show(false)
        ```
        ```text
        +---------------------------------------+--------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------+
        |DescOut                                |TFOut                                                   |IDFOut                                                                                                              |
        +---------------------------------------+--------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------+
        |[gingham, heart, , doorstop, red]      |(10000,[3372,4291,4370,6594,9160],[1.0,1.0,1.0,1.0,1.0])|(10000,[3372,4291,4370,6594,9160],[1.2992829841302609,0.0,1.2992829841302609,1.2992829841302609,1.2992829841302609])|
        |[red, floral, feltcraft, shoulder, bag]|(10000,[155,1152,4291,5981,6756],[1.0,1.0,1.0,1.0,1.0]) |(10000,[155,1152,4291,5981,6756],[0.0,0.0,0.0,0.0,0.0])                                                             |
        |[alarm, clock, bakelike, red]          |(10000,[4291,4852,4995,9668],[1.0,1.0,1.0,1.0])         |(10000,[4291,4852,4995,9668],[0.0,0.0,0.0,0.0])                                                                     |
        |[pin, cushion, babushka, red]          |(10000,[4291,5111,5673,7153],[1.0,1.0,1.0,1.0])         |(10000,[4291,5111,5673,7153],[0.0,0.0,0.0,1.2992829841302609])                                                      |
        |[red, retrospot, mini, cases]          |(10000,[547,1576,2591,4291],[1.0,1.0,1.0,1.0])          |(10000,[547,1576,2591,4291],[0.0,0.0,1.0116009116784799,0.0])                                                       |
        |[red, kitchen, scales]                 |(10000,[3461,4291,6214],[1.0,1.0,1.0])                  |(10000,[3461,4291,6214],[0.0,0.0,0.0])                                                                              |
        |[gingham, heart, , doorstop, red]      |(10000,[3372,4291,4370,6594,9160],[1.0,1.0,1.0,1.0,1.0])|(10000,[3372,4291,4370,6594,9160],[1.2992829841302609,0.0,1.2992829841302609,1.2992829841302609,1.2992829841302609])|
        |[large, red, babushka, notebook]       |(10000,[2782,2787,4291,7153],[1.0,1.0,1.0,1.0])         |(10000,[2782,2787,4291,7153],[0.0,0.0,0.0,1.2992829841302609])                                                      |
        |[red, retrospot, oven, glove]          |(10000,[302,2591,4291,8242],[1.0,1.0,1.0,1.0])          |(10000,[302,2591,4291,8242],[0.0,1.0116009116784799,0.0,0.0])                                                       |
        |[red, retrospot, plate]                |(10000,[2591,4291,4456],[1.0,1.0,1.0])                  |(10000,[2591,4291,4456],[1.0116009116784799,0.0,0.0])                                                               |
        +---------------------------------------+--------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------+
        ```

### 5. Word2Vec

- [스파크 공식 문서](http://spark.apache.org/docs/latest/mllib-feature-extraction.html#word2vec)
- 단어 집합의 벡터를 계산하기 위한 딥러닝 도구
- 비슷한 단어를 벡터 공간에서 가깝게 배치함으로써 단어를 일반화
- 엔티티 인식, 모호성 제거, 구문 분석, 태그 지정, 기계 번역 등 여러 자연어 처리에서 활용 가능
- 단어간 의미 파악 가능
    - ex) v~king, v~queen, v~man, v~women의 관계
    - v~king - v~man + v~women ~= v~queen
- Word2Vec 과정
    1. 단어 건너뛰기(skip grams)를 사용해 문장을 벡터 표현으로 변환
    2. 어휘를 작성하고 모든 문장에 대해 토큰 제거, 모델 학습을 통해 'n-gram' 표현에서 누락된 토큰 예측
- Word2Vec 사용 예제
    ```scala
    import org.apache.spark.ml.feature.Word2Vec
    import org.apache.spark.ml.linalg.Vector
    import org.apache.spark.sql.Row

    // 입력 데이터: 각 로우는 문장 또는 문서의 단어주머니(bag of words)
    val documentDF = spark.createDataFrame(Seq(
      "Hi, I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // documentDF.show(false)

    // 단어를 벡터에 매핑
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(2)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)

    result.show(false)

    // result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
    //   println(s"Text: [${text.mkString(", ")}] => \\nVector: $features\\n")
    // }
    ```
    ```text
    +------------------------------------------+--------------------------------------------+
    |text                                      |result                                      |
    +------------------------------------------+--------------------------------------------+
    |[Hi, I, heard, about, Spark]              |[-0.006434932723641396,0.022580987215042116]|
    |[I, wish, Java, could, use, case, classes]|[-0.07318166151110615,0.051968332912240706] |
    |[Logistic, regression, models, are, neat] |[0.0940308004617691,-0.007929152250289917]  |
    +------------------------------------------+--------------------------------------------+
    ```

## 특징 조작하기

- 특징 공간(feature space)?
    - 머신러닝 수행 시 최적화 시키고자 하는 함수가 적용되는 공간
    - 즉, 두 데이터의 특징 공간이 같다면 같은 모집단에서 추출된 같은 종류의 데이터

### 1. 주성분 분석(Principal Components Analysis, PCA)

- 데이터의 주요 구성 요소를 찾는 수학적 기법
- 새로운 특징 집합을 작성하여 데이터의 특징 표현을 변경
- 새로운 특징은 기존 특징들의 조합
- 주요 정보를 포함하는 더 작은 특징 집합을 생성할 수 있는 장점 -> 모델링 시 입력값 활용. but, 이에 따른 잠재적 해석이 어려움
- **대규모 입력 데이터셋에서 특징 수를 줄이기 위해 사용**
    - ex) 특징 공간이 방대하고 특징 대부분이 관련성 없는 텍스트 분석 시에 PCA를 통해 특징 수 줄임
- 머신러닝 전에 주요 특징 조합을 선별하여 입력 가능
- *k* 파라미터: 새롭게 생성할 특징 수. 입력 벡터 크기보다 작아야함
    - 적절한 k를 선택하는 것이 중요
- PCA 사용 예제
    ```scala
    import org.apache.spark.ml.feature.PCA

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("result")
      .setK(2)

    pca
      .fit(scaleDF)
      .transform(scaleDF)
      .show(false)
    ```
    ```text
    +---+--------------+------------------------------------------+
    |id |features      |result                                    |
    +---+--------------+------------------------------------------+
    |0  |[1.0,0.1,-1.0]|[0.07137194992484153,-0.45266548881478463]|
    |1  |[2.0,1.1,1.0] |[-1.6804946984073725,1.2593401322219144]  |
    |0  |[1.0,0.1,-1.0]|[0.07137194992484153,-0.45266548881478463]|
    |1  |[2.0,1.1,1.0] |[-1.6804946984073725,1.2593401322219144]  |
    |1  |[3.0,10.1,3.0]|[-10.872398139848944,0.030962697060149758]|
    +---+--------------+------------------------------------------+
    ```

### 2. 상호작용

- 일반적인 선형 모델은 입력 데이터의 모든 특징 조합을 고려하지 않음
- 사용자가 특정 변수에 대해 중요한 변수라는 사전 지식이 있을 경우?
    - Interaction 변환자를 사용해 두 변수 사이의 상호작용을 수동으로 생성
    - Interaction 변환자는 스칼라에서만 사용 가능. 타 언어에서는 RFormula를 사용
    - 수동으로 상호작용 생성하기보다는 RFormula를 사용하는 것을 추천

### 3. 다항식 전개

- 모든 입력 컬럼의 상호작용 변수를 생성할 때 사용
- 상호작용 범위 지정 가능 (2차 다항식, 3차 다항식, ...)
- 특징 간의 상호작용을 검토하고싶지만, 정확히 무엇을 해야할지 확신하지 못할 때 사용 가능
- 다항식 전개에 계산 비용이 많이 들 수 있고, 과적합을 초래할 수 있으므로 높은 차수의 다항식은 사용시 주의
- PolynomialExpansion 사용한 2차 다항식 전개 예제
    ```scala
    import org.apache.spark.ml.feature.PolynomialExpansion

    val pe = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("result")
      .setDegree(2)

    pe.transform(scaleDF).show(false)
    ```
    ```text
    +---+--------------+-----------------------------------------------------------------------------------+
    |id |features      |result                                                                             |
    +---+--------------+-----------------------------------------------------------------------------------+
    |0  |[1.0,0.1,-1.0]|[1.0,1.0,0.1,0.1,0.010000000000000002,-1.0,-1.0,-0.1,1.0]                          |
    |1  |[2.0,1.1,1.0] |[2.0,4.0,1.1,2.2,1.2100000000000002,1.0,2.0,1.1,1.0]                               |
    |0  |[1.0,0.1,-1.0]|[1.0,1.0,0.1,0.1,0.010000000000000002,-1.0,-1.0,-0.1,1.0]                          |
    |1  |[2.0,1.1,1.0] |[2.0,4.0,1.1,2.2,1.2100000000000002,1.0,2.0,1.1,1.0]                               |
    |1  |[3.0,10.1,3.0]|[3.0,9.0,10.1,30.299999999999997,102.00999999999999,3.0,9.0,30.299999999999997,9.0]|
    +---+--------------+-----------------------------------------------------------------------------------+
    ```

## 특징 선택

- 데이터의 특징이 강한 상관관계를 나타내거나 너무 많은 특징을 모델 학습에 사용할 경우 과적합 발생
- 따라서, 후보 특징 중 일부만 선택적으로 사용해야 함 -> 특징 선택(feature selection)

### 1. ChiSqSelector

- 통계적 검정을 통해 예측 레이블과 독립적이지 않은 특징을 선별하고 관련 없는 특징 삭제 수행
- 주로 범주 데이터와 함께 사용
- [카이제곱 분포](https://ko.wikipedia.org/wiki/%EC%B9%B4%EC%9D%B4%EC%A0%9C%EA%B3%B1_%EB%B6%84%ED%8F%AC)
- 카이제곱 검정(Chi-Square test) 기반으로 '최상'의 특징을 선택할 수 있는 방법
    - numTopFeatures: *p* 값으로 정렬
    - percentile: 입력 특징의 비율 크기
    - fpr: *p* 값을 어느 수준에서 판단할지 정함
- ChiSqSelector 사용 예제
    ```scala
    import org.apache.spark.ml.feature.{ChiSqSelector, Tokenizer}

    val tkn = new Tokenizer()
      .setInputCol("Description")
      .setOutputCol("DescOut")
    val tokenized = tkn
      .transform(sales.select("Description", "CustomerId"))
      .where("CustomerId IS NOT NULL")

    val prechi = fittedCV.transform(tokenized)
    val chisq = new ChiSqSelector()
      .setFeaturesCol("countVec")
      .setLabelCol("CustomerId")
      .setNumTopFeatures(2)

    chisq
      .fit(prechi)
      .transform(prechi)
      .drop("customerId", "Description", "DescOut")
      .show(false)
    ```

## 고급 주제

### 1. 변환자 저장하기

- 추정자를 fit하여 얻은 변환자는 필요할 때 불러오기 위해 디스크에 보통 기록함
- write 메서드를 사용하여 위치 저장하기
    ```scala
    val fittedPCA = pca.fit(scaleDF)

    fittedPCA.write.overwrite().save("/tmp/fittedPCA")
    ```
- 불러오기
    ```scala
    import org.apache.spark.ml.feature.PCAModel

    val loadedPCA = PCAModel.load("/tmp/fittedPCA")
    loadedPCA.transform(scaleDF).show()
    ```

### 2. 사용자 정의 변환자 작성하기

- 사용자가 직접 비즈니스 로직 등을 인코딩할 경우 사용
- 내장 모듈을 최대한 많이 사용해서 최적화 하는 것이 좋음
- 사용자 정의 tokenizer 예제
    ```scala
    import org.apache.spark.ml.UnaryTransformer
    import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
    import org.apache.spark.sql.types.{ArrayType, StringType, DataType}
    import org.apache.spark.ml.param.{IntParam, ParamValidators}

    class MyTokenizer(override val uid: String) extends UnaryTransformer[String, Seq[String]] with DefaultParamsWritable {
      def this() = this(Identifiable.randomUID("myTokenizer"))

      val maxWords: IntParam = new IntParam(this, "maxWords", "The max number of words to return.", ParamValidators.gtEq(0))

      def setMaxWords(value: Int): this.type = set(maxWords, value)

      def getMaxWords: Integer = $(maxWords)

      override protected def createTransformFunc: String => Seq[String] = (inputString: String) => {
        inputString.split("\\\\s").take($(maxwords))
      }

      override protected def validateInputType(inputType: DataType): Unit = {
        require(inputType == StringType, s"Bad input type: $inputThype. Requires String.")
      }

      override protected def outputDataType: DataType = new ArrayType(StringType, true)
    }

    // 이 객체를 사용해 추후에 다시 읽어오기
    object MyTokenizer extends DefaultParamsReadable[MyTokenizer]

    val myT = new MyTokenizer().setInputCol("someCol").setMaxWords(2)
    myT.transform(Seq("hello world. This text won't show.").toDF("someCol")).show()
    ```

## 정리

- 일반적인 데이터 전처리에 대한 방법을 살펴보았다
- 자세한 정보는 [공식문서](http://spark.apache.org/docs/latest/ml-features.html)를 참고