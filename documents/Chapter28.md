# 28 추천

- 명시적 피드백 : 평점 매기기
- 암시적 피드백 : 영화/음악 감상 이력 등

### 28.1 활용 사례

- 추천 예시
  - 영화 추천 : 아마존, 넷플리스, HBO
  - 과목 추천 : 과거 수강 신청 이력으로 추천

- 교차최소제곱
  - Spark 에서는 추천 알고리즘으로 Alternating Least Square를 제공
  - Collaborative Filtering 기술을 활용하여 사용자의 과거 상호작용을 기반으로 추천 (암시적 피드백 가능)
  - Spark ALS 는 명시적, 암묵적 피드백 모두 지원 가능

- 빈발 패턴 마이닝
  - 연관 규칙 (장바구니 분석)

### 28.2 ALS 사용하여 CF 구현

- ALS
  - 사용자가 아직 평가하지 않은 아이템의 평점을 예측하는 데 사용할 수 있는 특징 벡터를 생성
  - k 차원 특징 벡터 찾기 : ("아이템의 특징 벡터"와 "사용자의 특징 벡터"의 내적) ~근사~ (사용자의 평점)
- 사용자,아이템 행렬에서의 수치
  - 명시적 피드백 : 평점
  - 암시적 피드백 : 방문수
- 이슈
  - 신제품 : cold start problem
  - 신규 사용자 : first-rater problem
- 장점
  - 수백만명의 사용자, 아이템 처리 가능. 수십억 개의 평점 반영 가능.
  - 병렬 처리 가능

#### 28.2.1 하이퍼파라미터

- rank : 특징 벡터 차원. 너무 크면 과적합, 너무 작으면 낮은 예측 (default : 10)
- alpha : for 암시적 피드백 데이터, 기본 신뢰도 설정 (default : 10)
- regParam : 모델 과적합 방지를 위한 일반화를 제어. (default : 0.1)
- implicitPrefs : 데이터의 암시적 여부 (default : false)
- nonnegative : 최소제곱 문제에 비음수 제약 조건 설정 (default : false)

#### 28.2.2 학습 파라미터

| 데이터 분산 방식을 저수준까지 제어 가능. 데이터 그룹을 블록이라 함. 블록당 100~500만 정도가 좋음.
- numUserBlocks : 사용자 분할 블록 수 (default : 10)
- numItemBlocks : 사용자 분할 블록 수 (default : 10)
- maxIter : 총 방복 횟수 (default : 10)
- checkpointInterval : 체크포인팅 주기. 노드 오류시 복구 빠르게 가능
- seed : 임의 시드를 통해 재연 가능

#### 28.2.3 예측 파라미터

- 학습 완료된 모델이 어떻게 예측할 것인지 방법을 결정
- coldStartStrategy만 제공됨 (NaN은 drop 처리)
- cold start 문제는 ...
  - 새로운 사용자나 아이템 이력이 없을 때도 문제이지만
  - 모델 성과를 적절히 평가하지 못해서 최적의 모델 선택이 불가한 이슈도 있음

#### 28.2.4 ALS

#### Dataset Load
```scala
import org.apache.spark.ml.recommendation.ALS
val ratings = spark.read.textFile("/data/sample_movielens_ratings.txt")
  .selectExpr("split(value , '::') as col")
  .selectExpr(
    "cast(col[0] as int) as userId",
    "cast(col[1] as int) as movieId",
    "cast(col[2] as float) as rating",
    "cast(col[3] as long) as timestamp")
val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")
println(als.explainParams())
val alsModel = als.fit(training)
val predictions = alsModel.transform(test)
```
output :
```console
alpha: alpha for implicit preference (default: 1.0)
checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)
coldStartStrategy: strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)
finalStorageLevel: StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
implicitPrefs: whether to use implicit preference (default: false)
intermediateStorageLevel: StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
itemCol: column name for item ids. Ids must be within the integer value range. (default: item, current: movieId)
maxIter: maximum number of iterations (>= 0) (default: 10, current: 5)
nonnegative: whether to use nonnegative constraint for least squares (default: false)
numItemBlocks: number of item blocks (default: 10)
numUserBlocks: number of user blocks (default: 10)
predictionCol: prediction column name (default: prediction)
rank: rank of the factorization (default: 10)
ratingCol: column name for ratings (default: rating, current: rating)
regParam: regularization parameter (>= 0) (default: 0.1, current: 0.01)
seed: random seed (default: 1994790107)
userCol: column name for user ids. Ids must be within the integer value range. (default: user, current: userId)
import org.apache.spark.ml.recommendation.ALS
ratings: org.apache.spark.sql.DataFrame = [userId: int, movieId: int ... 2 more fields]
training: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [userId: int, movieId: int ... 2 more fields]
test: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [userId: int, movieId: int ... 2 more fields]
als: org.apache.spark.ml.recommendation.ALS = als_01f5bcdf6a33
alsModel: org.apache.spark.ml.recommendation.ALSModel = als_01f5bcdf6a33
predictions: org.apache.spark.sql.DataFrame = [userId: int, movieId: int ... 3 more fields]
```

#### ALS
```scala
alsModel.recommendForAllUsers(10)
  .selectExpr("userId", "explode(recommendations)").show()
alsModel.recommendForAllItems(10)
  .selectExpr("movieId", "explode(recommendations)").show()
```
output :
```console
+------+---------------+
|userId|            col|
+------+---------------+
|    28| [93, 6.575739]|
|    28| [81, 5.116676]|
|    28| [92, 4.990994]|
|    28|[10, 4.6011553]|
|    28| [40, 4.456021]|
|    28|[12, 4.4496927]|
|    28|  [9, 4.441847]|
|    28|[32, 4.4279385]|
|    28| [52, 4.419746]|
|    28|[74, 4.3055973]|
|    26|[94, 6.3221154]|
|    26| [24, 5.032882]|
|    26|[90, 4.9520164]|
|    26|  [7, 4.912031]|
|    26| [88, 4.796612]|
|    26| [68, 4.670635]|
|    26|[23, 4.5560145]|
|    26| [32, 4.422547]|
|    26| [17, 4.361108]|
|    26|[72, 4.2279005]|
+------+---------------+
only showing top 20 rows

+-------+---------------+
|movieId|            col|
+-------+---------------+
|     31|[12, 3.8703227]|
|     31|[16, 3.6222498]|
|     31|[22, 3.2817945]|
|     31|[14, 3.1975749]|
|     31|  [7, 2.954986]|
|     31|   [6, 2.90699]|
|     31|  [8, 2.818896]|
|     31| [15, 2.095858]|
|     31|[26, 1.9227128]|
|     31|[11, 1.5846266]|
|     85|  [16, 4.99182]|
|     85| [14, 4.814225]|
|     85|  [8, 4.656166]|
|     85|[22, 3.9892545]|
|     85|[18, 3.5882034]|
|     85|  [6, 3.269388]|
|     85|[24, 3.1881623]|
|     85|[26, 2.8329728]|
|     85| [3, 2.7982516]|
|     85| [21, 2.756485]|
+-------+---------------+
only showing top 20 rows
```

### 28.3 추천을 위한 평가기

- 콜드스타트전략을 사용하는 경우 자동 모델 평가 가능
- 회귀 문제와 비슷해서 평점의 예측값과 실젯값의 차이를 줄이는 최적화 수행 (RegressionEvaluator)

#### RegressionEvaluator
```scala
import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator = new RegressionEvaluator()
  .setMetricName("rmse")
  .setLabelCol("rating")
  .setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")
```
output :
```console
Root-mean-square error = 2.0005399337316847
import org.apache.spark.ml.evaluation.RegressionEvaluator
evaluator: org.apache.spark.ml.evaluation.RegressionEvaluator = regEval_637b88a2f9ea
rmse: Double = 2.0005399337316847
```

### 28.4 성과 평가지표

- 추천 결과는 ...
  - 표준 회귀 평가지표뿐만 아니라
  - 추천에 특화된 평가지표를 활용하여 평가 가능 (상대적으로 더 정교함)

#### 28.4.1 회귀 평가지표

```scala
import org.apache.spark.mllib.evaluation.{
  RankingMetrics,
  RegressionMetrics}
val regComparison = predictions.select("rating", "prediction")
  .rdd.map(x => (x.getFloat(0).toDouble,x.getFloat(1).toDouble))
val metrics = new RegressionMetrics(regComparison)
```
output :
```console
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
regComparison: org.apache.spark.rdd.RDD[(Double, Double)] = MapPartitionsRDD[820] at map at <console>:41
metrics: org.apache.spark.mllib.evaluation.RegressionMetrics = org.apache.spark.mllib.evaluation.RegressionMetrics@55a94851
```

#### 28.4.2 순위 평가지표

- 추천 결과를 사용자가 표현한 실제 평점과 비교 가능
- 이미 순위가 매겨진 아이템을 알고리즘이 다시 추천하는지 여부에 초점

#### ActualData
```scala
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.sql.functions.{col, expr}
val perUserActual = predictions
  .where("rating > 2.5")
  .groupBy("userId")
  .agg(expr("collect_set(movieId) as movies"))
```
output :
```console
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.sql.functions.{col, expr}
perUserActual: org.apache.spark.sql.DataFrame = [userId: int, movies: array<int>]
```

#### PredictedData
```scala
val perUserPredictions = predictions
  .orderBy(col("userId"), col("prediction").desc)
  .groupBy("userId")
  .agg(expr("collect_list(movieId) as movies"))
```
output :
```console
perUserPredictions: org.apache.spark.sql.DataFrame = [userId: int, movies: array<int>]
```

#### RankingMetrics
```scala
val perUserActualvPred = perUserActual.join(perUserPredictions, Seq("userId"))
  .map(row => (
    row(1).asInstanceOf[Seq[Integer]].toArray,
    row(2).asInstanceOf[Seq[Integer]].toArray.take(15)
  ))
val ranks = new RankingMetrics(perUserActualvPred.rdd)
```
```console
perUserActualvPred: org.apache.spark.sql.Dataset[(Array[Integer], Array[Integer])] = [_1: array<int>, _2: array<int>]
ranks: org.apache.spark.mllib.evaluation.RankingMetrics[Integer] = org.apache.spark.mllib.evaluation.RankingMetrics@32c4c9b
```

#### Result
```scala
ranks.meanAveragePrecision
ranks.precisionAt(5)
```
```console
res20: Double = 0.45384615384615384
```

## 28.5 빈발 패턴 마이닝

- 장바구니 분석 : 원시 데이터를 기반으로 연관 규칙을 찾는 알고리즘
- FP-성장 알고리즘 구현됨