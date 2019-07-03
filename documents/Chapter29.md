# 29 비지도 학습

- 패턴을 발견하기 위해 노력. 데이터셋의 근본적인 구조적 특징을 간결하게 표현하는 게 핵심
- 대표적인 게 군집화
- 성과 측정 어려움
- 차원의 저주
  - 고차원의 공간에서의 군집화는 엉뚱한 결과 생성
  - 공간의 크기가 증가할수록 밀도가 급격히 희소해짐

### 29.1 활용 사례

- 데이터 이상치 탐지
  - 큰 그룹으로 군집화하고, 소그룹이 이상치인지 추가 조사
- 토픽 모델링
  - 많은 양의 텍스트 문서에서 공통 주제 도출
- 유사한 객체 그룹화

### 29.2 모델 확장성

| 모델 | 통계적 권장 | 최대 연산 한계 |
| --- | --- | -- |
| k-means | 최대 50~100 | 특징 수 x 군집 수 < 1천만 건 |
| bisecting k-means | 최대 50~100 | 특징 수 x 군집 수 < 1천만 건 |
| GMM | 최대 50~100 | 특징 수 x 군집 수 < 1천만 건 |
| LDA | 사람이 해석 가능한 수준 | 최대 1,000개 토픽 |

#### Load Sample Data
```scala
import org.apache.spark.ml.feature.VectorAssembler

val va = new VectorAssembler()
  .setInputCols(Array("Quantity", "UnitPrice"))
  .setOutputCol("features")

val sales = va.transform(spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/data/retail-data/by-day/*.csv")
  .limit(50)
  .coalesce(1)
  .where("Description IS NOT NULL"))

sales.cache()
```
output : 
```console
import org.apache.spark.ml.feature.VectorAssembler
va: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_34bf9e20439c
sales: org.apache.spark.sql.DataFrame = [InvoiceNo: string, StockCode: string ... 7 more fields]
res2: sales.type = [InvoiceNo: string, StockCode: string ... 7 more fields]
```

### 29.3 K-Means
- k개로 군집하는 알고리즘
- 알고리즘 개요
  1. k개 포인트가 공간에 임의 할당
  2. 각 데이터가 포인트와의 유클리드 거리를 계산하여 가장 가까이에 위치한 군집으로 할당
  3. 각 군집별 중심(센트로이드)을 계산
  4. 지정한 횟수 또는 수렴될때까지 2~3을 반복
- 서로 다른 초깃값으로 여러 번 수행해보는 게 좋다.
- 적절한 k 값이 중요하다.

#### 29.3.1 하이퍼파라미터
- k : 최종 생성하고자 하는 군집 수

#### 29.3.2 학습 파라미터
| 파라미터에 크게 민감하지 않기에 기본값 추천
- initMode : 군집 중심의 시작 위치 결정 알고리즘 [ **k-means||** , random ]
  - k-means++ : 초기값을 가능하게 하는 k-means
  - k-means|| : k-means++를 분산 처리 가능하도록 변형한 알고리즘
- initSteps : k-means|| 초기화 모드의 단계 수 (default : 2, > 2)
- maxIter : 반복 횟수 (default : 20)
- tol : 임곗값 (default : 0.00001)

#### 29.3.3 K-means
```scala
import org.apache.spark.ml.clustering.KMeans
val km = new KMeans().setK(5)
println(km.explainParams())
val kmModel = km.fit(sales)
```
output :
```console
distanceMeasure: The distance measure. Supported options: 'euclidean' and 'cosine' (default: euclidean)
featuresCol: features column name (default: features)
initMode: The initialization algorithm. Supported options: 'random' and 'k-means||'. (default: k-means||)
initSteps: The number of steps for k-means|| initialization mode. Must be > 0. (default: 2)
k: The number of clusters to create. Must be > 1. (default: 2, current: 5)
maxIter: maximum number of iterations (>= 0) (default: 20)
predictionCol: prediction column name (default: prediction)
seed: random seed (default: -1689246527)
tol: the convergence tolerance for iterative algorithms (>= 0) (default: 1.0E-4)
import org.apache.spark.ml.clustering.KMeans
km: org.apache.spark.ml.clustering.KMeans = kmeans_b640586f900a
kmModel: org.apache.spark.ml.clustering.KMeansModel = kmeans_b640586f900a
```

#### 29.3.4 Summary of K-means
- summary 클래스는 k-means 성공을 판단하는 공통적인 척도를 제공
- 생성된 군집 정보와 상대적 크기 등 포함
- computeCost를 사용하여 군집내 오차제곱합 계산 가능. 군집의 중심으로부터 값이 얼마나 가까운지 측정

```scala
val summary = kmModel.summary
summary.clusterSizes.foreach(println) // number of points
kmModel.computeCost(sales)
// kmModel.computeCost(sales).foreach(println)
println("Cluster Centers: ")
kmModel.clusterCenters.foreach(println)
```
```console
warning: there was one deprecation warning; re-run with -deprecation for details
10
20
12
3
5
Cluster Centers: 
[23.200000000000003,0.9560000000000001]
[4.55,4.5965]
[11.333333333333332,1.0999999999999996]
[44.0,1.1633333333333333]
[2.4000000000000004,13.040000000000001]
summary: org.apache.spark.ml.clustering.KMeansSummary = org.apache.spark.ml.clustering.KMeansSummary@21478bc4
```

### 29.4 BiSecting K-means
- K-means와 반대로 하향식 군집화 방식
- 최초에 단일 그룹에서 시작하여, k그룹을 만든다.
- K-means 보다 빠르다.
- K-means와 결과가 다르다.

#### 29.4.1 하이퍼파라미터
- k : 최종 생성하고자 하는 군집 수

#### 29.4.2 학습 파라미터
| 최상의 결과를 위해서는 대부분의 파리미터 조정 필요. 절대적 규칙은 없음
- maxIter : 반복 횟수 (default : 20)
- minDivisibleClusterSize : (default : 1.0)
  - 군집에 포함될 최소 데이터 수 (>= 1.0)
  - 최소 데이터 비율 (< 1.0)

#### 29.4.3 BiSecting K-means
```scala
import org.apache.spark.ml.clustering.BisectingKMeans
val bkm = new BisectingKMeans().setK(5).setMaxIter(5)
println(bkm.explainParams())
val bkmModel = bkm.fit(sales)
```
```console
distanceMeasure: The distance measure. Supported options: 'euclidean' and 'cosine' (default: euclidean)
featuresCol: features column name (default: features)
k: The desired number of leaf clusters. Must be > 1. (default: 4, current: 5)
maxIter: maximum number of iterations (>= 0) (default: 20, current: 5)
minDivisibleClusterSize: The minimum number of points (if >= 1.0) or the minimum proportion of points (if < 1.0) of a divisible cluster. (default: 1.0)
predictionCol: prediction column name (default: prediction)
seed: random seed (default: 566573821)
import org.apache.spark.ml.clustering.BisectingKMeans
bkm: org.apache.spark.ml.clustering.BisectingKMeans = bisecting-kmeans_f34c812e88b1
bkmModel: org.apache.spark.ml.clustering.BisectingKMeansModel = bisecting-kmeans_f34c812e88b1
```

#### 29.4.4 Summary of BiSecting K-means
- summary 클래스는 k-means의 summary와 거의 유사
- 생성된 군집 정보와 상대적 크기 등 포함

```scala
val summary = bkmModel.summary
summary.clusterSizes.foreach(println) // number of points
kmModel.computeCost(sales)
//kmModel.computeCost(sales).foreach(println)
println("Cluster Centers: ")
kmModel.clusterCenters.foreach(println)
```
```console
warning: there was one deprecation warning; re-run with -deprecation for details
16
8
13
10
3
Cluster Centers: 
[23.200000000000003,0.9560000000000001]
[4.55,4.5965]
[11.333333333333332,1.0999999999999996]
[44.0,1.1633333333333333]
[2.4000000000000004,13.040000000000001]
summary: org.apache.spark.ml.clustering.BisectingKMeansSummary = org.apache.spark.ml.clustering.BisectingKMeansSummary@7d18bdd8
```

### 29.5 GMM
- 소프트 군집화 : 어느 군집에 속하는지 확률 또는 가중치로 표현
- 각 군집이 가우시안 분포로부터 무작위 추출을 하여 데이터를 생성한다고 가정
- 어느 가우시안 분포에 포함될지 확률적으로 큰 곳의 군집에 할당
- 가우시안 분포 = 정규 분포 : 좌우 대칭
- 표준 정규 분포 : 정규분포이면서 평균 0, 표준편차 1

#### 29.5.1 하이퍼파라미터
- k : 최종 생성하고자 하는 군집 수

#### 29.5.2 학습 파라미터
| 파라미터에 크게 민감하지 않기에 기본값 추천
- maxIter : 반복 횟수 (default : 100)
- tol : 임곗값 (default : 0.01)

#### 29.5.3 GMM
```scala
import org.apache.spark.ml.clustering.GaussianMixture
val gmm = new GaussianMixture().setK(5)
println(gmm.explainParams())
val model = gmm.fit(sales)
```
```console
featuresCol: features column name (default: features)
k: Number of independent Gaussians in the mixture model. Must be > 1. (default: 2, current: 5)
maxIter: maximum number of iterations (>= 0) (default: 100)
predictionCol: prediction column name (default: prediction)
probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities (default: probability)
seed: random seed (default: 538009335)
tol: the convergence tolerance for iterative algorithms (>= 0) (default: 0.01)
import org.apache.spark.ml.clustering.GaussianMixture
gmm: org.apache.spark.ml.clustering.GaussianMixture = GaussianMixture_448cbe438110
model: org.apache.spark.ml.clustering.GaussianMixtureModel = GaussianMixture_448cbe438110
```

### 29.5.4 Summary of GMM
- 생성된 클러스터 정보 (가중치, 평균 및 가우스 혼합의 공분산)등이 포함

```scala
val summary = model.summary
model.weights.foreach(println)
model.gaussiansDF.show()
summary.cluster.show()
summary.clusterSizes
summary.probability.show()
```
```console
0.2037672399608971
0.13999983831435028
0.1633437562383104
0.1755724681410311
0.3173166973454111
+--------------------+--------------------+
|                mean|                 cov|
+--------------------+--------------------+
|[26.8969402794066...|155.4804310513139...|
|[10.8571461378584...|3.265300498555194...|
|[2.52584147369065...|0.770164672724497...|
|[17.7437989010750...|39.65470455661214...|
|[4.82603980059328...|1.655630468857532...|
+--------------------+--------------------+

+----------+
|prediction|
+----------+
|         0|
|         0|
|         0|
|         0|
|         4|
|         0|
|         4|
|         0|
|         4|
|         4|
|         4|
|         4|
|         1|
|         1|
|         3|
|         3|
|         0|
|         0|
|         4|
|         4|
+----------+
only showing top 20 rows

+--------------------+
|         probability|
+--------------------+
|[0.99999999999717...|
|[0.99997354327726...|
|[0.99999999999985...|
|[0.99999999999963...|
|[0.00553284170817...|
|[0.99999999999929...|
|[8.19503968942260...|
|[0.99999999999983...|
|[5.77416968211996...|
|[5.77416968211996...|
|[7.83213162884820...|
|[1.40641820378790...|
|[2.59516003767789...|
|[2.59516003767789...|
|[0.08975856024991...|
|[0.21615743245013...|
|[0.99999999999966...|
|[0.99999999999963...|
|[5.77416968211996...|
|[5.77416968211996...|
+--------------------+
only showing top 20 rows

summary: org.apache.spark.ml.clustering.GaussianMixtureSummary = org.apache.spark.ml.clustering.GaussianMixtureSummary@4ebf3887
```

### 29.6 LDA
- 텍스트 문서에 대한 토픽 모델링
- 문서와 키워드로부터 주제를 추출 -> 각 문서가 입력된 여러 주제에 얼마나 기여했는지 횟수 계산
- 구현 방법 2가지
  - 온라인 LDA : 샘플 데이터 많은 경우에 적합
  - 기댓값 최대화 : 어휘 수 많은 경우에 적합
- 텍스트 데이터를 수치형으로 변환하기 위해 CountVectorizer 필요

#### CountVectorize for LDA
```scala
import org.apache.spark.ml.feature.{Tokenizer, CountVectorizer}
val tkn = new Tokenizer().setInputCol("Description").setOutputCol("DescOut")
val tokenized = tkn.transform(sales.drop("features"))
val cv = new CountVectorizer()
  .setInputCol("DescOut")
  .setOutputCol("features")
  .setVocabSize(500)
  .setMinTF(0)
  .setMinDF(0)
  .setBinary(true)
val cvFitted = cv.fit(tokenized)
val prepped = cvFitted.transform(tokenized)
```
```console
import org.apache.spark.ml.feature.{Tokenizer, CountVectorizer}
tkn: org.apache.spark.ml.feature.Tokenizer = tok_e163bc71ac63
tokenized: org.apache.spark.sql.DataFrame = [InvoiceNo: string, StockCode: string ... 7 more fields]
cv: org.apache.spark.ml.feature.CountVectorizer = cntVec_ea984e77adb0
cvFitted: org.apache.spark.ml.feature.CountVectorizerModel = cntVec_ea984e77adb0
prepped: org.apache.spark.sql.DataFrame = [InvoiceNo: string, StockCode: string ... 8 more fields]
```

#### 29.6.1 하이퍼파라미터
- k : 총 주제 수 (default : 10, > 0)
- dotConcentration (alpha) : 문서가 가지는 **주제분포** (theta)의 사전 추정치. 클수록 편평화.
- topicConcentration (beta) :  주제가 가지는 **단어분포**의 사전 추정치.

#### 29.6.2 학습파라미터
- maxIter : 반복 횟수 (default : 20)
- optimizer : 학습 알고리즘 [ em , onilne ] (default : online)
- learningDecay : for online, 지수적 감쇠율로 설정된 학습 속도 (0.5, 1.0] (default : 0.51)
- learningOffset : for online, 초기 반복 수행횟수를 줄이는 (긍정적) 학습 파라미터, 클수록 횟수가 줄어든다. (default : 1,024.0)
- optimizeDocConcentration : for online, docConcentration 최적화 여부 (default : true)
- subsamplingRate : for online, 미니배치 경사하강법의 반복 수행에서 샘플링 및 적용되는 말뭉치의 비율 (0, 1] (default : 0.5)
- seed : 재현성을 위해 임의의 시드 지정 가능
- checkpointInterval : 체크포인트 기능

#### 29.6.3 예측파라미터
- topicDistributionCol : 각 문서의 주제 혼합 분포의 결과를 출력하는 컬럼

### 29.56.4 LDA
```scala
import org.apache.spark.ml.clustering.LDA
val lda = new LDA().setK(10).setMaxIter(5)
println(lda.explainParams())
val model = lda.fit(prepped)
```
```console
checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)
docConcentration: Concentration parameter (commonly named "alpha") for the prior placed on documents' distributions over topics ("theta"). (undefined)
featuresCol: features column name (default: features)
k: The number of topics (clusters) to infer. Must be > 1. (default: 10, current: 10)
keepLastCheckpoint: (For EM optimizer) If using checkpointing, this indicates whether to keep the last checkpoint. If false, then the checkpoint will be deleted. Deleting the checkpoint can cause failures if a data partition is lost, so set this bit with care. (default: true)
learningDecay: (For online optimizer) Learning rate, set as an exponential decay rate. This should be between (0.5, 1.0] to guarantee asymptotic convergence. (default: 0.51)
learningOffset: (For online optimizer) A (positive) learning parameter that downweights early iterations. Larger values make early iterations count less. (default: 1024.0)
maxIter: maximum number of iterations (>= 0) (default: 20, current: 5)
optimizeDocConcentration: (For online optimizer only, currently) Indicates whether the docConcentration (Dirichlet parameter for document-topic distribution) will be optimized during training. (default: true)
optimizer: Optimizer or inference algorithm used to estimate the LDA model. Supported: online, em (default: online)
seed: random seed (default: 1435876747)
subsamplingRate: (For online optimizer) Fraction of the corpus to be sampled and used in each iteration of mini-batch gradient descent, in range (0, 1]. (default: 0.05)
topicConcentration: Concentration parameter (commonly named "beta" or "eta") for the prior placed on topic' distributions over terms. (undefined)
topicDistributionCol: Output column with estimates of the topic mixture distribution for each document (often called "theta" in the literature).  Returns a vector of zeros for an empty document. (default: topicDistribution)
import org.apache.spark.ml.clustering.LDA
lda: org.apache.spark.ml.clustering.LDA = lda_1d8684c4a71a
model: org.apache.spark.ml.clustering.LDAModel = lda_1d8684c4a71a
```

#### 색인결과 of LDA
```scala
model.describeTopics(3).show()
cvFitted.vocabulary
```
```console
+-----+---------------+--------------------+
|topic|    termIndices|         termWeights|
+-----+---------------+--------------------+
|    0|   [69, 88, 81]|[0.00908107405013...|
|    1|[107, 127, 100]|[0.00888050121255...|
|    2|  [15, 77, 120]|[0.01734248131232...|
|    3|    [36, 72, 9]|[0.01159491697518...|
|    4|  [137, 34, 22]|[0.00905035557253...|
|    5|  [34, 124, 95]|[0.00911696062041...|
|    6|     [2, 7, 18]|[0.01756473154171...|
|    7|   [87, 58, 73]|[0.00867808805804...|
|    8|      [1, 3, 0]|[0.01801302800320...|
|    9|  [29, 132, 34]|[0.00954482252355...|
+-----+---------------+--------------------+

res6: Array[String] = Array(water, hot, vintage, bottle, paperweight, 6, home, doormat, landmark, bicycle, frame, ribbons, "", classic, rose, kit, leaf, sweet, bag, airline, doorstop, light, in, christmas, heart, calm, set, keep, balloons, night, lights, 12, tin, english, caravan, stuff, tidy, oxford, full, cottage, notting, drawer, mushrooms, chrome, champion, amelie, mini, the, giant, design, elegant, tins, jet, fairy, 50's, holder, message, blue, storage, tier, covent, world, skulls, font, hearts, skull, clips, bell, red, party, chalkboard, save, 4, coloured, poppies, garden, nine, girl, shimmering, doughnut, dog, 3, tattoos, chilli, coat, torch, sunflower, tale, cards, puncture, woodland, bomb, knack, lip, collage, rabbit, sex, of, rack, wall, cracker, scottie, hill, led, black, art...
```

