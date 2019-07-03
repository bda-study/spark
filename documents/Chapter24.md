# Chapter 24. 고급 분석과 머신러닝 개요

---
- 데이터 전처리 : 정제, 피처 엔지니어링
- 지도 학습
- 비지도 학습
- 추천 엔진
- 그래프 분석
- 딥러닝

---
## 24.1 고급 분석에 대한 짧은 입문서

데이터 기반의 인사이트를 도출하여 핵심 문제를 해결하거나 예측 또는 추천을 위한 기술

- 지도 학습 : 다양한 특징을 기반으로 각 샘플에 부여된 레이블을 예측하는 분류/회귀 문제
- 추천 엔진 : 사용자의 과거 행동에 기반하여 제품을 제안
- 비지도 학습 : 군집 분석, 이상징후 탐지, 토픽 모델링과 같은 데이터 구조 파악
- 그래프 분석 : 소셜 네트워크상에서 유의미한 패턴 찾기

### 24.1.1 지도학습

- 레이블을 포함하는 과거 데이터를 사용. 모델 학습
- 데이터 포인트의 다양한 특징을 기반으로 해당 레이블값을 예측
- 주로 경사 하강법과 같은 반복적 최적화 알고리즘을 사용. 내부 파라미터를 조정
- 예측하고자 하는 변수 타입에 따라 **분류** 와 **회귀** 로 나눔

#### * 분류

- 범주형(불연속적 유한한 값) 종속 변수를 예측
- 이진 분류. ex) 스팸 여부
- 다중 클래스 분류. ex) 이메일 분류
- 예시 : 질병 예측, 이미지 분류, 고객 이탈 예측, 구매 여부 예측 등에 사용

#### * 회귀

- 연속형 변수(실수)를 예측
- 예시 : 판매량 예측, 신장 예측, 관객 수 예측 등에 사용

### 24.1.2 추천

- 명시적 선호도(등급), 암시적 선호도(행동 관찰)를 통해 사용자 또는 아이템 간 유사성 도출
- 특정 사용자와 유사한 사용자가 선호하는 상품 추천
- 예시 : 영화 추천, 상품 추천 등에 사용

### 24.1.3 비지도 학습

- 특정 패턴을 찾거나 숨겨진 구조적 특징을 발견
- 종속변수(레이블)이 없음
- 좋고 나쁨의 판별 어려워.
- 예시 : 이상징후 탐지, 사용자 세분화, 토픽 모델링(문서 주제 추출) 등에 사용

### 24.1.4 그래프 분석

- 객체를 가리키는 **정점**, 객체 간의 관계를 나타내는 **에지**를 지정하는 구조 연구
- 예를 들어 사람, 상품의 정점들에서 구매라는 에지를 나타내어 사람과 상품 간의 관계 파악
- 예시 : 사기거래 예측, 이상징후 탐지, 네트워크 특성 분류, 웹 페이지 추천 등에 사용

### 24.1.5 고급 분석 프로세스

- 활용 사례 외에도 데이터 준비, 모델 학습, 모델 평가 등 훨씬 더 많은 작업이 존재
 
<img src="https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/assets/spdg_2401.png" width=70% style="border: 1px solid gray">

- 머신러닝 수행 단계
1. 수집
2. 정제 및 검토
3. 피처 엔지니어링 : 알고리즘이 인식할 수 있도록 데이터 조작 (특징 선택, 특징 추출 등)
4. 학습 : 일부 데이터를 학습 데이터셋으로 사용
5. 평가 : 모델을 테스트셋에 적용해 결과를 측정
6. 예측 : 예측, 감지 등 비즈니스 문제 해결 

#### 1) 데이터 수집

- 스파크는 다양한 데이터소스를 불러올 수 있다.

#### 2) 데이터 정제

- 탐색적 데이터 분석 (EDA) : 일반적으로 대화형 쿼리 및 시각화 방법 등을 통해 데이터의 분포, 상관 관계, 기타 세부 현황 파악
- 잘못 기록된 데이터, 누락된 데이터 등을 파악

#### 3) 피처 엔지니어링

- 머신러닝을 적용 가능한 형태로 변환
- 데이터 정규화, 새로운 변수 추가, 범주형 변수 조작, 형식 변환 등
- 스파크 MLlib에서는 모든 변수가 실수형 벡터로 입력되어야.

#### 4) 모델 학습

- 과거 데이터셋, 분석 목적이 주어지고, 적합한 출력을 예측하는 모델을 학습
- 모델 : 학습 과정의 결과. 통찰력을 얻거나 미래를 예측하는 데 활용

#### 5) 모델 튜닝 및 평가

- 주어진 데이터셋을 나누어, 학습 데이터셋에 특화되는 현상(과적합)을 피할 수 있다.
- 학습셋 : 모델 학습을 위한 데이터셋
- 검증셋 : 모델 학습 과정에서, 모델을 다양하게 변현시켜 적합성을 테스트하는 데이터셋
- 테스트셋 : 변형된 모델을 최종 평가하여 가장 적합한 모델을 선정하기 위한 데이터셋

#### 6) 모델 및 통찰력 활용하기

- 만들어진 모델을 운영 환경에 적용하기. 이 또한 쉽지 않은 일

---
## 24.2 스파크의 고급 분석 툴킷

- MLlib : 스파크에 내장된 패키지. 머신러닝 파이프라인 구축을 위한 인터페이스 제공. 두개의 패키지로 구성
- org.apache.spark.ml : DataFrame 인터페이스. <br/>선행 단계 수행 과정을 표준화하는 데 도움이 되는 머신러닝 파이프라인 구축을 위한 고급 인터페이스 제공
- org.apache.spark.mllib : RDD 인터페이스. 유지보수 모드

### MLlib은 언제, 왜 사용?

- 단일 머신 기반인 파이썬 사킷런, R 패키지는 데이터 크기, 처리 시간 면에서 한계
- 확장성을 갖춘 MLlib과 상호보완적
- Case 1) 스파크로 전처리, 특징 생성, 데이터셋 분리 등을 수행하고, 단일 머신 기반 라이프러리에서 학습
- Case 2) 입력 데이터나 모델 크기가 너무 커서 스파크를 사용하여 머신 러닝 수행
- 스파크는 모델 자체적으로 대기 시간이 짧은 예측을 제공하지 않음. 외부로 보내야 함.

---
## 24.3 고수준 MLlib의 개념

- MLlib에는 변환자, 추정자, 평가기, 파이프라인와 같은 구조적 유형 존재
<img src="https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/assets/spdg_2402.png" width=70% style="border: 1px solid gray">

- 변환자  : 원시 데이터를 변환하는 함수
- 추정자 : "데이터를 초기화하는 일종의 변환자" 또는 "모델 학습용 알고리즘"
- 평가기 : 모델 성능이 좋은지 (ROC 등) 확인하여 최종 모델 선택
- 파이프라인 : 변환, 추정, 평가를 직접 지정할 수 있지만, 파이프라인을 이용하면 간편함

#### 저수준 데이터 타입

- 머신러닝 모델에 전달되는 일련의 특징은 Double 타입으로 구성된 Vector 형태로 전달되어야.
- SparseVector : 희소한 벡터. 인덱스로 값 지정.
- DenseVector : 밀도가 높은 벡터. 배열.

```scala
import org.apache.spark.ml.linalg.Vectors
val denseVec = Vectors.dense(1.0, 2.0, 3.0)
val size = 3
val idx = Array(1,2) // locations of non-zero elements in vector
val values = Array(2.0,3.0)
val sparseVec = Vectors.sparse(size, idx, values)
sparseVec.toDense
denseVec.toSparse
```
```console
output :
    import org.apache.spark.ml.linalg.Vectors
    denseVec: org.apache.spark.ml.linalg.Vector = [1.0,2.0,3.0]
    size: Int = 3
    idx: Array[Int] = Array(1, 2)
    values: Array[Double] = Array(2.0, 3.0)
    sparseVec: org.apache.spark.ml.linalg.Vector = (3,[1,2],[2.0,3.0])
    res3: org.apache.spark.ml.linalg.SparseVector = (3,[0,1,2],[1.0,2.0,3.0])
```

---
## 24.4 MLlib 실제로 사용하기

- 예제 데이터 : Y(범주형 1개), X(범주형 1개, 수치형 2개)
```scala
var df = spark.read.json("data/simple-ml")
df.orderBy("value2").show()
```
```console
output :
    +-----+----+------+------------------+
    |color| lab|value1|            value2|
    +-----+----+------+------------------+
    |green|good|     1|14.386294994851129|
    |green| bad|    16|14.386294994851129|
    | blue| bad|     8|14.386294994851129|
    | blue| bad|     8|14.386294994851129|
    | blue| bad|    12|14.386294994851129|
    ...
```

### 24.4.1 변환자를 사용해서 피처 엔지니어링 수행하기

- 컬럼 조작 : 트징 수 줄이기, 더 많은 특징 추가, 특징 조작, 단순히 데이터 구성 등
- 입력 변수 : Doulbe 타입 (Y), Vector[Double] 타입 (X)
- RFormula : 머신러닝에서 데이터 변환을 지정하기 위한 선언적 언어. R 연산자 일부 지원
  - ~ : target(Y)과 term(X) 분리
  - \+ : 연결 기호. '+ 0' 절편 제거를 의미
  - \- : 삭제 기호. '- 1' 절편 제거를 의미
  - : : 상호 작용 (수치형 값이나 이진화된 범주 값에 대한 곱셈)
  - . : target을 제외한 모든 컬럼

```scala
import org.apache.spark.ml.feature.RFormula
val supervised = new RFormula()
  .setFormula("lab ~ . + color:value1 + color:value2")
```
```console
output :
    import org.apache.spark.ml.feature.RFormula
    supervised: org.apache.spark.ml.feature.RFormula = RFormula(lab ~ . + color:value1 + color:value2) (uid=rFormula_487c30b0411d)
```
- RFormula 변환자를 데이터에 적합
- 학습된 변환자는 항상 Model이라는 단어와 함께 쓰인다.
- 이 변환자를 사용하면 스파크는 자동으로 범주형 변수를 Double 타입으로 변환한다.
```scala
val fittedRF: org.apache.spark.ml.feature.RFormulaModel = supervised.fit(df)
val preparedDF: org.apache.spark.sql.DataFrame = fittedRF.transform(df)
preparedDF.show()
preparedDF.select("label", "features").collect.foreach(println)
```
```console
output :
    +-----+----+------+------------------+--------------------+-----+
    |color| lab|value1|            value2|            features|label|
    +-----+----+------+------------------+--------------------+-----+
    |green|good|     1|14.386294994851129|(10,[1,2,3,5,8],[...|  1.0|
    | blue| bad|     8|14.386294994851129|(10,[2,3,6,9],[8....|  0.0|
    ...
    |  red| bad|     1| 38.97187133755819|(10,[0,2,3,4,7],[...|  0.0|
    |  red| bad|     2|14.386294994851129|(10,[0,2,3,4,7],[...|  0.0|
    +-----+----+------+------------------+--------------------+-----+
    only showing top 20 rows

    [1.0,(10,[1,2,3,5,8],[1.0,1.0,14.386294994851129,1.0,14.386294994851129])]
    [0.0,(10,[2,3,6,9],[8.0,14.386294994851129,8.0,14.386294994851129])]
    [0.0,(10,[2,3,6,9],[12.0,14.386294994851129,12.0,14.386294994851129])]
    [1.0,(10,[1,2,3,5,8],[1.0,15.0,38.97187133755819,15.0,38.97187133755819])]
    [1.0,(10,[1,2,3,5,8],[1.0,12.0,14.386294994851129,12.0,14.386294994851129])]
    [0.0,(10,[1,2,3,5,8],[1.0,16.0,14.386294994851129,16.0,14.386294994851129])]
    [1.0,(10,[0,2,3,4,7],[1.0,35.0,14.386294994851129,35.0,14.386294994851129])]
    [0.0,(10,[0,2,3,4,7],[1.0,1.0,38.97187133755819,1.0,38.97187133755819])]
    ...
    fittedRF: org.apache.spark.ml.feature.RFormulaModel = RFormulaModel(ResolvedRFormula(label=lab, terms=[color,value1,value2,{color,value1},{color,value2}], hasIntercept=true)) (uid=rFormula_487c30b0411d)
    preparedDF: org.apache.spark.sql.DataFrame = [color: string, lab: string ... 4 more fields]
```

- 학습셋과 테스트셋을 분리
```scala
val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3))
```

### 24.4.2 추정자

- 분류기를 생성하기 위해 기본 설정값 또는 하이퍼파라미터를 사용하여 로지스틱 회귀 알고리즘을 객체화
- 이후 레이블 컬럼과 특징 컬럼 설정
```scala
import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
println(lr.explainParams())
```
```console
output :
    aggregationDepth: suggested depth for treeAggregate (>= 2) (default: 2)
    elasticNetParam: the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty (default: 0.0)
    family: The name of family which is a description of the label distribution to be used in the model. Supported options: auto, binomial, multinomial. (default: auto)
    featuresCol: features column name (default: features, current: features)
    fitIntercept: whether to fit an intercept term (default: true)
    labelCol: label column name (default: label, current: label)
    lowerBoundsOnCoefficients: The lower bounds on coefficients if fitting under bound constrained optimization. (undefined)
    lowerBoundsOnIntercepts: The lower bounds on intercepts if fitting under bound constrained optimization. (undefined)
    maxIter: maximum number of iterations (>= 0) (default: 100)
    predictionCol: prediction column name (default: prediction)
    probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities (default: probability)
    rawPredictionCol: raw prediction (a.k.a. confidence) column name (default: rawPrediction)
    regParam: regularization parameter (>= 0) (default: 0.0)
    standardization: whether to standardize the training features before fitting the model (default: true)
    threshold: threshold in binary classification prediction, in range [0, 1] (default: 0.5)
    thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold (undefined)
    tol: the convergence tolerance for iterative algorithms (>= 0) (default: 1.0E-6)
    upperBoundsOnCoefficients: The upper bounds on coefficients if fitting under bound constrained optimization. (undefined)
    upperBoundsOnIntercepts: The upper bounds on intercepts if fitting under bound constrained optimization. (undefined)
    weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0 (undefined)
```

- 알고리즘을 객체화한 다음에는 데이터에 적합시켜야.
```scala
val fittedLR = lr.fit(train)
```
```console
output :
    fittedLR: org.apache.spark.ml.classification.LogisticRegressionModel = LogisticRegressionModel: uid = logreg_0ec849a33556, numClasses = 2, numFeatures = 10
```

- 이제 모델을 사용 가능. 예측은 transform 메서드로 수행
```scala
fittedLR.transform(train).select("label", "prediction").show()
```
```console
+-----+----------+
|label|prediction|
+-----+----------+
|  0.0|       0.0|
|  0.0|       0.0|
|  0.0|       0.0|
...
```

#### * 하이퍼파라미터 : 머신러닝 알고리즘을 적용하기 위해 분석가가 설정해야 하는 매개변수. 학습 전에 설정됨.
#### * 표준화 standardization : 평균을 기준으로 관측값들이 얼마나 떨어져 있는지 재표현하는 방법.
        z-transformation, one-hot encoding, value - 0.5 / max value 등
#### * 정규화 normalization : 데이터의 범위를 바꾸는 방법
#### * 일반화 regularization : 모델 과적합을 방지하기 위한 기법
        리지 회귀, 라쏘, 엘라스틱넷, 최소각 회귀 등
#### * 머신러닝에서 표준화 및 정규화는 일종의 스케일링 방법으로 주로 전처리 과정에서 사용
#### * 머신러닝에서 일반화는 모델의 일반화 오류를 줄여서 과적합을 방지

### 24.4.3 워크플로를 파이프라인으로 만들기

- 여러 단계를 파이프라인으로 구성 가능
- 파이프라인을 사용하면 튜닝된 모델을 얻을 수 있음
- **변환자 객체** 나 **모델 객체** 가 다른 파이프라인에서 재사용되지 않는 것이 중요

<img src="https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/assets/spdg_2404.png" width=70% style="border: 1px solid gray">


- 검증셋을 기반으로 하이퍼 파라미터 조정해야 하는데, 검증셋은 원시 데이터로 작업해야 함

```scala
val Array(train, test) = df.randomSplit(Array(0.7, 0.3))
```

- 파이프라인에 RFromula, Logistic Regression 두 개의 추정자를 넣어서 구성할 수 있음
```scala
val rForm = new RFormula()
val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")

import org.apache.spark.ml.Pipeline
val stages = Array(rForm, lr)
val pipeline = new Pipeline().setStages(stages)
```
```console
output :
    import org.apache.spark.ml.Pipeline
    stages: Array[org.apache.spark.ml.Estimator[_ >: org.apache.spark.ml.classification.LogisticRegressionModel with org.apache.spark.ml.feature.RFormulaModel <: org.apache.spark.ml.Model[_ >: org.apache.spark.ml.classification.LogisticRegressionModel with org.apache.spark.ml.feature.RFormulaModel <: org.apache.spark.ml.Transformer with org.apache.spark.ml.param.shared.HasLabelCol with org.apache.spark.ml.param.shared.HasFeaturesCol with org.apache.spark.ml.util.MLWritable] with org.apache.spark.ml.param.shared.HasLabelCol with org.apache.spark.ml.param.shared.HasFeaturesCol with org.apache.spark.ml.util.MLWritable] with org.apache.spark.ml.param.shared.HasLabelCol with org.apache.spark.ml.param.shared.HasFeaturesCol with org.apache.spark.ml.util.DefaultP...

```

### 24.4.4 모델 학습 및 평가

- 다양한 하이퍼파라미터의 조합을 지정해 다양한 모델을 학습
- 평가기를 사용하여 **검증셋**으로 각 모델의 예측 결과를 비교 후 최적의 모델 선택

```scala
import org.apache.spark.ml.tuning.ParamGridBuilder
val params = new ParamGridBuilder()
  .addGrid(rForm.formula, Array(
    "lab ~ . + color:value1",
    "lab ~ . + color:value1 + color:value2"))
  .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
  .addGrid(lr.regParam, Array(0.1, 2.0))
  .build() // 2 x 3 x 2 가지 케이스
```
```console
output :
    import org.apache.spark.ml.tuning.ParamGridBuilder
    params: Array[org.apache.spark.ml.param.ParamMap] =
    Array({
        logreg_28204f6d67f5-elasticNetParam: 0.0,
        rFormula_8099b1751546-formula: lab ~ . + color:value1,
        logreg_28204f6d67f5-regParam: 0.1
    }, {
        logreg_28204f6d67f5-elasticNetParam: 0.0,
        rFormula_8099b1751546-formula: lab ~ . + color:value1 + color:value2,
        logreg_28204f6d67f5-regParam: 0.1
    }, {
        logreg_28204f6d67f5-elasticNetParam: 0.5,
        rFormula_8099b1751546-formula: lab ~ . + color:value1,
        logreg_28204f6d67f5-regParam: 0.1
    }, {
        logreg_28204f6d67f5-elasticNetParam: 0.5,
        rFormula_8099b1751546-formula: lab ~ . + color:value1 + color:value2,
        logreg_28204f6d67f5-regParam: 0.1
    }, {
        logreg_28204f6d67f5-elasticNetParam: 1.0,
        rFormula_8099b1751546-formula: lab ~ . + color:value1,
        ...
```

- 평가기를 통해 객관적 기준으로 모델 비교 가능
- 
```scala
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val evaluator = new BinaryClassificationEvaluator()
  .setMetricName("areaUnderROC")
  .setRawPredictionCol("prediction")
  .setLabelCol("label")
```

- TrainValidationSplit : 데이터를 두개의 서로 다른 그룹으로 무작위로 임의 분할
- CrossValidator는 데이터 집합을 겹치지 않게 임의로 구분된 k개의 폴드로 분할하여 교차 검증 수행

```scala
import org.apache.spark.ml.tuning.TrainValidationSplit
val tvs = new TrainValidationSplit()
  .setTrainRatio(0.75) // also the default.
  .setEstimatorParamMaps(params) // 하이퍼파라미터 종류
  .setEstimator(pipeline)        // 파이프라인
  .setEvaluator(evaluator)       // 평가기

val tvsFitted = tvs.fit(train) // 얻어진 최적 모델
```
```console
output :
    tvsFitted: org.apache.spark.ml.tuning.TrainValidationSplitModel = tvs_a5fe5ed91927
```

- 모델의 transform 으로 테스트세 데이터에 대해 예측
- 예측한 DF를 평가기의 evalucate로 최종 모델 평가
```scala
evaluator.evaluate(tvsFitted.transform(test))
```
```console
output :
    res36: Double = 0.9666666666666667
```

- 알고리즘이 최적의 모델을 도출하기 위해 진행하고 있는 과정을 확인 가능
  
```scala
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel
val trainedPipeline = tvsFitted.bestModel.asInstanceOf[PipelineModel]
val TrainedLR = trainedPipeline.stages(1).asInstanceOf[LogisticRegressionModel]
val summaryLR = TrainedLR.summary
summaryLR.objectiveHistory // 0.6751425885789243, 0.5543659647777687, 0.473776...
```
```console
output :
    import org.apache.spark.ml.PipelineModel
    import org.apache.spark.ml.classification.LogisticRegressionModel
    trainedPipeline: org.apache.spark.ml.PipelineModel = pipeline_9ebd8079446d
    TrainedLR: org.apache.spark.ml.classification.LogisticRegressionModel = LogisticRegressionModel: uid = logreg_28204f6d67f5, numClasses = 2, numFeatures = 7
    summaryLR: org.apache.spark.ml.classification.LogisticRegressionTrainingSummary = org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummaryImpl@38a5ec7e
    res37: Array[Double] = Array(0.6812657296632041, 0.5899346944480829, 0.5222380691050227, 0.4726491448920599, 0.46144842276295756, 0.4545074412720459, 0.45178257279667433, 0.44936868874607405, 0.44667029029190597, 0.444612137742413, 0.44365611486324813, 0.44359661291885666, 0.4435947433230...
```


### 24.4.5 모델 저장 및 적용

- **모델**을 디스크에 저장 가능
```scala
tvsFitted.write.overwrite().save("/tmp/modelLocation")
```

- 디스크에 저장된 **모델**을 로드하여 사용 가능
```scala
import org.apache.spark.ml.tuning.TrainValidationSplitModel
val model = TrainValidationSplitModel.load("/tmp/modelLocation")
val result = model.transform(test)
result.show
```
```console
output :
    +-----+----+------+------------------+--------------------+-----+--------------------+--------------------+----------+
    |color| lab|value1|            value2|            features|label|       rawPrediction|         probability|prediction|
    +-----+----+------+------------------+--------------------+-----+--------------------+--------------------+----------+
    | blue| bad|     8|14.386294994851129|(7,[2,3,6],[8.0,1...|  0.0|[1.93336946413028...|[0.87362189977703...|       0.0|
    | blue| bad|     8|14.386294994851129|(7,[2,3,6],[8.0,1...|  0.0|[1.93336946413028...|[0.87362189977703...|       0.0|
    | blue| bad|     8|14.386294994851129|(7,[2,3,6],[8.0,1...|  0.0|[1.93336946413028...|[0.87362189977703...|       0.0|
    | blue| bad|     8|14.386294994851129|(7,[2,3,6],[8.0,1...|  0.0|[1.93336946413028...|[0.87362189977703...|       0.0|
    | blue| bad|    12|14.386294994851129|(7,[2,3,6],[12.0,...|  0.0|[2.27770265400771...|[0.90701346972999...|       0.0|
    | blue| bad|    12|14.386294994851129|(7,[2,3,6],[12.0,...|  0.0|[2.27770265400771...|[0.90701346972999...|       0.0|
    | blue| bad|    12|14.386294994851129|(7,[2,3,6],[12.0,...|  0.0|[2.27770265400771...|[0.90701346972999...|       0.0|
    | blue| bad|    12|14.386294994851129|(7,[2,3,6],[12.0,...|  0.0|[2.27770265400771...|[0.90701346972999...|       0.0|
    |green| bad|    16|14.386294994851129|[0.0,1.0,16.0,14....|  0.0|[-0.1642908969054...|[0.45901941162539...|       1.0|
    |green|good|     1|14.386294994851129|[0.0,1.0,1.0,14.3...|  1.0|[-0.4117606725661...|[0.39849002013486...|       1.0|
    |green|good|    12|14.386294994851129|[0.0,1.0,12.0,14....|  1.0|[-0.2302828370816...|[0.44268236420811...|       1.0|
    |green|good|    12|14.386294994851129|[0.0,1.0,12.0,14....|  1.0|[-0.2302828370816...|[0.44268236420811...|       1.0|
    ...
    only showing top 20 rows

    import org.apache.spark.ml.tuning.TrainValidationSplitModel
    model: org.apache.spark.ml.tuning.TrainValidationSplitModel = tvs_a5fe5ed91927
    res39: org.apache.spark.sql.DataFrame = [color: string, lab: string ... 7 more fields]
```


---
## 24.5 모델 배포 방식

<img src="https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/assets/spdg_2405.png" width=70% style="border: 1px solid gray">

- 배포 방법 1) 오프라인 학습, 오프라인 적용. 신속하게 응답해야 하는 데이터가 아니라 분석을 위해 저장된 데이터에 적용
- 배포 방법 2) 오프라인 학습, DB(K-V 저장소)에 결과 저장. 추천 분야에 적합. 분류/회귀에는 부적합
- 배포 방법 3) 오프라인 학습. 모델을 디스크에 저장 후 서비스. 스파크를 서비스에서 사용하는 경우 오버헤드 문제
- 배포 방법 4) 단일 시스템상에서 사용자의 분산 모델을 훨씬 더 빠르게 수행하도록 수동 변환. 유지보수 어려울 수 있음. PMML 활용 
- 배포 방법 5) 온라인 학습, 온라인 사용. 구조적 스트리밍과 함께 사용할 때 가능. 복잡해질 수 있음.

