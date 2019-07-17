# Chapter 31. 딥러닝

딥러닝은 아직 새로운 분야이기 때문에 스파크에서 제공하는 딥러닝 도구 대부분이 외부 라이브러리다.

아래에서는 스파크에서 딥러닝을 사용하기 위한 방법을 알아보자.

## 딥러닝이란

> Deep learning (also known as deep structured learning or hierarchical learning) is part of a broader family of machine learning methods based on artificial neural networks. Learning can be supervised, semi-supervised or unsupervised.
>
> 딥 러닝 또는 심층학습(深層學習, 영어: deep structured learing, deep learning 또는 hierarchical learning)은 여러 비선형 변환기법의 조합을 통해 높은 수준의 추상화(abstractions, 다량의 데이터나 복잡한 자료들 속에서 핵심적인 내용 또는 기능을 요약하는 작업)를 시도하는 기계학습 알고리즘의 집합으로 정의되며, 큰 틀에서 사람의 사고방식을 컴퓨터에게 가르치는 기계학습의 한 분야라고 이야기할 수 있다.
>
> *Wikipedia 설명 인용*

![](https://learning.oreilly.com/library/view/spark-the-definitive/9781491912201/assets/spdg_3101.png)

- 대규모 데이터에 대해서 기존 머신러닝 알고리즘의 한계를 극복
- 컴퓨터 비전, 음성 인식, 자연어 처리 등 수 많은 분야에 활용

## 스파크에서 딥러닝을 사용하는 방법

### 1. 추론

- 학습된 모델을 사용해 분류, 인식 등을 수행

### 2. 특징 생성과 전이 학습(Transfer Learning)

- 학습된 모델을 사용해 결과 도출이 아닌 **새로운 특징 생성**에 사용
- **전이학습**: 사전에 학습된 모델의 마지막 몇 개 계층을 또 다른 데이터셋으로 학습하여 새로운 문제 해결을 위한 모델 학습에 활용하는 방법
- 모델을 생성하려면 많은 데이터셋이 필요하나, 전이학습은 작은 데이터셋으로도 구현 가능

### 3. 모델 학습

- 단일 모델 학습 vs 다수의 모델 학습 및 최종 모델 선택
- 병렬 처리 기반 vs 단일 머신 기반
- 위의 모든 경우에 대해 일반적인 처리가 가능하며, ETL 어플리케이션과 함께 워크플로 구성 가능

## 딥러닝 라이브러리

### 1. MLlib에서 지원하는 신경망

- `ml.classification.MultilayerPerceptronClassifier` Class 사용
- 다층 퍼셉트론 분류기와 같은 단일 심층 학습 알고리즘 지원
- [시그모이드 활성화 함수](https://en.wikipedia.org/wiki/Sigmoid_function)를 사용하는 완전 연결 계층(이전 계층의 모든 뉴런과 결합된 형태의 계층) 사용
- [소프트맥스 활성화 함수](https://en.wikipedia.org/wiki/Softmax_function)를 사용하는 출력 계층 사용
- 상대적으로 얕은 네트워크를 학습하도록 제한되며, 전이 학습에 사용할 때 유용
- 사용예제 참고 - https://iamksu.tistory.com/49

### 2. 텐서프레임 (TensorFrames)

- https://github.com/databricks/tensorframes
- Databricks에서 개발한 텐서플로우의 스파크 DataFrame wrapper
- 텐서플로우에서 스파크로 데이터를 전달하기 위한 단순하지만 최적화된 인터페이스 제공
- 추론, 스트리밍, 배치 설정, 전이 학습으로 유용

### 3. BigDL

- https://github.com/intel-analytics/BigDL
- 인텔에서 개발한 스파크의 분산 딥러닝 프레임워크
- 다른 라이브러리에 비해 CPU 활용에 최적화되어있는 특장점
- 하둡과 같은 CPU 기반 클러스터에서도 효율적으로 구현 가능
- 기본적으로 모든 작업을 분산 처리
- Caffe, Torch, Keras 모델 사용 가능

### 4. TensorFlowOnSpark

- https://github.com/yahoo/TensorFlowOnSpark
- 야후에서 개발했으며, 스파크 클러스터 상에서 텐서플로 모델을 병렬 학습하기 위한 프레임워크
- 스파크의 ML 파이프라인 API와 통합

### 5. DeepLearning4J (DL4J)

- https://github.com/eclipse/deeplearning4j
- 단일, 분산 환경 모두 학습을 지원하며 JVM에 최적화된 Java/Scala의 오픈소스 프레임워크
- 2017년 10월부터 이클립스 재단에 합류함
- [Skymind](https://skymind.ai/platform)에서 제공하는 `Skymind Intelligence Layer`라는 엔터프라이즈 배포판을 통해 DL4J가 포함된 상업 버전을 지원받을 수 있음

### 6. 딥러닝 파이프라인

- https://github.com/databricks/spark-deep-learning
- 딥러닝 기능을 스파크의 ML 파이프라인 API에 통합시킨 오픈소스 패키지
- 아직까지는 텐서플로우, 케라스 두 개의 프레임워크만 지원
- 표준 스파크 API와 통합하며, 모든 연산을 분산처리
- 최종적으로는 ML 파이프라인 API에서 모든 딥러닝 기능을 사용할 수 있도록 하는 것이 목표
- 다음 내용부터는 해당 라이브러리 기준으로 설명

## 딥러닝 파이프라인을 사용한 간단한 예제

### 1. 설정하기

- 다른 스파크 패키지를 로드하는 방식과 동일하게 진행
- 파이썬 종속 라이브러리 설치 확인 - 텐서프레임, 텐서플로, 케라스, h5py 등
- 플라워 데이터셋 다운로드: https://www.tensorflow.org/hub/tutorials/image_retraining
- 예제 구동 환경 세팅: https://github.com/FVBros/Spark-The-Definitive-Guide

### 2. 이미지와 DataFrame

- `readImages`: 분산 처리 방식으로 이미지 로드, 디코딩을 해주는 유틸리티 함수
- 스파크 2.3에 정식으로 포함
- 이미지 로딩 example

    ```python
    from sparkdl import readImages
    img_dir = '/data/deep-learning-images/'
    image_df = readImages(img_dir)

    image_df.printSchema()
    ```

    ```text
    root
    |-- filePath: string (nullable = false)
    |-- image: struct (nullable = true)
    |    |-- mode: string (nullable = false)
    |    |-- height: integer (nullable = false)
    |    |-- width: integer (nullable = false)
    |    |-- nChannels: integer (nullable = false)
    |    |-- data: binary (nullable = false)
    ```

### 3. 전이 학습

- 유형별 꽃에 대한 데이터를 로드하고 학습셋, 테스트셋 생성

    ```python
    from sparkdl import readImages
    from pyspark.sql.functions import lit
    tulips_df = readImages(img_dir + "/tulips").withColumn("label", lit(1))
    daisy_df = readImages(img_dir + "/daisy").withColumn("label", lit(0))
    tulips_train, tulips_test = tulips_df.randomSplit([0.6, 0.4])
    daisy_train, daisy_test = daisy_df.randomSplit([0.6, 0.4])
    train_df = tulips_train.unionAll(daisy_train)
    test_df = tulips_test.unionAll(daisy_test)
    ```

- `DeepImageFeaturizer` 변환자를 사용해 `인셉션(Inception)`이라는 보편적인 사물 인식을 위해 사전 학습된 모델을 꽃을 인식하기 위한 모델로 수정한다.
- 위의 작업은 결국 로지스틱 회귀 모델을 추가하여 최종 모델의 학습을 쉽게 하기 위한 작업이며, 로지스틱 회귀 대신 다른 분류기를 사용할 수 있다.
- 모델 추가 example (매우 오래 걸릴 수 있음)

    ```python
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml import Pipeline
    from sparkdl import DeepImageFeaturizer
    featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features",
    modelName="InceptionV3")
    lr = LogisticRegression(maxIter=1, regParam=0.05, elasticNetParam=0.3,
    labelCol="label")
    p = Pipeline(stages=[featurizer, lr])
    p_model = p.fit(train_df)
    ```

    ```text
    2019-07-16 13:24:41.806029: I tensorflow/core/platform/cpu_feature_guard.cc:141] Your CPU supports instructions that this TensorFlow binary was not compiled to use: AVX2 FMA
    Downloading data from https://github.com/fchollet/deep-learning-models/releases/download/v0.5/inception_v3_weights_tf_dim_ordering_tf_kernels_notop.h5

    16384/87910968 [..............................] - ETA: 0s
    40960/87910968 [..............................] - ETA: 7:24
    90112/87910968 [..............................] - ETA: 6:21
    212992/87910968 [..............................] - ETA: 4:15
    442368/87910968 [..............................] - ETA: 2:39
    696320/87910968 [..............................] - ETA: 2:06
    1236992/87910968 [..............................] - ETA: 1:14
    1417216/87910968 [..............................] - ETA: 1:14
    2146304/87910968 [..............................] - ETA: 50s 
    ...
    ```

- 모델 학습이 완료되면 분류 평가기를 통해 평가 수행

    ```python
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    tested_df = p_model.transform(test_df)
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    print("Test set accuracy = " + str(evaluator.evaluate(tested_df.select(
    "prediction", "label"))))
    ```

- 이전 학습에서 예측이 잘못된 로우와 이미지 검사

    ```python
    from pyspark.sql.types import DoubleType
    from pyspark.sql.functions import expr
    # a simple UDF to convert the value to a double
    def _p1(v):
    return float(v.array[1])
    p1 = udf(_p1, DoubleType())
    df = tested_df.withColumn("p_1", p1(tested_df.probability))
    wrong_df = df.orderBy(expr("abs(p_1 - label)"), ascending=False)
    wrong_df.select("filePath", "p_1", "label").limit(10).show()
    ```

### 4. 인기 있는 표준 모델 사용하기

- `DeepImagePredictor` 변환자를 통해 단순히 모델 이름만 지정하여 외부 표준 딥러닝 모델 사용
- DeepImagePredictor 사용 example

    ```python
    from sparkdl import readImages, DeepImagePredictor
    image_df = readImages(img_dir)
    predictor = DeepImagePredictor(
    inputCol="image",
    outputCol="predicted_labels",
    modelName="InceptionV3",
    decodePredictions=True,
    topK=10)
    predictions_df = predictor.transform(image_df)
    ```

- 위에서 적용한 기본 모델(InceptionV3)을 사용하면 daisy가 모든 샘플에 대해 높은 확률값을 갖게 됨. 그러나 확률값의 차이에서 보면 기본 모델을 기반으로 데이지와 튤립의 차이점을 적절히 학습함.

    ```python
    df = p_model.transform(image_df)
    df.select("image.origin", (1-p1(df.probability)).alias("p_daisy")).show()
    ```

#### 4.1 사용자 정의 케라스 모델 적용하기

- https://github.com/databricks/spark-deep-learning#for-keras-users-1
- 딥러닝 파이프라인을 통해 스파크에 케라스 모델을 적용 가능
- `KerasImageFileTransformer` 사용

    ```python
    from keras.applications import InceptionV3

    model = InceptionV3(weights="imagenet")
    model.save('/tmp/model-full.h5')
    ```

- on prediction side

    ```python
    from keras.applications.inception_v3 import preprocess_input
    from keras.preprocessing.image import img_to_array, load_img
    import numpy as np
    import os
    from pyspark.sql.types import StringType
    from sparkdl import KerasImageFileTransformer

    def loadAndPreprocessKerasInceptionV3(uri):
    # this is a typical way to load and prep images in keras
    image = img_to_array(load_img(uri, target_size=(299, 299)))  # image dimensions for InceptionV3
    image = np.expand_dims(image, axis=0)
    return preprocess_input(image)

    transformer = KerasImageFileTransformer(inputCol="uri", outputCol="predictions",
                                            modelFile='/tmp/model-full-tmp.h5',  # local file path for model
                                            imageLoader=loadAndPreprocessKerasInceptionV3,
                                            outputMode="vector")

    files = [os.path.abspath(os.path.join(dirpath, f)) for f in os.listdir("/data/myimages") if f.endswith('.jpg')]
    uri_df = sqlContext.createDataFrame(files, StringType()).toDF("uri")

    keras_pred_df = transformer.transform(uri_df)
    ```

#### 4.2 텐서플로 모델 사용하기

- https://github.com/databricks/spark-deep-learning#for-tensorflow-users
- 딥러닝 파이프라인을 통해 텐서플로우 모델 적용 가능
- `TFImageTransformer` 사용

    ```python
    from pyspark.ml.image import ImageSchema
    from sparkdl import TFImageTransformer
    import sparkdl.graph.utils as tfx  # strip_and_freeze_until was moved from sparkdl.transformers to sparkdl.graph.utils in 0.2.0
    from sparkdl.transformers import utils
    import tensorflow as tf

    graph = tf.Graph()
    with tf.Session(graph=graph) as sess:
        image_arr = utils.imageInputPlaceholder()
        resized_images = tf.image.resize_images(image_arr, (299, 299))
        # the following step is not necessary for this graph, but can be for graphs with variables, etc
        frozen_graph = tfx.strip_and_freeze_until([resized_images], graph, sess, return_graph=True)

    transformer = TFImageTransformer(inputCol="image", outputCol="predictions", graph=frozen_graph,
                                    inputTensor=image_arr, outputTensor=resized_images,
                                    outputMode="image")

    image_df = ImageSchema.readImages(sample_img_dir)
    processed_image_df = transformer.transform(image_df)
    ```

#### 4.3 SQL 함수를 사용하여 모델 전개하기

## 정리
