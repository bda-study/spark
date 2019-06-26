# Chapter 5 구조적 API 기본 연산 (124~, 31)


- DataFrame

. Row : 레코드. 바이트 배열. 스키마 정보 없음. 순서 의미 있음

. Column : 연산 표현식

. 파티셔닝 : 물리적으로 배치되는 형태

. 파티셔닝 스키마 : 파티션을 배치하는 방법 정의 (nondeterministic algorithm, not purely function)

> df.printSchema

> df.schema

> df.columns

1. Column

- Schema : 컬럼명과 데이터 타입. StructField (와 StructType)를 통해 StructType을 구성

* 자동으로 스키마를 얻으면 원하지 않는 형태로 변환될 가능성 있음

- StructField : 이름, 타입, nullable, 메타데이터

- 컬럼 표현 : df.col(“name”) col(“name"), column(“name"), $”name”, ‘name, expr(“name”)

- 표현식 : expr(“….”)

- select : 문자열 1개 이상, Column타입 1개 이상

* 문자열, Column 타입 섞어 쓰기 안됨

- selectExpr = select(exprt(….))

: “*” 사용 가능

- lit : literal로 상수 또는 프로그래밍 언어의 변수 사용 가능

- withColumn : 컬럼 추가

- withColumnRenamed : 컬럼명 변경

- drop : 컬럼 제거 (1개 이상 가능)

- column.cast : 타입 변경 ex. col(“name”).cast(“string”)

- 예약문자, 키워드 사용을 위해 ` (백틱)

- 대소문자 구분 : set spark.sql.caseSensitive true ( in SQL )

2. Row

- 참조 : myRow(0), myRow(0).asInstanceOf[String], myRow.getString(0)

- 생성 예시

> import org.apache.spark.sql.Row

> import org.apache.spark.sql.types.{StructField, StructType, StringType}

> val schema = new StructType(Array(new StructField(“d1”, StringType, true), new StructField(“d2”, StringType, true)))

> val rdd = spark.sparkContext.parallelize( Seq( Row(“a”, “b”) ) )

> val df = spark.createDataFrame(rdd, schema)

> spark.implicits._

> val df = Seq( Row(“a”, “b”).toDF

- df.filter / df.where : 조건 필터링

- df.distinct : 고유 로우 얻기

- sample(복원추출여부, 비율, 시드) : 무작위 샘플

- randomSplit( Array(비율1, 비율2), 시드) : 무작위 분할

- union : 컬럼 위치 기반으로 통합

- sort / orderBy : expr(“name”), desc(“name”), asc_null_first(“name”), desc_null_last(“name”), … : 기본은 오름차순

- sortWithinPartitions(“name”) : 파티션 안에서 정렬

- limit : 로우 짜르기

- repartition : 파티셔닝 스키마와 파티션 수를 포함해 클러스터 전반의 물리적 데이터 구성 제어. 전체 데이터 셔플 발생

- coalesce : 파티션 병합. 항상 셔플 발생하는 건 아님

- take(n), show(n, 20자 넘으면 짜를지), collect : 드라이버로 모으기

- toLocalIterator : 파티션을 차례로 모으기
