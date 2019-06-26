# Chapter 6 다양한 데이터 타입 다루기 (155~, 46)

스파크 데이터 타입으로의 변환

    import org.apache.spark.sql.functions.lit
    df.select( lit(5), lit("five"), lit(5.0) )
    
    sql(""" select 5, "five", 5.0 """)

Boolean Type

기본 사용 예

    
    import org.apache.spark.sql.functions.col
    df.where( col("InvoiceNo") =!= 12345 )
    
    df.where( "InvoiceNo = 12345" )
      .where( "InvoiceNo <> 12345" )

and 메소드를 chaining해서 사용하면 좋다. 결국 한 문장으로 바뀐다.

    // better
    df.where( 
      col("StockCode").isin("DOT") 
      and
      col("InvoiceNo").isin("536544", "536592")
    )
    
    // good
    df.where( """
      StockCode in ("DOT")
      and
      InvoiceNo in ("536544", "536592")
    """ )

boolean column을 활용한 filtering

    df.withColumn("isExpensive", col("UnitPrice") > 60)
      .where("isExpensive")

수치형 데이터

올림/내림

    > sql(""" select round(2.5), bround(2.5), round(2.4), bround(2.4) """).show
    +-------------+--------------+-------------+--------------+
    |round(2.5, 0)|bround(2.5, 0)|round(2.4, 0)|bround(2.4, 0)|
    +-------------+--------------+-------------+--------------+
    |            3|             2|            2|             2|
    +-------------+--------------+-------------+--------------+

 Stat 함수

    > df.stat.corr("Quantity", "UnitPrice")
    res7: Double = -0.04112314436835551
    
    > import org.apache.spark.sql.functions.{corr}
    > df.select( corr("Quantity", "UnitPrice") ).show
    +-------------------------+
    |corr(Quantity, UnitPrice)|
    +-------------------------+
    |     -0.04112314436835551|
    +-------------------------+

문자열 데이터

initcap : 공백으로 나뉜 모든 단어의 첫 글자를 대문자로 변경

lower : 모두 소문자로 변경

upper : 모두 대문자로 변경

ltrim / rtim : 왼쪽 / 오른쪽 공백 지우기

trim : 양쪽 공밷지우기

lpad : 왼쪽 채우기, 짧으면 오른쪽부터 지우기

rpad : 오른쪽 채우기, 짧으면 오른쪽부터 지우기

regexp_extract : 추출

regexp_replace : 변환

translate : 문자 단위로 변환

가변인자 처리 방법

seq:_*    ← 이렇게 해서 함수의 인자로 넘길 수 있음

날짜/타임스탬프 데이터

date : 달력 형태의 날짜. current_date()

        : yyyy-dd-MM 연월일까지만

timestamp : 날짜와 시간 정보 포함. currnet_timestamp()

        : yyyy-MM-dd HH:mm:ss 초단위까지만

NULL

스키마의 nullable은 강제성 없음. spark sql 옵티마이저가 해당 컬럼을 제어하는 동작을 단순히 도울뿐.

coalesce (n1, n2, ...) : null 아닌 첫번째 값

ifnull / nvl (n1, n2) : n1이 null이면 n2, 아니면 n1

nullif (n1, n2) : 두 값이 같으면 null, 아니면 n1

nvl2 (n, r, e) : n이 null 이면 r, 아니면 e

df.na.drop() // default drop("any") : 하나라도 null일때

df.na.drop("all") : 모두 null일때

df.na.drop("all", Seq("n1", n2")) : 특정 컬럼에 대해서만 모두 null일때

df.na.fill("xx" : String) : 타입 확인해서 채우기

df.na.fill( Map("n1" -> 5) ) : 컬럼 채우기

df.na.replace("Description, Map("" -> "UNKNOWN")) : 기존 값을 새 값으로 바꾸기

복합 데이터 (struct, array, map)

struct

    df.selectExpr("struct(Description, InvoiceNo) as c") // struct 생략 가능
      .selectExpr("c.InvoiceNo") // c.* 가능

array

    df.select( split( 'Description, " ")(0) )

explode

    df.withColumn("splitted", split('Description, " "))
      .withColumn("exploded", explode('splitted))
      .select("Description", "InvoiceNo", "exploded")
      .show(5)
    +--------------------+---------+--------+
    |         Description|InvoiceNo|exploded|
    +--------------------+---------+--------+
    |WHITE HANGING HEA...|   536365|   WHITE|
    |WHITE HANGING HEA...|   536365| HANGING|
    |WHITE HANGING HEA...|   536365|   HEART|
    |WHITE HANGING HEA...|   536365| T-LIGHT|
    |WHITE HANGING HEA...|   536365|  HOLDER|
    +--------------------+---------+--------+
    only showing top 5 rows

Map