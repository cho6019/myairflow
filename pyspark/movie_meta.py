from pyspark.sql import SparkSession, DataFrame
import sys
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    #handlers = [logging.StreamHandler(sys.stdout)]
)

spark = SparkSession.builder.appName("movie_meta").getOrCreate()
exit_code = 0 #0면 정상 종료, 1이면 비정상 종료

def save_meta(df: DataFrame, meta_path: str):
    df.write.mode("overwrite").parquet(meta_path)
    logging.info("meta 저장 완료", meta_path)

try:
    if len(sys.argv) != 4:
        raise ValueError("필수 인자가 누락되었습니다")
    
    raw_path, mode, meta_path = sys.argv[1:4]
    
    meta_today = spark.read.parquet(raw_path).select("movieCd","multiMovieYn", "repNationCd")
    
    if mode == "create":
        save_meta(meta_today, meta_path)
    elif mode == "append":
        meta_yesterday = spark.read.parquet(meta_path)
        meta_yesterday.createOrReplaceTempView("temp_meta_yesterday")
        meta_today.createOrReplaceTempView("temp_meta_today")
        
        updated_meta = spark.sql("""
            SELECT
                movieCd,
                MAX(multiMovieYn) AS multiMovieYn,
                MAX(repNationCd) AS repNationCd
            FROM(
            SELECT 
                COALESCE(y.movieCd, t.movieCd) AS movieCd,
                COALESCE(y.multiMovieYn, t.multiMovieYn) AS multiMovieYn,
                COALESCE(y.repNationCd, t.repNationCd) AS repNationCd
            FROM temp_meta_yesterday y
            FULL OUTER JOIN temp_meta_today t
            ON y.movieCd = t.movieCd
            )
            GROUP BY movieCd
            """)
        updated_meta.createOrReplaceTempView("temp_meta_update")
        
        # ASSERT_TRUE - updated meta 는 항상 원천 소스 보다 크면 정산 / else 실패
    
        spark.sql("""
        SELECT ASSERT_TRUE(
            (SELECT COUNT(*) FROM temp_meta_update) > (SELECT COUNT(*) FROM temp_meta_yesterday) AND
            (SELECT COUNT(*) FROM temp_meta_update) > (SELECT COUNT(*) FROM temp_meta_today)
        ) AS is_valid
        
        /*
        WITH counts AS (
            SELECT 
                (SELECT COUNT(*) FROM temp_meta_update) AS count_update,
                (SELECT COUNT(*) FROM temp_meta_yesterday) AS count_yesterday,
                (SELECT COUNT(*) FROM temp_meta_today) AS count_today
        )
        SELECT ASSERT_TRUE(
            count_update > count_yesterday AND count_update > count_today
        ) AS is_valid
        FROM counts
        */
        """)
        spark.sql("""
            SELECT ASSERT_TRUE(
                (SELECT COUNT(DISTINCT movieCd) FROM temp_meta_update) == (SELECT COUNT(*) FROM temp_meta_update
                )) AS is_unique
                """)
        
        save_meta(updated_meta, meta_path)
    else:
        raise ValueError(f"알 수 없는 MODE: {mode}")
    
except Exception as e:
    logging.error(f"오류: {str(e)}")
    exit_code = 1
finally:
    spark.stop()
    sys.exit(exit_code)