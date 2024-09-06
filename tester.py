import pandas as pd
import sqlite3

list = pd.read_csv('CodeArchive/pagepile.csv', header=None)

def read_sqlite_table_to_dataframe(db_path, table_name):
    try:
        # SQLite 데이터베이스에 연결
        conn = sqlite3.connect(db_path)
        
        # SQL 쿼리 실행 및 결과를 DataFrame으로 변환
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        
        # 연결 종료
        conn.close()
        
        print(f"'{table_name}' 테이블을 성공적으로 DataFrame으로 변환했습니다.")
        return df
    
    except sqlite3.Error as e:
        print(f"SQLite 오류 발생: {e}")
        return None
    except pd.io.sql.DatabaseError as e:
        print(f"pandas 데이터베이스 오류 발생: {e}")
        return None
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
        return None

def filter_and_analyze_qids(df, qid_list):
    # DataFrame을 qid로 필터링
    filtered_df = df[df['qid'].isin(qid_list)]
    
    # 결과 분석
    total_claims = len(filtered_df)
    result_counts = filtered_df['result'].value_counts()
    
    print(f"\n검사 결과:")
    print(f"총 검사된 주장 수: {total_claims}")
    print("\n결과 분포:")
    for result, count in result_counts.items():
        percentage = (count / total_claims) * 100
        print(f"{result}: {count} ({percentage:.2f}%)")
    
    return filtered_df

# 사용 예시
db_path = 'reference_checked.db'
table_name = 'aggregated_results'

qid_list_to_check = list[0]

df = read_sqlite_table_to_dataframe(db_path, table_name)
filtered_df = filter_and_analyze_qids(df, qid_list_to_check)

filtered_df.to_csv('CodeArchive/resultPagepile.csv')
