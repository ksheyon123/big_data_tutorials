# 진짜 빅데이터 예제 - 메모리 한계를 넘어서는 데이터 처리

import pandas as pd
import numpy as np
from dask import dataframe as dd
import time

print("=== 일반 pandas vs 빅데이터 처리 비교 ===\n")

# 1. 메모리 한계 시뮬레이션
print("1. 메모리 한계 상황 만들기")

def create_large_dataset():
    """
    실제로 메모리 한계에 부딪히는 상황을 시뮬레이션
    """
    print("대용량 가상 데이터 생성 중...")
    
    # 1억 행의 데이터 시뮬레이션 (실제로는 파일들로 분산 저장됨)
    # 실제 환경에서는 이런 크기의 데이터가 일반적
    
    chunk_size = 1000000  # 100만 행씩 처리
    total_rows = 10000000  # 1000만 행 (실제 빅데이터는 수십억 행)
    
    print(f"총 {total_rows:,}행 데이터 처리 예정")
    print(f"청크 크기: {chunk_size:,}행")
    
    return chunk_size, total_rows

chunk_size, total_rows = create_large_dataset()

print("\n" + "="*60)
print("2. 일반 pandas 방식 (메모리 부족 시뮬레이션)")

def pandas_approach_simulation():
    """
    일반 pandas로는 메모리 부족으로 처리 불가능한 상황
    """
    try:
        print("❌ pandas.read_csv('huge_file.csv')  # 메모리 부족!")
        print("❌ MemoryError: Unable to allocate 8.5 GiB for an array")
        print("❌ 일반적인 컴퓨터로는 처리 불가능")
        return False
    except:
        return False

pandas_approach_simulation()

print("\n" + "="*60)
print("3. 빅데이터 방식 - 청크 단위 처리")

def bigdata_chunk_processing():
    """
    청크 단위로 나누어 처리 (MapReduce 개념)
    """
    results = []
    
    print("🔄 청크 단위로 분산 처리 시작...")
    
    for chunk_num in range(0, total_rows, chunk_size):
        # 각 청크를 별도로 처리
        end_row = min(chunk_num + chunk_size, total_rows)
        
        # 가상 데이터 생성 (실제로는 파일에서 읽음)
        chunk_data = {
            'user_id': np.random.randint(1, 1000000, chunk_size),
            'timestamp': pd.date_range('2024-01-01', periods=chunk_size, freq='1s'),
            'page_views': np.random.randint(1, 100, chunk_size),
            'session_duration': np.random.randint(10, 3600, chunk_size)
        }
        
        df_chunk = pd.DataFrame(chunk_data)
        
        # 각 청크에서 집계 처리
        chunk_result = {
            'chunk': chunk_num // chunk_size + 1,
            'total_users': df_chunk['user_id'].nunique(),
            'avg_page_views': df_chunk['page_views'].mean(),
            'total_sessions': len(df_chunk)
        }
        
        results.append(chunk_result)
        
        if chunk_num < 3000000:  # 처음 3개 청크만 출력
            print(f"  ✅ 청크 {chunk_result['chunk']} 처리 완료 - "
                  f"사용자 {chunk_result['total_users']:,}명, "
                  f"평균 페이지뷰 {chunk_result['avg_page_views']:.1f}")
    
    return results

chunk_results = bigdata_chunk_processing()

print(f"\n🎯 전체 처리 완료: {len(chunk_results)}개 청크")

print("\n" + "="*60)
print("4. 실제 빅데이터 도구 - Dask 사용 예제")

def dask_bigdata_example():
    """
    Dask: pandas의 빅데이터 버전
    """
    print("🚀 Dask로 분산 처리 (pandas API와 동일하지만 분산 처리)")
    
    try:
        # 가상의 대용량 파일들 (실제로는 여러 파일로 분산 저장)
        print("# 실제 코드 예제:")
        print("dd.read_csv('data/*.csv')  # 폴더의 모든 CSV를 분산 처리")
        print("dd.read_parquet('data/*.parquet')  # 페타바이트급 데이터도 처리")
        
        # 간단한 dask 예제
        # 큰 데이터프레임 생성 (지연 연산)
        df_large = dd.from_pandas(
            pd.DataFrame({
                'x': np.random.randn(1000000),
                'y': np.random.randn(1000000)
            }), 
            npartitions=4  # 4개 파티션으로 분산
        )
        
        print(f"\n📊 Dask DataFrame 생성:")
        print(f"   - 파티션 수: {df_large.npartitions}")
        print(f"   - 메모리 사용량: 지연 연산으로 최소화")
        
        # 연산 (지연 실행)
        result = df_large.x.mean()
        print(f"   - 평균값 계산: {result.compute():.4f}")
        
        return True
        
    except Exception as e:
        print(f"Dask 예제 실행 중 오류: {e}")
        print("pip install dask로 설치 후 재시도해보세요")
        return False

dask_success = dask_bigdata_example()

print("\n" + "="*60)
print("5. 실시간 스트림 처리 시뮬레이션")

def stream_processing_simulation():
    """
    실시간으로 들어오는 데이터 처리 (Kafka + Spark Streaming 개념)
    """
    print("📡 실시간 스트림 데이터 처리 시뮬레이션")
    
    # 실시간 데이터 스트림 시뮬레이션
    for second in range(5):
        # 1초마다 수천 건의 데이터가 들어온다고 가정
        events_per_second = np.random.randint(1000, 5000)
        
        # 실시간 집계
        print(f"⏰ {second+1}초: {events_per_second:,}개 이벤트 처리")
        print(f"   - 실시간 분석: 평균 응답시간, 에러율, 트래픽 패턴")
        
        time.sleep(0.5)  # 실제로는 실시간 처리
    
    print("✅ 실시간 처리 완료")

stream_processing_simulation()

print("\n" + "="*60)
print("🎯 빅데이터 vs 일반 데이터 분석 차이점 정리")

comparison = """
일반 데이터 분석:
❌ pandas.read_csv() - 메모리에 전체 로드
❌ 단일 머신 처리
❌ 배치 처리만 가능
❌ 구조화된 데이터만

빅데이터 처리:
✅ 청크/파티션 단위 분산 처리
✅ 여러 머신에서 병렬 처리 (클러스터)
✅ 실시간 스트림 처리 가능
✅ 다양한 형태 데이터 (텍스트, 이미지, 센서...)
✅ 페타바이트급 데이터 처리 가능

사용 도구:
🔧 Apache Spark (PySpark)
🔧 Dask (Python)
🔧 Apache Kafka (스트리밍)
🔧 Hadoop (분산 저장)
🔧 AWS EMR, Google Dataproc (클라우드)
"""

print(comparison)

print("\n💡 다음 학습 단계:")
print("1. Docker로 Spark 환경 구축")
print("2. 실제 대용량 데이터셋으로 PySpark 실습")
print("3. AWS/GCP에서 클러스터 구성")
print("4. 실시간 스트림 처리 파이프라인 구축")