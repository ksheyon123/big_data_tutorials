# 빅데이터용 CSV-JSON 변환기
import pandas as pd
import json
import os
from typing import List, Dict, Any, Iterator, Optional
from pathlib import Path
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigDataCSVConverter:
    """
    대용량 CSV 파일을 효율적으로 JSON으로 변환하는 클래스
    메모리 효율성과 확장성을 고려한 빅데이터 처리
    """
    
    def __init__(self, chunk_size: int = 10000, max_memory_mb: int = 500):
        """
        Args:
            chunk_size: 한 번에 처리할 행 수
            max_memory_mb: 최대 사용 메모리 (MB)
        """
        self.chunk_size = chunk_size
        self.max_memory_mb = max_memory_mb
        
    def convert_large_csv(self, 
                         csv_file_path: str, 
                         output_dir: str = "output",
                         split_files: bool = True) -> Dict[str, Any]:
        """
        대용량 CSV를 청크 단위로 JSON 변환
        
        Args:
            csv_file_path: 입력 CSV 파일 경로
            output_dir: 출력 디렉토리
            split_files: True시 청크별로 파일 분할, False시 단일 파일
            
        Returns:
            처리 결과 통계
        """
        start_time = datetime.now()
        
        # 출력 디렉토리 생성
        Path(output_dir).mkdir(exist_ok=True)
        
        # 파일 크기 확인
        file_size_mb = os.path.getsize(csv_file_path) / (1024 * 1024)
        logger.info(f"파일 크기: {file_size_mb:.2f}MB")
        
        # if file_size_mb > self.max_memory_mb:
        logger.info("대용량 파일 감지 - 청크 처리 모드")
        return self._process_large_file(csv_file_path, output_dir, split_files)
        # else:
        #     logger.info("소용량 파일 - 일반 처리 모드")
        #     return self._process_small_file(csv_file_path, output_dir)
    
    def _process_large_file(self, csv_file_path: str, output_dir: str, split_files: bool) -> Dict[str, Any]:
        """대용량 파일 청크 처리"""

        # ========== 1. 초기 변수 설정 ==========
        total_rows = 0           # int: 처리된 전체 행 수
        chunk_count = 0          # int: 처리된 청크 수
        all_data = [] if not split_files else None  # List[Dict] 또는 None

        """
        초기 상태:
        total_rows = 0
        chunk_count = 0
        all_data = [] (split_files=False인 경우) 또는 None (split_files=True인 경우)
        
        예시:
        - split_files=False: all_data = []
        - split_files=True:  all_data = None
        """
        
        try:
            # ========== 2. CSV 파일 청크 단위 읽기 ==========
            for chunk_df in pd.read_csv(csv_file_path, chunksize=self.chunk_size):

                """
                chunk_df의 형태: pandas.DataFrame
                
                예시 (첫 번째 청크, chunksize=3):
                chunk_df:
                    PassengerId  Survived  Pclass                         Name     Sex   Age  SibSp  Parch      Ticket     Fare Cabin Embarked
                0            1         0       3         Braund, Mr. Owen Harris    male  22.0      1      0   A/5 21171   7.2500   NaN        S
                1            2         1       1  Cumings, Mrs. John Bradley ...  female  38.0      1      0    PC 17599  71.2833   C85        C
                2            3         1       3          Heikkinen, Miss. Laina  female  26.0      0      0  STON/O2. 3101282   7.9250   NaN        S
                
                데이터 타입:
                chunk_df.dtypes:
                PassengerId      int64
                Survived         int64  
                Pclass           int64
                Name            object
                Sex             object
                Age            float64
                SibSp            int64
                Parch            int64
                Ticket          object
                Fare           float64
                Cabin           object
                Embarked        object
                """
                # ========== 3. 청크 정보 업데이트 ==========
                chunk_count += 1         # 청크 번호 증가
                current_rows = len(chunk_df)  # 현재 청크의 행 수
                total_rows += current_rows    # 전체 행 수에 누적

                """
                첫 번째 청크 처리 후:
                chunk_count = 1
                current_rows = 3
                total_rows = 3
                
                두 번째 청크 처리 후:
                chunk_count = 2  
                current_rows = 3
                total_rows = 6
                """
                
                logger.info(f"청크 {chunk_count} 처리 중... ({current_rows:,}행)")
                
                # ========== 4. 메모리 최적화 ==========
                chunk_df = self._optimize_dtypes(chunk_df)
                """
                최적화 전 메모리 사용량:
                PassengerId: int64 (8 bytes) → uint16 (2 bytes)
                Survived:    int64 (8 bytes) → uint8 (1 byte)  
                Pclass:      int64 (8 bytes) → uint8 (1 byte)
                Age:       float64 (8 bytes) → float32 (4 bytes)
                
                최적화 후 chunk_df:
                    PassengerId  Survived  Pclass                         Name     Sex   Age  SibSp  Parch      Ticket     Fare Cabin Embarked
                0           1         0       3         Braund, Mr. Owen Harris    male  22.0      1      0   A/5 21171   7.25   NaN        S
                1           2         1       1  Cumings, Mrs. John Bradley ...  female  38.0      1      0    PC 17599  71.28   C85        C
                2           3         1       3          Heikkinen, Miss. Laina  female  26.0      0      0  STON/O2. 3101282   7.92   NaN        S
                
                최적화 후 데이터 타입:
                PassengerId    uint16  # 메모리 절약
                Survived        uint8  # 메모리 절약
                Pclass          uint8  # 메모리 절약
                Age           float32  # 메모리 절약
                """
                
                chunk_df = chunk_df.where(pd.notnull(chunk_df), None)
                """
                NaN 값을 None으로 변환 (JSON 직렬화를 위해):
                
                변환 전:
                Cabin    NaN
                
                변환 후:
                Cabin    None
                
                최종 chunk_df:
                    PassengerId  Survived  Pclass                         Name     Sex   Age  SibSp  Parch      Ticket     Fare Cabin Embarked
                0           1         0       3         Braund, Mr. Owen Harris    male  22.0      1      0   A/5 21171   7.25  None        S
                1           2         1       1  Cumings, Mrs. John Bradley ...  female  38.0      1      0    PC 17599  71.28   C85        C
                2           3         1       3          Heikkinen, Miss. Laina  female  26.0      0      0  STON/O2. 3101282   7.92  None        S
                """
                
                # JSON 변환
                chunk_json = chunk_df.to_dict('records')

                """
                DataFrame을 딕셔너리 리스트로 변환:
                
                chunk_json의 형태: List[Dict[str, Any]]
                
                chunk_json = [
                    {
                        'PassengerId': 1,
                        'Survived': 0,
                        'Pclass': 3,
                        'Name': 'Braund, Mr. Owen Harris',
                        'Sex': 'male',
                        'Age': 22.0,
                        'SibSp': 1,
                        'Parch': 0,
                        'Ticket': 'A/5 21171',
                        'Fare': 7.25,
                        'Cabin': None,
                        'Embarked': 'S'
                    },
                    ...json
                ]

                len(chunk_json) = 3
                type(chunk_json[0]) = dict
                """
                
                if split_files:
                    # 청크별 개별 파일 저장
                    output_file = f"{output_dir}/chunk_{chunk_count:04d}.json"
                    self._save_json_chunk(chunk_json, output_file)
                else:
                    # 메모리에 누적 (메모리 제한 고려)
                    all_data.extend(chunk_json)
                    
                    # 메모리 사용량 체크
                    if len(all_data) % (self.chunk_size * 5) == 0:
                        logger.info(f"현재까지 처리: {len(all_data):,}행")
                
                # 메모리 정리
                del chunk_df, chunk_json
            
            # 단일 파일로 저장 (split_files=False인 경우)
            if not split_files and all_data:
                output_file = f"{output_dir}/complete_data.json"
                self._save_json_chunk(all_data, output_file)
                
        except Exception as e:
            logger.error(f"처리 중 오류: {e}")
            raise
        
        return {
            'total_rows': total_rows,
            'chunk_count': chunk_count,
            'output_files': chunk_count if split_files else 1,
            'processing_time': (datetime.now() - datetime.now()).total_seconds()
        }
    
    def _process_small_file(self, csv_file_path: str, output_dir: str) -> Dict[str, Any]:
        """소용량 파일 일반 처리"""
        df = pd.read_csv(csv_file_path)
        df = self._optimize_dtypes(df)
        df = df.where(pd.notnull(df), None)
        
        json_data = df.to_dict('records')
        output_file = f"{output_dir}/data.json"
        self._save_json_chunk(json_data, output_file)
        
        return {
            'total_rows': len(df),
            'chunk_count': 1,
            'output_files': 1,
            'processing_time': 0
        }
    
    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터 타입 최적화로 메모리 절약"""
        for col in df.columns:
            if df[col].dtype == 'int64':
                if df[col].min() >= 0:
                    if df[col].max() < 256:
                        df[col] = df[col].astype('uint8')
                    elif df[col].max() < 65536:
                        df[col] = df[col].astype('uint16')
                else:
                    if df[col].min() > -128 and df[col].max() < 128:
                        df[col] = df[col].astype('int8')
                    elif df[col].min() > -32768 and df[col].max() < 32768:
                        df[col] = df[col].astype('int16')
                        
            elif df[col].dtype == 'float64':
                df[col] = df[col].astype('float32')
                
        return df
    
    def _save_json_chunk(self, data: List[Dict[str, Any]], output_file: str) -> None:
        """JSON 청크 저장"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"저장 완료: {output_file} ({len(data):,}행)")
        except Exception as e:
            logger.error(f"저장 실패 {output_file}: {e}")
            raise
    
    def stream_json_processor(self, csv_file_path: str) -> Iterator[Dict[str, Any]]:
        """
        스트림 방식으로 JSON 반환 (메모리 효율적)
        대용량 파일을 실시간 처리할 때 사용
        """
        try:
            for chunk_df in pd.read_csv(csv_file_path, chunksize=self.chunk_size):
                chunk_df = self._optimize_dtypes(chunk_df)
                chunk_df = chunk_df.where(pd.notnull(chunk_df), None)
                
                for _, row in chunk_df.iterrows():
                    yield row.to_dict()
                    
        except Exception as e:
            logger.error(f"스트림 처리 오류: {e}")
            raise
    
    def get_file_info(self, csv_file_path: str) -> Dict[str, Any]:
        """파일 정보 조회"""
        file_size = os.path.getsize(csv_file_path)
        
        # 첫 몇 행으로 구조 파악
        sample_df = pd.read_csv(csv_file_path, nrows=5)
        
        return {
            'file_size_mb': file_size / (1024 * 1024),
            'estimated_rows': self._estimate_total_rows(csv_file_path),
            'columns': list(sample_df.columns),
            'dtypes': sample_df.dtypes.to_dict(),
            'recommended_chunk_size': self._recommend_chunk_size(file_size)
        }
    
    def _estimate_total_rows(self, csv_file_path: str) -> int:
        """전체 행 수 추정 (대용량 파일용)"""
        with open(csv_file_path, 'r') as f:
            # 첫 1000줄로 평균 라인 길이 계산
            lines = []
            for i, line in enumerate(f):
                if i >= 1000:
                    break
                lines.append(len(line))
        
        avg_line_length = sum(lines) / len(lines)
        file_size = os.path.getsize(csv_file_path)
        
        return int(file_size / avg_line_length)
    
    def _recommend_chunk_size(self, file_size: int) -> int:
        """파일 크기에 따른 권장 청크 크기"""
        size_mb = file_size / (1024 * 1024)
        
        if size_mb < 10:
            return 1000
        elif size_mb < 100:
            return 5000
        elif size_mb < 1000:
            return 10000
        else:
            return 50000


# 사용 예시
if __name__ == "__main__":
    # 기본 사용법
    converter = BigDataCSVConverter(chunk_size=100)
    
    # 파일 정보 확인
    print("=== 파일 정보 분석 ===")
    file_info = converter.get_file_info("data_sets/train.csv")
    print(f"파일 크기: {file_info['file_size_mb']:.2f}MB")
    print(f"예상 행 수: {file_info['estimated_rows']:,}행")
    print(f"권장 청크 크기: {file_info['recommended_chunk_size']:,}")
    
    # 변환 실행
    print("\n=== 변환 시작 ===")
    result = converter.convert_large_csv(
        csv_file_path="data_sets/train.csv",
        output_dir="json_output",
        split_files=False  # 단일 파일로 저장
    )
    
    print(f"처리 완료: {result['total_rows']:,}행")
    print(f"출력 파일: {result['output_files']}개")
    
    # 스트림 처리 예시 (메모리 효율적)
    print("\n=== 스트림 처리 예시 ===")
    count = 0
    for json_row in converter.stream_json_processor("data_sets/train.csv"):
        if count < 3:  # 처음 3개만 출력
            print(f"행 {count + 1}: {json_row}")
        count += 1
        if count >= 10:  # 10개만 처리하고 중단
            break
    
    print(f"스트림으로 {count}개 행 처리 완료")