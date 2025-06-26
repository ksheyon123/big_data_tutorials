import pandas as pd
import json
from typing import List, Dict, Any, Optional


def csv_to_json_formatter(csv_file_path: str) -> List[Dict[str, Any]]:
    """
    CSV 파일의 각 행을 JSON 포맷으로 변환하는 함수
    
    Args:
        csv_file_path (str): CSV 파일 경로
        
    Returns:
        List[Dict[str, Any]]: 각 행이 JSON 형태의 딕셔너리로 변환된 리스트
        
    Example:
        >>> data = csv_to_json_formatter('data_sets/train.csv')
        >>> print(data[0])
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
        }
    """
    try:
        # CSV 파일 읽기
        df = pd.read_csv(csv_file_path)
        
        # NaN 값을 None으로 변환하여 JSON 직렬화 가능하게 만들기
        df = df.where(pd.notnull(df), None)
        
        # 각 행을 딕셔너리로 변환
        json_data = df.to_dict('records')
        
        return json_data
        
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_file_path}")
    except pd.errors.EmptyDataError:
        raise ValueError("CSV 파일이 비어있습니다.")
    except Exception as e:
        raise Exception(f"CSV 파일 처리 중 오류가 발생했습니다: {str(e)}")


def csv_row_to_json(csv_file_path: str, row_index: int) -> Dict[str, Any]:
    """
    CSV 파일의 특정 행을 JSON 포맷으로 변환하는 함수
    
    Args:
        csv_file_path (str): CSV 파일 경로
        row_index (int): 변환할 행의 인덱스 (0부터 시작)
        
    Returns:
        Dict[str, Any]: 해당 행이 JSON 형태의 딕셔너리로 변환된 결과
        
    Example:
        >>> row_data = csv_row_to_json('data_sets/train.csv', 0)
        >>> print(row_data)
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
        }
    """
    try:
        # CSV 파일 읽기
        df = pd.read_csv(csv_file_path)
        
        # 인덱스 범위 확인
        if row_index < 0 or row_index >= len(df):
            raise IndexError(f"행 인덱스가 범위를 벗어났습니다. 유효 범위: 0-{len(df)-1}")
        
        # 특정 행 선택
        row = df.iloc[row_index]
        
        # NaN 값을 None으로 변환
        row_dict = row.where(pd.notnull(row), None).to_dict()
        
        return row_dict
        
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_file_path}")
    except pd.errors.EmptyDataError:
        raise ValueError("CSV 파일이 비어있습니다.")
    except Exception as e:
        raise Exception(f"CSV 파일 처리 중 오류가 발생했습니다: {str(e)}")


def save_json_to_file(data: List[Dict[str, Any]], output_file_path: str) -> None:
    """
    JSON 데이터를 파일로 저장하는 함수
    
    Args:
        data (List[Dict[str, Any]]): 저장할 JSON 데이터
        output_file_path (str): 출력 파일 경로
    """
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"JSON 파일이 성공적으로 저장되었습니다: {output_file_path}")
    except Exception as e:
        raise Exception(f"JSON 파일 저장 중 오류가 발생했습니다: {str(e)}")


def print_json_sample(data: List[Dict[str, Any]], num_samples: int = 3) -> None:
    """
    JSON 데이터의 샘플을 출력하는 함수
    
    Args:
        data (List[Dict[str, Any]]): 출력할 JSON 데이터
        num_samples (int): 출력할 샘플 개수 (기본값: 3)
    """
    print(f"총 {len(data)}개의 데이터 중 처음 {min(num_samples, len(data))}개 샘플:")
    print("-" * 50)
    
    for i in range(min(num_samples, len(data))):
        print(f"샘플 {i+1}:")
        print(json.dumps(data[i], ensure_ascii=False, indent=2))
        print("-" * 50)


# 사용 예시
if __name__ == "__main__":
    # CSV 파일 경로
    csv_file = "data_sets/train.csv"
    
    try:
        # 전체 CSV를 JSON으로 변환
        print("CSV 파일을 JSON 포맷으로 변환 중...")
        json_data = csv_to_json_formatter(csv_file)
        
        # 샘플 출력
        print_json_sample(json_data, 3)
        
        # 특정 행만 변환 예시
        print("\n특정 행(첫 번째 행) JSON 변환 결과:")
        first_row = csv_row_to_json(csv_file, 0)
        print(json.dumps(first_row, ensure_ascii=False, indent=2))
        
        print("JSON 파일로 저장 (선택사항)")
        save_json_to_file(json_data, "titanic_data.json")
        
    except Exception as e:
        print(f"오류 발생: {e}")
