#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from titanic import csv_to_json_formatter, csv_row_to_json
import json

def test_csv_formatter():
    """CSV 포맷터 함수 테스트"""
    
    print("=" * 60)
    print("CSV to JSON 포맷터 테스트")
    print("=" * 60)
    
    try:
        # 전체 CSV를 JSON으로 변환
        print("1. 전체 CSV 파일을 JSON으로 변환 중...")
        json_data = csv_to_json_formatter('data_sets/train.csv')
        print(f"   ✓ 총 {len(json_data)}개의 데이터가 성공적으로 변환되었습니다.")
        
        # 첫 번째 행 샘플 출력
        print("\n2. 첫 번째 행 JSON 변환 결과:")
        print("-" * 40)
        print(json.dumps(json_data[0], ensure_ascii=False, indent=2))
        
        # 특정 행 변환 테스트
        print("\n3. 특정 행(인덱스 5) JSON 변환 결과:")
        print("-" * 40)
        row_5 = csv_row_to_json('data_sets/train.csv', 5)
        print(json.dumps(row_5, ensure_ascii=False, indent=2))
        
        # 데이터 타입 확인
        print("\n4. 데이터 타입 확인:")
        print("-" * 40)
        sample = json_data[0]
        for key, value in sample.items():
            print(f"   {key}: {type(value).__name__} = {value}")
        
        # 빈 값 처리 확인
        print("\n5. 빈 값(Cabin) 처리 확인:")
        print("-" * 40)
        for i in range(5):
            cabin_value = json_data[i].get('Cabin')
            print(f"   행 {i+1}: Cabin = {cabin_value} (타입: {type(cabin_value).__name__})")
        
        print("\n" + "=" * 60)
        print("모든 테스트가 성공적으로 완료되었습니다! ✓")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_csv_formatter()
