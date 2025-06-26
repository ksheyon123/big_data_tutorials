#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from titanic import csv_to_json_formatter, csv_row_to_json
import json

def verify_and_save_results():
    """CSV 포맷터 검증 및 결과 저장"""
    
    results = []
    
    try:
        # 전체 CSV를 JSON으로 변환
        results.append("=== CSV to JSON 포맷터 검증 결과 ===\n")
        
        json_data = csv_to_json_formatter('data_sets/train.csv')
        results.append(f"✓ 총 {len(json_data)}개의 데이터가 성공적으로 변환되었습니다.\n")
        
        # 첫 번째 행 샘플
        results.append("첫 번째 행 JSON 변환 결과:")
        results.append(json.dumps(json_data[0], ensure_ascii=False, indent=2))
        results.append("\n")
        
        # 특정 행 변환 테스트
        row_5 = csv_row_to_json('data_sets/train.csv', 5)
        results.append("특정 행(인덱스 5) JSON 변환 결과:")
        results.append(json.dumps(row_5, ensure_ascii=False, indent=2))
        results.append("\n")
        
        # 데이터 타입 확인
        results.append("데이터 타입 확인:")
        sample = json_data[0]
        for key, value in sample.items():
            results.append(f"  {key}: {type(value).__name__} = {value}")
        results.append("\n")
        
        # 빈 값 처리 확인
        results.append("빈 값(Cabin) 처리 확인:")
        for i in range(5):
            cabin_value = json_data[i].get('Cabin')
            results.append(f"  행 {i+1}: Cabin = {cabin_value} (타입: {type(cabin_value).__name__})")
        
        results.append("\n✓ 모든 테스트가 성공적으로 완료되었습니다!")
        
        # 결과를 파일로 저장
        with open('formatter_test_results.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(results))
        
        print("검증 완료! 결과가 formatter_test_results.txt 파일에 저장되었습니다.")
        
        # 처음 3개 행의 JSON 데이터를 별도 파일로 저장
        sample_data = json_data[:3]
        with open('sample_json_output.json', 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, ensure_ascii=False, indent=2)
        
        print("샘플 JSON 데이터가 sample_json_output.json 파일에 저장되었습니다.")
        
    except Exception as e:
        error_msg = f"❌ 오류 발생: {e}"
        results.append(error_msg)
        print(error_msg)
        
        with open('formatter_test_results.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(results))

if __name__ == "__main__":
    verify_and_save_results()
