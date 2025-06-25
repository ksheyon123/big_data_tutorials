# 빅데이터 입문 - 실제 데이터로 시작하기
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import requests

# 1. COVID-19 데이터 분석 (실시간 데이터)
print("=== COVID-19 데이터 분석 ===")
covid_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"

try:
    covid_df = pd.read_csv(covid_url)
    print(f"데이터 크기: {covid_df.shape}")
    print("상위 5개 국가:")
    
    # 최신 데이터 컬럼 (날짜)
    latest_col = covid_df.columns[-1]
    top_countries = covid_df.groupby('Country/Region')[latest_col].sum().sort_values(ascending=False).head()
    print(top_countries)
    
except Exception as e:
    print(f"데이터 로드 실패: {e}")

print("\n" + "="*50 + "\n")

# 2. 주식 데이터 분석 (yfinance 필요: pip install yfinance)
print("=== 주식 데이터 분석 ===")
try:
    import yfinance as yf
    
    # 삼성전자, 애플 데이터
    tickers = ['005930.KS', 'AAPL']  # 삼성전자, 애플
    stock_data = {}
    
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        stock_data[ticker] = stock.history(period="3mo")
    
    # 수익률 계산
    for ticker, data in stock_data.items():
        returns = data['Close'].pct_change()
        print(f"{ticker} - 3개월 평균 수익률: {returns.mean():.4f}")
        print(f"{ticker} - 변동성 (표준편차): {returns.std():.4f}")
    
except ImportError:
    print("yfinance 라이브러리가 필요합니다: pip install yfinance")
except Exception as e:
    print(f"주식 데이터 로드 실패: {e}")

print("\n" + "="*50 + "\n")

# 3. Reddit API 데이터 (무료)
print("=== Reddit 데이터 분석 ===")
def get_reddit_data(subreddit, limit=10):
    url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
    headers = {'User-Agent': 'DataAnalysis/1.0'}
    
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        
        posts = []
        for post in data['data']['children']:
            posts.append({
                'title': post['data']['title'][:50] + "...",  # 제목 축약
                'score': post['data']['score'],
                'comments': post['data']['num_comments'],
                'created': datetime.fromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%d %H:%M')
            })
        
        return pd.DataFrame(posts)
    
    except Exception as e:
        print(f"Reddit 데이터 로드 실패: {e}")
        return pd.DataFrame()

# 프로그래밍 관련 서브레딧 분석
reddit_df = get_reddit_data('programming', 5)
if not reddit_df.empty:
    print("Reddit /r/programming 인기 포스트:")
    print(reddit_df.to_string(index=False))
    print(f"\n평균 점수: {reddit_df['score'].mean():.1f}")
    print(f"평균 댓글 수: {reddit_df['comments'].mean():.1f}")

print("\n" + "="*50 + "\n")

# 4. 간단한 데이터 생성 및 분석 (로컬 테스트용)
print("=== 가상 웹 로그 데이터 분석 ===")
import random
from datetime import datetime, timedelta

# 가상 웹 로그 생성
def generate_web_logs(num_logs=1000):
    pages = ['/home', '/products', '/cart', '/checkout', '/profile', '/about']
    devices = ['mobile', 'desktop', 'tablet']
    countries = ['KR', 'US', 'JP', 'CN', 'DE']
    
    logs = []
    base_time = datetime.now() - timedelta(days=30)
    
    for i in range(num_logs):
        log = {
            'timestamp': base_time + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            ),
            'user_id': f'user_{random.randint(1, 200)}',
            'page': random.choice(pages),
            'duration_seconds': random.randint(10, 600),
            'device': random.choice(devices),
            'country': random.choice(countries),
            'referrer': random.choice(['google', 'direct', 'facebook', 'twitter', 'other'])
        }
        logs.append(log)
    
    return pd.DataFrame(logs)

# 웹 로그 분석
web_logs = generate_web_logs(5000)
print(f"생성된 로그 수: {len(web_logs)}")

# 분석 예제들
print("\n📊 분석 결과:")
print("1. 가장 인기있는 페이지:")
popular_pages = web_logs['page'].value_counts().head()
print(popular_pages)

print("\n2. 디바이스별 평균 체류시간:")
device_duration = web_logs.groupby('device')['duration_seconds'].mean().sort_values(ascending=False)
print(device_duration)

print("\n3. 국가별 방문자 수:")
country_visitors = web_logs['country'].value_counts()
print(country_visitors)

print("\n4. 시간대별 트래픽 (상위 5개):")
web_logs['hour'] = web_logs['timestamp'].dt.hour
hourly_traffic = web_logs['hour'].value_counts().sort_index().head()
print(hourly_traffic)

print("\n✅ 빅데이터 분석 완료!")
print("💡 다음 단계: 이 코드를 수정해서 더 복잡한 분석을 해보세요!")
print("   - 시각화 추가 (matplotlib, seaborn)")
print("   - 실제 API 데이터 연동")
print("   - CSV 파일로 저장/불러오기")
print("   - Pandas 고급 기능 활용")