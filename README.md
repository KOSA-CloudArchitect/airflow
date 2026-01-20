# Airflow DAG 워크플로우 개요

## 📊 DAG 구성 및 흐름

### 주요 DAG 목록
1. **realtime_pipeline_monitor** - 실시간 파이프라인 모니터링 (메인 DAG)
2. **redshift_s3_copy_pipeline** - Redshift 데이터 적재 (트리거 DAG)
3. **summary_analysis_dag** - 전체 리뷰 요약 분석 (향후 구현 예정)
4. **airflow_pool_management** - 풀 관리 및 테스트
5. **crawler_trigger_dag** - 크롤러 트리거 테스트
6. **realtime_pipeline_monitor_dag** - 파이프라인 모니터링 (실제 운영)

---

## 🔄 DAG 실행 흐름

```
┌─────────────────────────────────────────────────────────────┐
│                realtime_pipeline_monitor                    │
│                     (메인 DAG)                              │
└─────────────────┬───────────────────────────────────────────┘
                  │
    ┌─────────────┼─────────────┐
    │             │             │
    ▼             ▼             ▼
┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
│크롤러   │ │Collection││Transform│ │Analysis │
│호출     │ │완료대기  ││ 완료대기 │ │완료대기 │
└─────────┘ └─────────┘ └─────────┘ └─────────┘
    │             │             │             │
    └─────────────┼─────────────┼─────────────┘
                  │             │
                  ▼             ▼
            ┌─────────┐   ┌─────────┐
            │Aggreg-  │   │완료     │
            │ation    │   │알림     │
            │완료대기 │   │및       │
            └─────────┘   │트리거   │
                          │데이터   │
                          │준비     │
                          └─────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│              redshift_s3_copy_pipeline                      │
│                   (트리거 DAG)                              │
└─────────────────┬───────────────────────────────────────────┘
                  │
    ┌─────────────┼─────────────┐
    │             │             │
    ▼             ▼             ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│트리거   │ │S3 파일  │ │Redshift │
│데이터   │ │필터링   │ │COPY     │
│추출     │ │(시간+   │ │실행     │
│         │ │job_id)  │ │         │
└─────────┘ └─────────┘ └─────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                summary_analysis_dag                         │
│              (전체 리뷰 요약 분석 DAG)                      │
└─────────────────┬───────────────────────────────────────────┘
                  │
    ┌─────────────┼─────────────┐
    │             │             │
    ▼             ▼             ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│Overall  │ │Control  │ │요약     │
│Summary  │ │Topic    │ │분석     │
│Request  │ │요약     │ │완료     │
│Topic    │ │완료     │ │알림     │
│메시지   │ │메시지   │ │         │
│발행     │ │센싱     │ │         │
└─────────┘ └─────────┘ └─────────┘
```

---

## 🎯 DAG별 주요 기능

### 1. realtime_pipeline_monitor (메인 DAG)
- **목적**: 전체 파이프라인 오케스트레이션
- **트리거**: 수동 또는 외부 API
- **주요 태스크**:
  - 크롤러 동적 호출 (단일/다중 상품)
  - Collection/Transform/Analysis/Aggregation 단계별 완료 대기
  - 완료 알림 및 Redshift 트리거 데이터 준비
- **실행 조건**: Control 토픽 메시지 기반 이벤트 감지

### 2. redshift_s3_copy_pipeline (트리거 DAG)
- **목적**: 데이터 웨어하우스 적재
- **트리거**: 메인 DAG 완료 후 자동 실행
- **주요 태스크**:
  - 트리거 데이터 추출 (job_id, 실행시간)
  - S3 파일 필터링 (시간 + job_id 기준)
  - Redshift COPY 실행
  - Overall Summary Request Topic 메시지 발행
- **실행 조건**: 메인 DAG에서 전달받은 파라미터 기반

### 3. summary_analysis_dag (구현 완료)
- **목적**: 전체 리뷰 요약 분석 요청 및 완료 감지
- **트리거**: Redshift COPY 완료 후 자동 실행
- **주요 태스크**:
  - Overall Summary Request Topic 메시지 발행
  - Control Topic 요약 완료 메시지 센싱 대기
- **실행 조건**: Redshift 적재 완료 후 자동 트리거

---

## ⚙️ DAG 실행 파라미터

### 메인 DAG 실행 예시
```json
// 단일 상품 크롤링
{
  "job_id": "job-2024-001",
  "product_id": "product-123",
  "url": "https://example.com/product/123",
  "review_cnt": 100
}

// 다중 상품 크롤링
{
  "job_id": "job-2024-002",
  "url_list": [
    "https://example.com/product/123",
    "https://example.com/product/456"
  ]
}
```

### 트리거 DAG 자동 전달 데이터
```json
{
  "job_id": "job-2024-001",
  "execution_time": "2024-01-15T13:30:00+09:00",
  "dag_run_id": "manual__2024-01-15T13:30:00+00:00",
  "source_dag": "realtime_pipeline_monitor"
}
```

### 요약 분석 DAG 실행 파라미터 (향후 구현)
```json
{
  "job_id": "job-2024-001",
  "redshift_copy_completed": true,
  "summary_request_topic": "overall-summary-request-topic",
  "analysis_timeout": 1800
}
```

---

## 🔧 DAG 설정 특징

### 타임아웃 설정
- **Collection**: 60분 (대용량 데이터 수집 고려)
- **Transform**: 30분 (스트림 처리)
- **Analysis**: 45분 (LLM API 호출 고려)
- **Aggregation**: 15분 (집계 처리)
- **Redshift COPY**: 120분 (대용량 데이터 적재)
- **Summary Analysis**: 30분 (전체 리뷰 요약 분석)

### 재시도 정책
- **기본 재시도**: 1회
- **재시도 간격**: 5분
- **실패 처리**: 단계별 실패 콜백 및 알림

### 모니터링
- **폴링 간격**: 30초
- **로그 레벨**: 상세 로깅
- **알림**: Discord/Slack 연동 (구현 예정)

---

## 📈 향후 확장 계획

### 추가 예정 DAG
1. **summary_analysis_dag** - 전체 리뷰 요약 분석
   - 트리거: Redshift COPY 완료 후
   - 기능: Overall Summary Request Topic 메시지 발행 → Kafka Worker 센싱 → Analysis Server 작업 완료 대기

2. **monitoring_dashboard_dag** - 모니터링 대시보드 업데이트
   - 스케줄: 매일 정기 실행
   - 기능: 메트릭 수집 및 시각화

### DAG 간 연계 강화
- **이벤트 체인**: 메인 → 적재 → 요약분석 → 대시보드 순차 실행
- **Kafka 메시지 연계**: Overall Summary Request Topic을 통한 비동기 처리
- **에러 전파**: 하위 DAG 실패 시 상위 DAG 알림
- **상태 공유**: XCom을 통한 DAG 간 데이터 전달 최적화

### Kafka 메시지 플로우
1. **Redshift COPY 완료** → Overall Summary Request Topic 메시지 발행
2. **Kafka Worker** → 메시지 수신 및 Analysis Server 호출
3. **Analysis Server** → LLM 기반 전체 리뷰 요약 분석 수행
4. **Analysis 완료** → Control Topic에 요약 완료 메시지 발행
5. **summary_analysis_dag** → Control Topic 메시지 센싱으로 완료 확인
