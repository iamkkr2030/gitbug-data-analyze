# 구현 및 검증 기록

2026-09-06에 저장소 CSV와 실제 Docker 서비스를 사용해 검증했습니다.

## 실행 환경

- 호스트: Windows / Docker Desktop, Linux WSL2 컨테이너
- Docker Engine: 29.7.2, 할당 CPU 12개, 메모리 8,284,413,952 bytes
- Python 3.11.11, Java 17, PySpark 3.5.6, Airflow 2.10.5
- MySQL 8.4.4, PostgreSQL 16.8 (Airflow 메타데이터)
- Spark `local[2]`, shuffle 파티션 4개, MySQL 삽입 청크 500개

버전 구성은 [Airflow 2.10.5 Docker 안내](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html)와 [Spark 3.5.6 환경 안내](https://spark.apache.org/docs/3.5.6/)를 확인하고 실제 이미지 빌드·실행으로 검증했습니다. 최상위 패키지와 컨테이너 태그를 고정했으며, 모든 전이 의존성과 OS 패키지를 digest까지 고정한 환경은 아닙니다.

## 자동 테스트

컨테이너에서 **25 passed**, 실패·건너뛰기 0개, 25.30초였습니다. [JUnit 원본](test-results.xml)을 제공합니다. 호스트 Python 3.14에서는 경량 테스트 19개가 통과했고, Spark·MySQL·Airflow 모듈 3개는 환경 부재로 건너뛰었습니다.

테스트 내용:

- CSV 스키마, 손상된 따옴표, 필드 수, 빈 입력 및 검사 후 원본 변경
- 양의 BIGINT 경계, 비정수, 0, 음수, 긴 선행 0, 중복·자기 참조
- 실제 CSV의 Spark 변환, Parquet 읽기 및 이웃 수 집계
- 실제 MySQL에서 staging 청크 커밋 후 실패 및 공개 커밋 직전 실패
- 실패 후 같은 배치 ID 재개, 성공 배치 건너뛰기, 이전 포인터 유지
- 새 스냅샷에서 제거된 관계가 현재 조회에 남지 않는지 확인
- 소유권이 바뀐 오래된 작업자의 공개 차단
- Airflow DAG import, 8단계 의존성, 수동 스케줄 설정

호스트의 pytest 캐시 경로에 컨테이너가 쓰지 못해 캐시 경고 2개가 있었으며 테스트·JUnit 저장에는 영향이 없었습니다.

## 실제 Airflow 실행

LocalExecutor 스케줄러에서 `implementation-validation` run을 실행했습니다. DAG와 **8개 작업 모두 SUCCESS**입니다. [DAG 실행 기록](dag-runs.json), [작업별 상태·시각](dag-tasks.json)을 보존했습니다.

- 시작: 2026-09-06 11:42:50.814666 UTC
- 종료: 2026-09-06 11:43:23.221700 UTC
- 전체 경과: 약 32.41초 (스케줄링·Spark 시작·적재 포함)
- Spark 변환 함수: 11.6053초 (Spark 세션 생성 제외)
- 배치 ID: `1bcfeb4316b01533b87988b9dc3eeed9`

이어 동일 입력을 CLI로 다시 실행해 `skipped: true`, `SUCCESS`, 1,034개 관계 유지를 확인했습니다. [재실행 결과](rerun-result.json)를 참고하세요. 검증 종료 시 MySQL, PostgreSQL, Airflow webserver·scheduler의 healthcheck도 모두 통과했습니다.

## 데이터 결과

| 항목 | 결과 |
| --- | ---: |
| 원본 행 | 1,021 |
| 분리 후 관계 | 1,042 |
| 격리 관계 | 0 |
| 동일 방향 중복 제거 | 8 |
| 최종 방향 관계 | 1,034 |
| 전체 고유 이슈 | 1,068 |
| 무방향 관계 | 560 |
| 상호 참조 쌍 | 474 |
| 무방향 쌍 중 상호 참조 비율 | 84.64% |

[Spark 품질 보고서](spark-quality.json), [Spark 실행 계획](spark-plan.txt), [분석 SQL 실행 결과](analysis-results.txt), [독립 Python 프로파일](source-profile.json)을 보존했습니다. 연결 수가 가장 많은 이슈는 `13363492`로 고유 이웃 3개, in-degree 1, out-degree 2입니다. 이웃 수 분포는 1개: 1,017개 이슈, 2개: 50개, 3개: 1개입니다.

## 합성 데이터 측정

`scripts/benchmark.py --rows 1000 10000 100000`을 같은 Spark 세션에서 순차 실행했습니다. 각 행은 다음 이슈 ID를 두 번 포함하며 중복 제거 후 관계 수가 입력 행 수와 같습니다. [측정 원본](benchmark.json)에 환경·시작 시간·품질 지표를 기록했습니다.

| 합성 입력 행 | 변환 시간 (초) | 입력 행/초 |
| --- | ---: | ---: |
| 1,000 | 9.6059 | 104 |
| 10,000 | 4.5809 | 2,183 |
| 100,000 | 6.3232 | 15,815 |

Spark 시작은 별도로 3.2258초였습니다. 각 규모 1회 측정이며 첫 작업의 JVM 준비 비용과 이후 세션 재사용 효과가 포함됩니다. 변환 시간은 집계·Parquet·격리 출력까지이며, MySQL 적재 시간은 포함하지 않습니다. 성능 개선률, 분산 확장성 또는 반복 측정의 통계적 결론으로 사용하지 않습니다. 프로세스 최대 메모리는 측정하지 않았습니다.

## 설계와 남은 범위

원본 방향을 유지하고 `neighbor_count`만 무방향 고유 이웃을 집계합니다. 관계가 동일한 실제 버그를 의미하는 정답 라벨이라고 단정하지 않습니다. 입력은 전체 스냅샷이며, 과거 성공 배치를 다시 요청해도 현재 포인터를 되돌리지 않습니다.

MySQL 적재는 Spark DataFrame의 `toLocalIterator()`와 제한된 청크 삽입을 사용합니다. 배치 소유 토큰, DB 세션 잠금, staging 초기화, 트랜잭션 공개로 중복 재시도와 부분 실패를 처리합니다. 대규모 분산 JDBC 적재는 구현 범위에 포함하지 않았습니다.

Airflow UI는 localhost:8080에서 제공됩니다. 자동화 세션에 연결 가능한 브라우저가 없어 UI 스크린샷은 수집하지 못했습니다. README의 화면 캡처 항목을 미완료로 남겼으며, 스크린샷 대신 실제 CLI 상태 원본을 제공합니다. Airflow 자체의 일시적 네트워크 장애 재시도는 별도 실장애 주입까지 수행하지 않았고, DB 부분 실패·복구는 자동 통합 테스트로 검증했습니다.

원본 CSV가 Hugging Face의 어떤 split·변환에서 나왔는지와 재배포 라이선스는 아직 확인되지 않았습니다. 선택 확장인 그래프 연결 요소 분석도 포함하지 않았습니다.
