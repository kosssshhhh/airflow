# Airflow

각 쇼핑몰 사이트의 카테고리 별 Top 100 상품들은 일별로 업데이트 되기 때문에 매일 수집되어 DB에 Load 하는 과정을 거침.

`Apache Airflow`를 활용하여 스케쥴링 및 모니터링 할 수 있도록 관리.

## Extract, Transform

`Python`, `BS4`, `HTTPX`를 활용하여 각 쇼핑몰 별로 크롤링 코드를 작성하였고, 이 과정에서 수집되는 데이터들을 원하는 형태로 변환하는 과정도 함께 진행.

<img width="482" alt="crawled data visualizaion" src="https://github.com/user-attachments/assets/e95842db-212a-4c1e-a4b8-fed7e110ca4a" />



## Dag

각 쇼핑몰 별  각각 DAG 구성하여 스케쥴러를 통한 데이터 파이프라인 자동화 진행.

Extract, Transfrom, Load 세 개의 Task로 분리하여 DAG 구성

<img width="1426" alt="DAGs" src="https://github.com/user-attachments/assets/bcd4cc5a-206f-4efa-9bc3-f1566e50d555" />

<img width="1007" alt="Each Task" src="https://github.com/user-attachments/assets/cfce0eb3-1da3-4aec-85c0-7075bc198ea2" />
