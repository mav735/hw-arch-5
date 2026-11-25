# Online Cinema: Event Streaming + Analytics Pipeline

```
producer (Go, Gin) ──Avro──▶ Kafka (3 brokers) ──▶ ClickHouse (Kafka Engine → MV → ReplacingMergeTree)
                                                        │
                       aggregator (Go, Gin) ◀── SELECT ─┤── Grafana (aggregate tables)
                          │ UPSERT        │ export
                          ▼               ▼
                     PostgreSQL       MinIO (S3)
```

## Запуск

```bash
docker compose up -d --build                      # весь стек
docker compose --profile test run --rm tests      # интеграционные тесты
```

| Сервис | Адрес |
|---|---|
| Producer API | http://localhost:8080 |
| Aggregator API | http://localhost:8090 |
| Grafana | http://localhost:3000 (admin / admin, дашборд *Cinema → Online Cinema Analytics*) |
| Schema Registry | http://localhost:18081 |
| ClickHouse HTTP | http://localhost:8123 (default / clickhouse) |
| MinIO console | http://localhost:9001 (minio / minio12345) |

## Схема события

`schemas/movie_event.avsc` (Avro). Init-контейнер `schema-init` регистрирует её в Schema Registry как
`movie-events-value` с совместимостью `BACKWARD`. Producer при старте берёт последнюю версию схемы
и пишет сообщения в Confluent wire format (magic byte + id схемы). ClickHouse читает их форматом
`AvroConfluent` по той же схеме из реестра.

Топик `movie-events`: 3 партиции, `replication.factor=3`, `min.insync.replicas=2`
(`infra/kafka/create-topic.sh`).

**Ключ партиционирования — `user_id`.** Все события пользователя попадают в одну партицию и читаются
в порядке записи: сессия START → PAUSE → RESUME → FINISH не перемешивается, а это и есть основа для
сессий, конверсии и retention. Ключ `movie_id` дал бы горячие партиции на популярных фильмах
и не гарантировал бы порядок событий пользователя, который смотрит разные фильмы.

## Producer

- `POST /events`: JSON-событие. `event_id` и `timestamp` необязательны (сгенерируются).
  Ошибка валидации → `400`, успешная запись → `201 {"event_id": ...}`, Kafka недоступна → `503`.
- Валидация: поля и enum'ы + сериализация по Avro-схеме.
- Kafka: `acks=all`, идемпотентный producer, retry с экспоненциальным backoff (200 мс → 5 с, 5 попыток),
  лог каждой публикации (`event_id`, `event_type`, `timestamp`, partition, offset).
- Генератор (`GENERATOR_ENABLED`): сначала детерминированно заполняет прошлые
  `GENERATOR_BACKFILL_DAYS` дней (повторный запуск даёт те же `event_id`, дубли схлопываются в ClickHouse),
  затем генерирует живой поток с частотой `GENERATOR_EVENTS_PER_SECOND`. Сессия:
  `[SEARCHED] → VIEW_STARTED → (VIEW_PAUSED → VIEW_RESUMED)* → VIEW_FINISHED [→ LIKED]` или брошенный
  просмотр после паузы; `progress_seconds` только растёт; пользователи возвращаются с затухающей вероятностью.

## ClickHouse

Миграции `clickhouse/migrations` применяет контейнер `clickhouse-migrate` при каждом старте (все DDL идемпотентны).

- `movie_events_queue`: Kafka Engine, `AvroConfluent`, битые сообщения уходят в `movie_events_errors`.
- `movie_events`: `ReplacingMergeTree`, `PARTITION BY toYYYYMM(event_date)`,
  `ORDER BY (event_date, event_type, user_id, event_id)`. Все запросы агрегатора фильтруют по дню и типу
  события; `event_id` в ключе убирает дубли at-least-once доставки.
- `movie_events_mv`: перекладка из Kafka-таблицы в `movie_events`.
- Агрегаты: `daily_metrics`, `daily_top_movies`, `cohort_retention`.

## Aggregator

Отдельный контейнер, читает только ClickHouse. Каждые `AGGREGATION_INTERVAL` (по умолчанию `1m`)
пересчитывает последние `AGGREGATION_LOOKBACK_DAYS` дней и сегодня. Лог начала и конца каждого цикла:
сколько событий обработано, сколько метрик, сколько времени.

| Метрика | Как считается |
|---|---|
| `dau` | `uniqExact(user_id)` за день |
| `avg_watch_seconds` | `avgIf(progress_seconds, VIEW_FINISHED)` |
| `top_movie_views` | `row_number() OVER (ORDER BY views DESC)`, топ-10 |
| `view_conversion` | `countIf(VIEW_FINISHED) / countIf(VIEW_STARTED)` |
| `retention_d1`, `retention_d7` | доля когорты (день первого `VIEW_STARTED`), активной через 1 / 7 дней |
| `cohort_retention` | матрица когорта × день 0..7: пересчёт дня D заполняет диагональ cohort = D − N |
| `dau_by_device`, `sessions`, `likes`, `searches`, `views_*` | дополнительно |

PostgreSQL: таблица `daily_metrics (metric_date, metric_name, dimension, value, computed_at)`,
PK `(metric_date, metric_name, dimension)`, запись через `INSERT ... ON CONFLICT DO UPDATE`
(повторный пересчёт обновляет строки, а не дублирует), retry с backoff. Миграции применяются
сервисом при старте (`schema_migrations`).

- `POST /recompute?date=YYYY-MM-DD`: ручной пересчёт за дату.
- `POST /export?date=YYYY-MM-DD`: ручной экспорт в S3.

## Экспорт в S3

Каждые `EXPORT_INTERVAL` (по умолчанию `1h`) агрегатор выгружает вчерашний день из PostgreSQL в
`s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json`: все метрики дня с `computed_at` и временем
экспорта. Ключ фиксирован, повторный экспорт перезаписывает файл. Если PostgreSQL или S3 недоступны,
ошибка логируется, дата запоминается и выгружается при следующем запуске.

MinIO берётся из образа `pgsty/minio`: официальный `minio/minio` больше не публикуется на Docker Hub.

## Grafana

Datasource ClickHouse и дашборд провижинятся автоматически. Все панели читают таблицы агрегатов:
Retention Cohort Heatmap (когорта × Day 0..7, %), DAU, конверсия, среднее время просмотра,
DAU по устройствам, топ-10 фильмов и stat-панели.

## Отказоустойчивость

3 брокера KRaft (каждый ещё и контроллер, кворум 2 из 3), RF=3, `min.insync.replicas=2`:
при падении одного брокера запись с `acks=all` продолжается. Health checks настроены у всех
долгоживущих сервисов, порядок старта задан через `depends_on: condition`.

## Тесты

`tests/pipeline_test.go` (`docker compose --profile test run --rm tests`):

- событие из `POST /events` появляется в ClickHouse со всеми полями;
- невалидные события получают `400`;
- на изолированных датах 2001-01-01/02: пересчёт метрик, идемпотентность (повторный пересчёт
  не меняет число строк), значения DAU / конверсии / retention, экспорт в S3 и его перезапись.

Тестовые данные удаляются до и после теста, поэтому тест можно запускать многократно.
