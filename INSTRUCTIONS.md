# Инструкция по развертыванию BDSpark

## Предварительные требования
- Docker
- Docker Compose

## Быстрый старт

1. Перейдите в исходную директорию проекта:
   ```bash
   cd BDSpark-main
   ```

2. Запустите сервисы:
   ```bash
   docker-compose up -d
   ```

3. Доступность сервисов:
   - **PostgreSQL**: `localhost:5432` (Пользователь: `admin`, Пароль: `adminpassword`, БД: `bdspark`)
   - **ClickHouse**: `localhost:8123` и `9000` (Пользователь: `default`, Пароль: `defaultpassword`, БД: `bdspark`)
   - **Jupyter Notebook (PySpark)**: `http://localhost:8888` (Токен можно найти в логах: `docker logs bdspark-jupyter`)

При первом запуске база данных PostgreSQL автоматически инициализирует схемы и загрузит тестовые данные из директории с CSV файлами.

## Запуск ETL пайплайнов

В проекте есть два скрипта PySpark для выполнения необходимых трансформаций:

1. `ETL_Postgres_StarSchema.py` - Читает сырые данные из таблицы `mock_data_raw` в PostgreSQL, трансформирует их в схему "звезда" (таблица фактов и таблицы измерений) и записывает обратно в PostgreSQL.
2. `ETL_ClickHouse_Reports.py` - Читает схему "звезда" из PostgreSQL, агрегирует данные для 6 различных витрин по заданиям и записывает итоговые агрегированные таблицы напрямую в ClickHouse.

### Как запустить

1. Откройте Jupyter Lab по адресу `http://localhost:8888` (используйте токен из логов).
2. Запустите терминал внутри Jupyter Lab (File -> New -> Terminal).
3. Запустите ETL для схемы "звезды" (PostgreSQL):
   ```bash
   spark-submit --packages org.postgresql:postgresql:42.6.0 ETL_Postgres_StarSchema.py
   ```
4. Запустите ETL для построения отчетов (ClickHouse):
   ```bash
   spark-submit --packages org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.4.6 ETL_ClickHouse_Reports.py
   ```

Для проверки результатов используйте DBeaver, чтобы посмотреть как БД PostgreSQL (`bdspark`), так и БД ClickHouse.