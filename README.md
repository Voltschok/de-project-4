# Проект 5-го спринта

### Описание
В рамках проекта по построению хранилища данных (de-project-sprint-4) с использованием нескольких источников (БД MongoDB, API, БД PostgresSQL) были написаны DDL-скрипты для формирования STG, DDS и CDM-слоев хранилища, а также DAGs Airflow для автоматизации наполнения хранилища. 

### Структура репозитория
- `/src/dags`

### Как запустить контейнер
Запустите локально команду:

```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
--mount src=airflow_sp5,target=/opt/airflow \
--mount src=lesson_sp5,target=/lessons \
--mount src=db_sp5,target=/var/lib/postgresql/data \
--name=de-sprint-5-server-local \
sindb/de-pg-cr-af:latest
```

После того как запустится контейнер, вам будут доступны:
- Airflow
	- `localhost:3000/airflow`
- БД
	- `jovyan:jovyan@localhost:15432/de`
