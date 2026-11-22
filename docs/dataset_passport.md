# Паспорт датасета — ozbekova

**Тема:** доступность велосипедов Citi Bike (Нью-Йорк) и влияние погоды.
**Источник:** открытый фид GBFS Citi Bike — https://gbfs.citibikenyc.com/gbfs/gbfs.json

| № | Источник | Тип | Роль | Как загружается | stg → ods |
|---|----------|-----|------|-----------------|-----------|
| 1 | `data/citibike_stations.csv` (из фидов GBFS `station_information` + `station_status`) | CSV | **справочник** станций: id, название, координаты, вместимость | целиком при каждом запуске | `stg_stations` → `ods_stations` |
| 2 | `data/station_status/station_status_YYYY-MM-DD.csv` | CSV | **события с датой**: снимок состояния каждой станции (велосипеды, доки, статус) каждые 15 минут | файл за дату `ds` | `stg_station_status` → `ods_station_status` |
| 3 | Open-Meteo `https://api.open-meteo.com/v1/forecast` | внешний API (JSON, без ключа) | погода в Нью-Йорке за день: t° max/min, осадки, ветер | один запрос за дату `ds` | `stg_weather` → `ods_weather` |
| 4 | `ozbekova.maintenance_tickets` («база сайта») | таблица Postgres, CDC | заявки на ремонт станций (статусы open / in_progress / closed) | Debezium → Kafka → JDBC sink | `ods_maintenance_tickets` |
| 5 | Архив поездок `https://s3.amazonaws.com/tripdata/<YYYYMM>-citibike-tripdata.zip` → `data/trips/trips_daily_<YYYYMM>.csv` | CSV (месячный архив) | **история поездок**: при извлечении 4–5 млн поездок за месяц сворачиваются до день × станция × тип клиента × тип велосипеда | DAG `final_ozbekova_history`, помесячно | `stg_trips_daily` → `ods_trips_daily` |
| 6 | Open-Meteo Archive `https://archive-api.open-meteo.com/v1/archive` | внешний API | погода за каждый день того же месяца | один запрос на месяц | `stg_weather` → `ods_weather` |

## Как появляется файл событий

GBFS отдаёт только текущее состояние, поэтому DAG `final_ozbekova_collector` каждые 15 минут
скачивает `https://gbfs.citibikenyc.com/gbfs/en/station_status.json` и дописывает снимок
в файл за текущий день по времени Нью-Йорка (`collect_status_snapshot` в `dag/ozbekova_common.py`).
Повтор одного и того же снимка (то же `last_updated`) не дописывается.
За полный день: ~2 500 станций × 96 снимков ≈ 240 000 строк.

## История поездок

Живой фид GBFS не хранит прошлого, поэтому вопрос «как дождь влияет на спрос» решается
отдельным DAG `final_ozbekova_history` (schedule `@monthly`, catchup с июля по сентябрь 2026).
Сырые поездки в общий Postgres стенда не кладутся: задача `extract_trips` скачивает месячный архив
(~1 ГБ), сворачивает поездки (короче минуты — не считаются; в длительность поездка идёт максимум
3 часа) и пишет файл-агрегат ≈ 300 000 строк — он и загружается в `stg_trips_daily` как есть.

## Объём

| Файл | Строк | Колонок |
|------|-------|---------|
| citibike_stations.csv | 2 519 | 10 |
| station_status_<дата>.csv | ≈ 240 000 в день | 12 |
| ответ API | 1 строка в день | 4 метрики + сырой JSON |
| trips_daily_<YYYYMM>.csv | ≈ 300 000 в месяц (из 4–5 млн поездок) | 6 |

## Утверждение

- [ ] утверждено преподавателем: ______________  дата: __________
