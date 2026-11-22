# Модель данных — ozbekova

Хранилище по доступности велосипедов Citi Bike (Нью-Йорк) и погоде.
База `aiacademy`, Postgres 16. Все даты `ds` — календарный день по времени Нью-Йорка.

```
CSV stations ─┐                                    ┌─ dm_daily_station_activity ──► ClickHouse ozbekova.dm_daily_station_activity
CSV status ───┼─► ozbekova_stg ─► ozbekova_ods ────┤
API weather ──┘                        ▲           └─ dm_station_size_monthly
ozbekova.maintenance_tickets ─ Debezium/Kafka ─┘ (ods_maintenance_tickets)
```

## Слои

| Слой | Схема | Назначение |
|------|-------|-----------|
| stg | `ozbekova_stg` | данные как есть, все колонки `text` + `_ds`, `_source_file`, `_loaded_at`; журнал `etl_load_log` |
| ods | `ozbekova_ods` | типы, понятные имена, первичные ключи, без дублей |
| datamart | `ozbekova_datamart` | витрины для ответов на вопросы одним запросом без JOIN |

## Таблицы, зерно, ключи

| Таблица | Зерно (одна строка =) | Ключ | Загрузка |
|---------|----------------------|------|----------|
| `stg_stations` | строка файла станций | — | truncate + insert целиком |
| `stg_station_status` | строка файла снимков | — | delete `_ds = ds` + insert |
| `stg_weather` | ответ API за день | — | delete `_ds = ds` + insert |
| `etl_load_log` | попытка задачи | `id` | insert при старте, update при завершении |
| `ods_stations` | станция | `station_id` | полная перезапись |
| `ods_station_status` | станция × момент снимка | `(station_id, snapshot_ts)` | delete `status_date = ds` + insert |
| `ods_weather` | день | `weather_date` | delete `weather_date = ds` + insert |
| `ods_maintenance_tickets` | заявка (текущее состояние) | `ticket_id` | CDC upsert / delete |
| `dm_daily_station_activity` | **день** | `activity_date` | delete день `ds` + insert |
| `dm_station_size_monthly` | **месяц × категория размера станции** | `(month_start, capacity_category)` | delete месяц `ds` + insert |

Повторный запуск за ту же дату удаляет и заново пишет ровно ту же порцию — дублей нет (НФТ-1).

### Вычисляемые поля ods

- `ods_stations.capacity_category`: `small` (< 20 доков), `medium` (20–39), `large` (≥ 40), `unknown` (0 или нет данных).
- `ods_station_status.station_state` — состояние станции в момент снимка:
  - `offline` — станция не установлена (`is_installed = false`) или не выдаёт велосипеды (`is_renting = false`);
  - `empty` — работает, но доступных велосипедов 0;
  - `full` — работает, но свободных доков 0;
  - `normal` — всё остальное.
- `ods_station_status.last_reported_at` — `null`, если в фиде заглушка (epoch < 10⁹, напр. `86400`).
- `ods_weather.is_rainy` — осадков за день ≥ 1 мм.

## Витрины и метрики

Снимок = одна строка `ods_station_status` (станция в момент сбора, раз в 15 минут).
«Работающая станция» = `station_state <> 'offline'`.

### `dm_daily_station_activity` — по времени, зерно: день

| Колонка | Определение |
|---------|-------------|
| `activity_date` | день |
| `stations_reported` | число разных станций в снимках за день |
| `snapshots_cnt` | число снимков за день |
| `empty_snapshots` | число снимков со `station_state = 'empty'` |
| `full_snapshots` | число снимков со `station_state = 'full'` |
| `offline_snapshots` | число снимков со `station_state = 'offline'` |
| `empty_share` | `empty_snapshots` / снимки работающих станций |
| `avg_bikes_available` | среднее `bikes_available` по снимкам работающих станций |
| `avg_ebikes_available` | среднее `ebikes_available` по снимкам работающих станций |
| `temp_max_c`, `temp_min_c`, `precipitation_mm`, `is_rainy` | **погода за день из API Open-Meteo** |

Копируется в ClickHouse: `ozbekova.dm_daily_station_activity`.
dbt-версия: `ozbekova_datamart.dm_daily_station_activity_dbt`.

### `dm_station_size_monthly` — по справочнику, зерно: месяц × категория

| Колонка | Определение |
|---------|-------------|
| `month_start` | первый день месяца |
| `capacity_category` | категория размера станции из `ods_stations` (`unknown`, если станции нет в справочнике) |
| `stations_cnt` | число разных станций категории со снимками в месяце |
| `snapshots_cnt`, `empty_snapshots`, `full_snapshots`, `offline_snapshots` | как в дневной витрине, за месяц |
| `rainy_day_empty_snapshots` | **`empty_snapshots` в дни, где `is_rainy` (API Open-Meteo)** |
| `empty_share` | `empty_snapshots` / снимки работающих станций |
| `avg_fill_rate` | среднее `bikes_available / (bikes_available + docks_available)` по снимкам работающих станций |

### `dm_hourly_station_state` — по часам, зерно: день × час × категория

| Колонка | Определение |
|---------|-------------|
| `activity_date`, `hour_of_day` | день и час снимка по времени Нью-Йорка (0–23) |
| `capacity_category` | категория размера станции из `ods_stations` |
| `snapshots_cnt`, `working_snapshots` | все снимки / снимки работающих станций |
| `empty_snapshots`, `full_snapshots` | как в дневной витрине |
| `empty_share` | `empty_snapshots` / `working_snapshots` |
| `avg_bikes_available` | среднее `bikes_available` по снимкам работающих станций |

Отвечает: **в какие часы и какие станции чаще остаются без велосипедов** (утро против вечера).
Долю за несколько дней считать как `sum(empty_snapshots) / sum(working_snapshots)`, не `avg(empty_share)`.

### `dm_station_daily` — по станциям, зерно: день × станция

Те же счётчики, что в часовой витрине, плюс `station_name`, `latitude`, `longitude`, `capacity`,
`capacity_category` из справочника, поэтому карта в Superset строится без JOIN.
Отвечает: **где в городе станции чаще пустеют.**

### `dm_weather_trips_daily` — поездки и погода, зерно: день (DAG истории)

| Колонка | Определение |
|---------|-------------|
| `trip_date`, `iso_weekday`, `is_weekend` | день, день недели (1 = пн), выходной |
| `trips_cnt`, `member_trips`, `casual_trips`, `ebike_trips` | поездки за день: все / подписчики / разовые / на электровелосипеде |
| `avg_duration_min` | средняя длительность поездки, мин |
| `temp_max_c`, `temp_min_c`, `precipitation_mm`, `is_rainy` | погода за день (архив Open-Meteo) |
| `rain_level` | `dry` < 1 мм, `rain` 1–10 мм, `heavy_rain` ≥ 10 мм |

Отвечает: **сколько поездок теряется в дождливые дни.** Сравнивать будни с буднями: спрос в выходные другой.
Проверки `dq_history`: поездки и погода есть за каждый день месяца, по строке витрины на день,
сумма `trips_cnt` = сумма `ods_trips_daily.trips`.

### Согласованность витрин

`empty_snapshots` аддитивна: за любой месяц
`sum(dm_daily_station_activity.empty_snapshots) = sum(dm_station_size_monthly.empty_snapshots)`,
а за любой день сумма дневной витрины равна сумме `dm_hourly_station_state` и сумме `dm_station_daily`.
Это проверяет задача `dq_checks` (15 проверок).

## Вопросы, на которые отвечают витрины (одним запросом, без JOIN)

1. **В какие дни октября 2026 станции чаще всего оставались без велосипедов и какая была погода?**
   ```sql
   select activity_date, empty_share, empty_snapshots, temp_max_c, precipitation_mm, is_rainy
   from ozbekova_datamart.dm_daily_station_activity
   where activity_date >= '2026-10-01' and activity_date < '2026-11-01'
   order by empty_share desc;
   ```
2. **Какие станции по размеру чаще пустеют и сколько таких случаев пришлось на дождливые дни?**
   ```sql
   select capacity_category, empty_share, empty_snapshots, rainy_day_empty_snapshots, avg_fill_rate
   from ozbekova_datamart.dm_station_size_monthly
   where month_start = '2026-10-01'
   order by empty_share desc;
   ```
3. **Сверка (ФТ-5):** сумма `empty_snapshots` за октябрь одинакова в обеих витринах.
   ```sql
   select sum(empty_snapshots) from ozbekova_datamart.dm_daily_station_activity
   where activity_date >= '2026-10-01' and activity_date < '2026-11-01';
   select sum(empty_snapshots) from ozbekova_datamart.dm_station_size_monthly
   where month_start = '2026-10-01';
   ```

## Проверки качества (ФТ-10)

Задача `dq_checks` (DAG витрин) и dbt-тесты:
- unique / not_null: `ods_stations.station_id`, `ods_station_status (station_id, snapshot_ts)`,
  `ods_weather.weather_date`, `ods_maintenance_tickets.ticket_id`,
  `dm_daily_station_activity.activity_date`, `dm_station_size_monthly (month_start, capacity_category)`;
- допустимые значения: `ods_maintenance_tickets.status ∈ {open, in_progress, closed}`,
  `ods_station_status.station_state ∈ {offline, empty, full, normal}`,
  `ods_stations.capacity_category ∈ {small, medium, large, unknown}`;
- за `ds` ровно одна строка погоды и одна строка дневной витрины;
- сверка `empty_snapshots` между витринами.
