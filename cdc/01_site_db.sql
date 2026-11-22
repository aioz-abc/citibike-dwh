-- «База сайта» для CDC: схема ozbekova в базе aiacademy.
-- Сущность: заявки на ремонт станций Citi Bike (maintenance_tickets).
create schema if not exists ozbekova;

create table if not exists ozbekova.maintenance_tickets (
    ticket_id    int primary key,
    station_id   text      not null,
    status       text      not null default 'open',  -- open / in_progress / closed
    priority     text      not null default 'medium', -- low / medium / high
    description  text,
    opened_at    timestamp not null default now(),
    updated_at   timestamp not null default now()
);

-- Полный образ строки в WAL (нужен Debezium для update/delete без потери полей).
alter table ozbekova.maintenance_tickets replica identity full;

insert into ozbekova.maintenance_tickets (ticket_id, station_id, status, priority, description, opened_at, updated_at) values
    (1,  '06439006-11b6-44f0-8545-c9d39035f32a', 'open',        'high',   'Сломан замок дока 12',        '2026-10-01 08:15', '2026-10-01 08:15'),
    (2,  '1986707977001495180',                  'in_progress', 'medium', 'Не работает экран киоска',    '2026-10-02 10:00', '2026-10-03 09:30'),
    (3,  '3eb10c63-ea4c-4161-b07d-a29034f7367a', 'closed',      'low',    'Граффити на стойке',          '2026-10-03 12:40', '2026-10-05 16:00'),
    (4,  'ba33c334-e17d-4478-ae3c-9c2f4dbfbb51', 'open',        'medium', 'Станция не принимает велосипеды', '2026-10-06 07:05', '2026-10-06 07:05'),
    (5,  '06439006-11b6-44f0-8545-c9d39035f32a', 'closed',      'high',   'Нет связи с сервером',        '2026-10-07 18:20', '2026-10-08 11:10'),
    (6,  '1986707977001495180',                  'open',        'low',    'Нужна покраска',              '2026-10-09 09:45', '2026-10-09 09:45'),
    (7,  '3eb10c63-ea4c-4161-b07d-a29034f7367a', 'in_progress', 'high',   'Повреждён платёжный терминал', '2026-10-10 14:30', '2026-10-11 08:00'),
    (8,  'ba33c334-e17d-4478-ae3c-9c2f4dbfbb51', 'open',        'medium', 'Шатается док 3',              '2026-10-12 11:11', '2026-10-12 11:11')
on conflict (ticket_id) do nothing;

-- Проверка ФТ-9 (подождать до 30 секунд и посмотреть ozbekova_ods.ods_maintenance_tickets):
-- update ozbekova.maintenance_tickets set status = 'closed', updated_at = now() where ticket_id = 1;
