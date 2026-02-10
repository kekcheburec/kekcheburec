# Архитектура stateless worker-подов без дублей задач

## 1) Цель

Нужно, чтобы stateless worker-поды:

1. брали задачи из очереди;
2. выполняли их;
3. передавали результат дальше;
4. при рестартах подов не обрабатывали одну и ту же задачу одновременно и не дублировали финальный результат.

---

## 2) Ключевая идея

Используем **PostgreSQL как источник истины для состояния задач** + паттерн **lease (аренда)** + **fencing token** + **outbox**.

Это дает:

- атомарный захват задачи одним воркером;
- автоматический возврат задачи в очередь после падения воркера;
- защиту от "устаревшего" воркера, который пытается завершить задачу после потери владения;
- гарантированную отправку результата даже при сбоях сети/процесса.

---

## 3) Компоненты

1. **Task Producer / API**
   - создает задачи в таблице `tasks`;
   - использует `dedupe_key` (идемпотентный ключ), чтобы одинаковый запрос не создавал дубль.

2. **Worker (Deployment, N реплик)**
   - stateless: локально ничего не хранит;
   - в цикле: `claim -> process -> complete/fail`;
   - периодически продлевает lease (heartbeat).

3. **PostgreSQL**
   - таблица `tasks` хранит статус, lease, попытки, результат;
   - таблица `outbox` хранит события о готовых результатах.

4. **Outbox Dispatcher**
   - читает `outbox`;
   - отправляет результат во внешний сервис/шину;
   - помечает доставленным.

5. **Sweeper / Reaper (CronJob или фоновый процесс)**
   - находит просроченные lease;
   - переводит задачи обратно в `queued/retry`.

---

## 4) Модель данных

Пример (упрощенно):

```sql
create table tasks (
  id uuid primary key,
  dedupe_key text unique,
  payload jsonb not null,

  status text not null check (status in ('queued', 'in_progress', 'done', 'retry', 'dead')),
  attempt int not null default 0,
  max_attempts int not null default 10,
  priority int not null default 0,

  lease_owner text,
  lease_until timestamptz,
  claim_token bigint not null default 0,

  available_at timestamptz not null default now(),
  result jsonb,
  error text,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  completed_at timestamptz
);

create index tasks_poll_idx
  on tasks (status, available_at, priority desc, created_at)
  where status in ('queued', 'retry');

create table outbox (
  id bigserial primary key,
  task_id uuid not null references tasks(id),
  claim_token bigint not null,
  event_type text not null,
  payload jsonb not null,
  delivered_at timestamptz,
  created_at timestamptz not null default now(),
  unique (task_id, claim_token, event_type)
);
```

---

## 5) Протокол обработки задачи

### 5.1 Claim (захват) без гонок

В одной транзакции:

```sql
with cte as (
  select id
  from tasks
  where status in ('queued', 'retry')
    and available_at <= now()
    and (lease_until is null or lease_until < now())
  order by priority desc, created_at
  for update skip locked
  limit 1
)
update tasks t
set status = 'in_progress',
    lease_owner = $1, -- worker_id (pod_uid + instance_id)
    lease_until = now() + interval '30 seconds',
    claim_token = claim_token + 1,
    attempt = attempt + 1,
    updated_at = now()
from cte
where t.id = cte.id
returning t.*;
```

`FOR UPDATE SKIP LOCKED` гарантирует, что одну задачу одновременно не заберут два воркера.

### 5.2 Heartbeat (продление аренды)

Каждые 10 секунд:

```sql
update tasks
set lease_until = now() + interval '30 seconds',
    updated_at = now()
where id = $task_id
  and status = 'in_progress'
  and lease_owner = $worker_id
  and claim_token = $claim_token;
```

Если обновлено `0` строк - воркер потерял владение задачей и обязан остановить обработку.

### 5.3 Успешное завершение + публикация результата

Обе операции в одной транзакции:

```sql
begin;

update tasks
set status = 'done',
    result = $result_json,
    lease_owner = null,
    lease_until = null,
    completed_at = now(),
    updated_at = now()
where id = $task_id
  and status = 'in_progress'
  and lease_owner = $worker_id
  and claim_token = $claim_token;

-- Проверяем, что обновилась ровно 1 строка.
-- Иначе воркер "устарел" и результат нельзя записывать.

insert into outbox(task_id, claim_token, event_type, payload)
values ($task_id, $claim_token, 'task_done', $result_json);

commit;
```

### 5.4 Ошибка и ретрай

Если ошибка временная:

- `status = 'retry'`
- `available_at = now() + backoff(attempt)` (например, экспоненциальный)
- `lease_owner/lease_until = null`

Если превышен `max_attempts`:

- `status = 'dead'` (dead letter), алерт в мониторинг.

---

## 6) Поведение при рестартах подов

### Сценарий A: pod упал в середине обработки

1. Heartbeat прекращается.
2. `lease_until` истекает.
3. Sweeper переводит задачу в `retry`.
4. Другой pod забирает задачу новым `claim_token`.

### Сценарий B: старый pod "ожил" и пытается дозавершить

Его `claim_token` уже не актуален, поэтому `UPDATE ... WHERE claim_token = old_token` не обновит строки.
Итог: устаревший воркер не может перезаписать результат и сделать дубль.

---

## 7) Гарантии

1. **Нет конкурентного дубля обработки** (одновременный захват одной задачи) - за счет `SKIP LOCKED`.
2. **Нет дубля финальной фиксации результата** - за счет `claim_token` (fencing).
3. **Результат не теряется между "done" и отправкой наружу** - за счет `outbox`.
4. Во внешнем канале доставки обычно гарантия **at-least-once**; для "эффективно exactly-once" получатель должен принимать `idempotency key = task_id:claim_token`.

---

## 8) Kubernetes-рекомендации

1. `Deployment` для воркеров, несколько реплик.
2. `PodDisruptionBudget` (например, `minAvailable: 1`).
3. `preStop`:
   - перестать брать новые задачи;
   - попытаться завершить текущую в рамках `terminationGracePeriodSeconds`.
4. `readinessProbe`: pod готов только если есть соединение с БД.
5. `HPA`: масштабировать по метрике глубины очереди (`queued + retry`) и latency.

---

## 9) Набор метрик/алертов

- `tasks_claimed_total`
- `tasks_completed_total`
- `tasks_retried_total`
- `tasks_dead_total`
- `lease_renew_failed_total`
- `claim_conflict_total` (когда воркер потерял владение)
- `outbox_pending_count`
- `oldest_queued_task_age_seconds`

Алерты:

- растет `tasks_dead_total`;
- высокий `outbox_pending_count`;
- `oldest_queued_task_age_seconds` выше SLA.

---

## 10) Минимальный псевдокод воркера

```text
loop:
  task = claim_one_task()
  if no task:
    sleep(short_interval)
    continue

  start heartbeat(task.id, task.claim_token)
  result = process(task.payload)

  if result.ok:
    complete_task_and_write_outbox(task.id, task.claim_token, result)
  else:
    schedule_retry_or_dead(task.id, task.claim_token, result.error)
```

---

## 11) Почему это подходит именно для stateless pod

- pod можно убивать и пересоздавать в любой момент;
- вся критичная консистентность хранится в БД, а не в памяти процесса;
- любой новый pod продолжает работу без ручного восстановления состояния.
