# SKJ Project - Message Broker

FastAPI projekt implementující asynchronní Message Broker přes WebSockety. Broker podporuje topics, publish/subscribe komunikaci, JSON, MessagePack, benchmark, integrační testy a durable queue logiku s ACK potvrzením.

## Správné spuštění projektu

Nejdřív přejdi do složky projektu:

```bash
cd /mnt/c/Users/balus/Desktop/skj/messageBroker/Skj_project
```

Aktivuj virtuální prostředí:

```bash
source ../venv/bin/activate
```

Nainstaluj všechny balíčky:

```bash
python -m pip install -r requirements.txt
```

Pokud by některý balíček chyběl, důležité balíčky pro tento projekt jsou:

```bash
python -m pip install fastapi uvicorn sqlalchemy alembic python-multipart websockets msgpack pytest pytest-asyncio httpx
```

Aplikuj databázové migrace:

```bash
python -m alembic upgrade head
```

Spusť server:

```bash
uvicorn main:app --reload
```

API dokumentace pro REST endpointy je na:

```text
http://127.0.0.1:8000/docs
```

Pozor: WebSocket endpoint se ve Swagger `/docs` nezobrazuje, protože OpenAPI dokumentuje hlavně HTTP endpointy.

## Spuštění klienta

Subscriber v JSON režimu:

```bash
python mb_client.py sensors json subscribe
```

Publisher v JSON režimu:

```bash
python mb_client.py sensors json publish --message '{"temperature": 22.5}'
```

Subscriber v MessagePack režimu:

```bash
python mb_client.py sensors msgpack subscribe
```

Publisher v MessagePack režimu:

```bash
python mb_client.py sensors msgpack publish --message '{"temperature": 22.5}'
```

## Spuštění testů

Používej `python -m pytest`, aby se testy spustily přes Python z aktivního virtuálního prostředí:

```bash
python -m pytest test_broker.py -v
```

Jednotlivé skupiny testů:

```bash
python -m pytest test_broker.py::test_connection_and_disconnect -v
python -m pytest test_broker.py::test_correct_routing -v
python -m pytest test_broker.py::test_topic_isolation -v
python -m pytest test_broker.py::test_publish_persists_undelivered_message -v
python -m pytest test_broker.py::test_ack_marks_message_delivered -v
python -m pytest test_broker.py::test_reconnect_receives_pending_message -v
python -m pytest test_broker.py::test_delivered_message_is_not_replayed_after_ack -v
python -m pytest test_broker.py::test_msgpack_publish_deliver_and_ack -v
```

## Spuštění benchmarku

Nejdřív musí běžet server:

```bash
uvicorn main:app --reload
```

V druhém terminálu spusť:

```bash
python benchmark.py --mode json
python benchmark.py --mode msgpack
```

Benchmark vypíše počet odeslaných zpráv, počet přijatých zpráv, čas a propustnost v `msg/s`.

Pro Úkol 3 benchmark ve výchozím nastavení měří rychlý broker bez durable DB zápisu:

```bash
python benchmark.py --mode json --publishers 5 --subscribers 5 --messages 10000
python benchmark.py --mode msgpack --publishers 5 --subscribers 5 --messages 10000
```

Pokud chceš změřit i durable queue režim z Úkolu 5, přidej `--durable`. Tento režim je výrazně pomalejší, protože každá zpráva se ukládá do SQLite a potvrzuje přes ACK:

```bash
python benchmark.py --mode json --publishers 2 --subscribers 2 --messages 100 --durable
```

## Úkol 1: WebSocket Broker

Broker je implementovaný v `main.py` endpointem:

```python
@app.websocket("/ws/broker/{topic}")
```

Topic je v URL, například:

```text
ws://localhost:8000/ws/broker/sensors
```

Správu připojení řeší `ConnectionManager` v `broker_utils.py`. Udržuje mapu:

```python
topic -> set[WebSocket]
```

Metoda `connect()` přidá klienta do topicu, `disconnect()` ho odstraní a `broadcast()` rozešle zprávu klientům v topicu.

Testy k úkolu:

- `test_connection_and_disconnect` dokazuje, že se klient uloží do `manager.topics` a po odpojení se odstraní.
- `test_correct_routing` dokazuje, že zpráva poslaná do topicu dorazí klientovi v tomto topicu.
- `test_topic_isolation` dokazuje, že zpráva z topicu A nedorazí klientovi v topicu B.

## Úkol 2: Klient a JSON / MessagePack

Klient je v `mb_client.py`. Používá `asyncio` a knihovnu `websockets`.

Podporuje:

- `publish` - odeslání zprávy.
- `subscribe` - příjem zpráv.
- `json` - textový formát.
- `msgpack` - binární formát.

Formát se předává brokeru přes query parametr:

```text
?mode=json
?mode=msgpack
```

Test k úkolu:

- `test_msgpack_publish_deliver_and_ack` dokazuje, že MessagePack zpráva projde přes publish, deliver i ACK.

## Úkol 3: Benchmark

Benchmark je v `benchmark.py`. Pomocí `asyncio` vytvoří více publisherů a subscriberů.

Výchozí hodnoty:

- 5 publisherů.
- 5 subscriberů.
- 10 000 zpráv na publishera.

Benchmark dokazuje:

- broker zvládne více klientů současně,
- lze porovnat výkon JSON a MessagePack,
- ve výchozím režimu měří rychlý WebSocket broker pro Úkol 3,
- s parametrem `--durable` měří i perzistenci a ACK z Úkolu 5.

## Úkol 4: Automatizované testy

Testy jsou v `test_broker.py` a používají `pytest` + `FastAPI TestClient`.

Testy dokazují:

- připojení a odpojení klienta,
- správný routing zpráv,
- izolaci topiců,
- uložení zprávy do databáze,
- označení zprávy jako doručené přes ACK,
- opětovné doručení nedoručené zprávy po reconnectu,
- že potvrzená zpráva se znovu neposílá,
- funkčnost JSON i MessagePack režimu.

## Úkol 5: Durable Queue a ACK

Původní broker zprávy pouze přeposílal aktivním klientům. Pokud subscriber nebyl připojený, zpráva se ztratila. Úkol 5 přidal perzistentní frontu přes model `QueuedMessage` v `models.py`.

Model `QueuedMessage` obsahuje:

- `id` - ID zprávy.
- `topic` - topic zprávy.
- `payload` - obsah zprávy jako bytes.
- `payload_format` - `json` nebo `msgpack`.
- `created_at` - čas vytvoření.
- `is_delivered` - stav doručení.

Tok zprávy:

1. Publisher pošle `publish`.
2. Broker uloží zprávu do databáze jako `is_delivered=False`.
3. Broker pošle klientům `deliver` s `message_id`.
4. Subscriber pošle `ack`.
5. Broker nastaví `is_delivered=True`.

Testy k úkolu:

- `test_publish_persists_undelivered_message` dokazuje, že publish vytvoří DB záznam.
- `test_ack_marks_message_delivered` dokazuje, že ACK nastaví `is_delivered=True`.
- `test_reconnect_receives_pending_message` dokazuje, že nedoručená zpráva přežije výpadek klienta.
- `test_delivered_message_is_not_replayed_after_ack` dokazuje, že potvrzená zpráva se znovu neposílá.

## Asynchronní chování

Broker používá:

- `async def`,
- `await websocket.receive()`,
- `await send_text()`,
- `await send_bytes()`.

Klient i benchmark používají `asyncio`.

Protože SQLAlchemy session je synchronní, databázové operace v broker flow běží přes:

```python
run_in_threadpool(...)
```

Díky tomu synchronní práce s databází neběží přímo v async event loopu brokeru.

## Jak části souvisí

`mb_client.py` se připojuje na WebSocket endpoint v `main.py`. `main.py` používá `ConnectionManager` z `broker_utils.py`, model `QueuedMessage` z `models.py` a session z `database.py`. Migrace v `alembic/` vytváří tabulku pro durable queue. `test_broker.py` ověřuje chování brokeru a `benchmark.py` měří výkon.
