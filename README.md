# SKJ Project - Personal Cloud, Message Broker, Image Worker

## Spusteni projektu

Nejdriv prejdi do slozky projektu:

```bash
cd Skj_project
```

Aktivuj virtualni prostredi.

Linux / WSL:

```bash
source ../venv/bin/activate
```

Windows PowerShell:

```powershell
..\venv\Scripts\Activate.ps1
```

Nainstaluj zavislosti:

```bash
python -m pip install -r requirements.txt
```

Aplikuj Alembic migrace:

```bash
alembic upgrade head
```

Spust server:

```bash
uvicorn main:app --reload
```

REST API dokumentace je pak na:

```text
http://127.0.0.1:8000/docs
```

Pokud chces pouzivat asynchronni zpracovani obrazku, spust v dalsim terminalu i workera:

```bash
python worker.py
```

## Co aplikace dela

Projekt spojuje tri casti do jedne aplikace:

1. osobni cloud nad bucket/object modelem,
2. WebSocket message broker s durable queue,
3. image worker pro asynchronni zpracovani obrazku.

Bucket/object cast umi ukladat objekty do bucketu, vest metadata v SQLite databazi, pocitat billing a podporuje soft delete.

Message broker funguje pres WebSockety, podporuje topics, publish/subscribe komunikaci, JSON i MessagePack a durable dorucovani s ACK potvrzenim.

Image worker nasloucha topicu `image.jobs`, stahuje objekt z REST API, provede NumPy operaci nad obrazkem, nahraje vysledek zpet a posle stav do topicu `image.done`.

## Databaze a migrace

Aplikace pouziva SQLite databazi `metadata.db` a Alembic migrace.

Aktivni tabulky jsou:

- `buckets` - bucket metadata a billing pocitadla,
- `objects` - metadata ulozenych objektu,
- `queued_messages` - durable broker zpravy,
- `alembic_version` - aktualni Alembic revize.

Aktualni schema je vedene pres migrace v `alembic/versions`.

## Hlavni endpointy

### `POST /buckets/`
Vytvori novy bucket.

Body:

```json
{
  "name": "my-bucket"
}
```

Pouziti:
- vytvoreni bucketu pro dalsi uploady.

### `PUT /buckets/{bucket_id}/objects/{object_key}`
Nahraje nebo prepise objekt v bucketu.

Parametry:
- `bucket_id` - identifikator bucketu,
- `object_key` - nazev nebo klic objektu,
- `user_id` - povinny query parametr,
- `file` - multipart upload souboru.

Poznamka:
- `user_id` je povinny pro kazdy upload,
- worker pri internim prepisu predava puvodni `user_id` dal.

### `GET /buckets/{bucket_id}/objects/{object_key}`
Stahne objekt z bucketu.

Parametry:
- `bucket_id` - identifikator bucketu,
- `object_key` - nazev objektu.

Pouziti:
- bezny download,
- interni worker download pred zpracovanim.

### `GET /buckets/{bucket_id}/objects/`
Vrati seznam vsech aktivnich objektu v bucketu.

Parametry:
- `bucket_id` - identifikator bucketu.

Vraci:
- `record_id`,
- `bucket_id`,
- `object_key`,
- `size`.

### `DELETE /buckets/{bucket_id}/objects/{object_key}`
Soft delete objektu.

Parametry:
- `bucket_id` - identifikator bucketu,
- `object_key` - objekt ke smazani.

Chovani:
- objekt zmizi z listu,
- nejde stahnout pres API,
- v DB zustane radek s `is_deleted = true`,
- fyzicky soubor na disku se nemaze.

### `GET /buckets/{bucket_id}/billing/`
Vrati billing a request statistiky bucketu.

Parametry:
- `bucket_id` - identifikator bucketu.

Vraci:
- `current_storage_bytes`,
- `ingress_bytes`,
- `egress_bytes`,
- `internal_transfer_bytes`,
- `count_write_requests`,
- `count_read_requests`.

### `POST /buckets/{bucket_id}/objects/{object_key}/process`
Spusti asynchronni zpracovani obrazku.

Parametry:
- `bucket_id` - identifikator bucketu,
- `object_key` - objekt, ktery se ma zpracovat.

Body:

```json
{
  "operation": "grayscale",
  "params": {}
}
```

Podporovane operace:
- `invert`
- `flip`
- `crop`
- `brightness`
- `grayscale`

Poznamka:
- endpoint vraci jen `processing_started`,
- skutecne zpracovani dela `worker.py`,
- worker musi bezet zvlast.

### `WS /ws/broker/{topic}`
WebSocket endpoint pro message broker.

Query parametry:
- `mode=json|msgpack`
- `role=subscriber|publisher`
- `durable=true|false`

Podporovane akce:
- `publish`
- `ack`

Priklady broker zprav:

Publisher:

```json
{
  "action": "publish",
  "topic": "image.jobs",
  "payload": {
    "operation": "grayscale",
    "object_key": "obrazek.png",
    "bucket_id": "my-bucket",
    "user_id": "david",
    "params": {}
  }
}
```

Subscriber ACK:

```json
{
  "action": "ack",
  "message_id": 42
}
```

## Spusteni workeru

Worker je potreba pro image processing flow.

```bash
python worker.py
```

Co worker dela:
- pripoji se na `image.jobs`,
- stahne obrazek pres REST API,
- zpracuje ho v `image_processor.py`,
- nahraje vysledek zpet,
- potvrdi broker ACK,
- posle status do `image.done`.

## Rucni test image process flow

1. Spust server:

```bash
uvicorn main:app --reload
```

2. Spust worker:

```bash
python worker.py
```

3. V `/docs` vytvor bucket pres `POST /buckets/`.

4. V `/docs` nahraj obrazek pres `PUT /buckets/{bucket_id}/objects/{object_key}`.

Vypln:
- `bucket_id`
- `object_key`
- `user_id`
- `file`

5. V `/docs` zavolej `POST /buckets/{bucket_id}/objects/{object_key}/process`.

Napriklad:

```json
{
  "operation": "invert",
  "params": {}
}
```

6. Znovu stahni objekt pres `GET /buckets/{bucket_id}/objects/{object_key}` a over, ze se zmenil.

Pokud chces sledovat statusy workera pres broker, spust v dalsim terminalu:

```bash
python mb_client.py image.done json subscribe
```

## Spusteni testu

Pouzivej `python -m pytest`, aby se testy spoustely v aktivnim virtualnim prostredi.

Vsechny hlavni testy:

```bash
python -m pytest test_broker.py -v
python -m pytest test_objects_api.py -v
python -m pytest test_worker.py -v
```

Co testy overuji:

- `test_broker.py`
  - pripojeni a odpojeni klientu,
  - routing zpravy do spravneho topicu,
  - topic isolation,
  - durable queue ulozeni,
  - ACK zpracovani,
  - replay nedorucene zpravy po reconnectu,
  - JSON i MessagePack chovani.

- `test_objects_api.py`
  - upload objektu,
  - list objektu,
  - soft delete,
  - validaci process requestu,
  - povinnost `user_id` pri uploadu.

- `test_worker.py`
  - 10 process jobu,
  - 10 completion statusu,
  - ACK durable zprav,
  - spolupraci serveru, brokeru a workera.

## Spusteni benchmarku

Nejdriv musi bezet server:

```bash
uvicorn main:app --reload
```

Potom benchmark:

```bash
python benchmark.py --mode json
python benchmark.py --mode msgpack
```

Rozsirene varianty:

```bash
python benchmark.py --mode json --publishers 5 --subscribers 5 --messages 10000
python benchmark.py --mode msgpack --publishers 5 --subscribers 5 --messages 10000
```

Durable benchmark:

```bash
python benchmark.py --mode json --publishers 2 --subscribers 2 --messages 100 --durable
```

## Manualni broker klient

Subscriber:

```bash
python mb_client.py sensors json subscribe
```

Publisher:

```bash
python mb_client.py sensors json publish --message '{"temperature": 22.5}'
```

MessagePack subscriber:

```bash
python mb_client.py sensors msgpack subscribe
```

MessagePack publisher:

```bash
python mb_client.py sensors msgpack publish --message '{"temperature": 22.5}'
```

## Co je dulezite zminit

- `/docs` zobrazuje jen REST endpointy, ne WebSocket broker.
- `process` sam obrazek neupravi; job jen zaradi do brokeru.
- pro image processing musi bezet i `worker.py`.
- `user_id` je ted povinny pro upload objektu.
- soft delete je logicky, ne fyzicky delete souboru.
- durable broker pouziva globalni ACK model: po prvnim platnem ACK se zprava oznaci jako dorucena.
- pokud menis schema, spust znovu `alembic upgrade head`.
