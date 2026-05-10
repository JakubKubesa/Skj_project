# SKJ Project

Osobni cloud pro obrazky postaveny nad FastAPI, SQLite a WebSocket brokerem.
Projekt dnes spojuje tri hlavni casti:

- `main.py` - webove GUI, autentizace, bucket/object REST API, billing a integrovany message broker
- `worker.py` - image processing node pro asynchronni upravy obrazku pres NumPy
- `haystack_node.py` - samostatny Haystack storage node s append-only volume soubory

Prakticky jde o `S3-like` object storage aplikaci, ne o plne AWS S3 kompatibilni implementaci.

## Co aplikace umi

- registraci a prihlaseni uzivatelu
- osobni bucket pro kazdeho uzivatele
- upload, preview, download a soft delete obrazku
- billing nad bucketem
- asynchronni image processing pres worker
- WebSocket broker s durable queue, topics a ACK potvrzenim
- samostatny Haystack node pro zapis do velkych volume souboru

## Aktualni stav architektury

Je dobre vedet, co uz je hotove a co je zatim jen pripravene:

- gateway a broker dnes bezi v jedne FastAPI aplikaci v `main.py`
- worker je samostatny proces
- Haystack node je samostatny proces
- gateway zatim stale fyzicky uklada soubory do adresare `storage/`
- Haystack node je implementovany, ale gateway na nej zatim neni prepojena

To znamena:

- GUI dnes funguje nad gateway + worker flow
- Haystack endpointy a broker flow `storage.write` / `storage.ack` jdou pouzivat a testovat samostatne

## Rychle spusteni z konzole

### 1. Prejdi do projektu

```powershell
cd Skj_project
```

### 2. Vytvor a aktivuj virtualni prostredi

Windows PowerShell:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Linux / WSL:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Nainstaluj dependencies

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 4. Aplikuj databazove migrace

```bash
python -m alembic upgrade head
```

Tento krok vytvori nebo zaktualizuje `metadata.db`.

### 5. Spust hlavni gateway aplikaci

```bash
uvicorn main:app --reload
```

Po spusteni:

- GUI: `http://127.0.0.1:8000/`
- Swagger REST docs gateway: `http://127.0.0.1:8000/docs`

### 6. Volitelne spust worker

Bez workera pujde upload, preview, download i billing, ale neprobihaji asynchronni graficke upravy.

```bash
python worker.py
```

### 7. Volitelne spust Haystack node

Haystack node je samostatna sluzba pro ulohu 1.

```bash
python haystack_node.py
```

Po spusteni:

- Haystack docs: `http://127.0.0.1:8002/docs`
- Haystack read endpoint: `http://127.0.0.1:8002/volume/{volume_id}/{offset}/{size}`

## Kde co bezi

| Cast | Soubor | Default adresa | Ucel |
|---|---|---|---|
| Gateway + broker + GUI | `main.py` | `127.0.0.1:8000` | Web, REST API, billing, broker |
| Image worker | `worker.py` | klient k `:8000` | Zpracovani obrazku |
| Haystack node | `haystack_node.py` | `127.0.0.1:8002` | Append-only volume storage |

## Jak ovladat GUI

Po otevreni `http://127.0.0.1:8000/`:

1. Zaregistruj se nebo se prihlas.
2. Po prihlaseni uvidis svou galerii, bucket a billing.
3. Pres formular nahraj obrazek.
4. Kazdy obrazek se v galerii zobrazi s preview, velikosti a akcemi.
5. Muzes:
   - `Upravit` - poslat job do workera
   - `Stahnout` - stahnout aktualni verzi souboru
   - `Smazat` - provest soft delete
6. V prave casti / billing sekci vidis:
   - current storage
   - ingress
   - egress
   - internal transfer
   - write requests
   - read requests

### Operace v GUI

Podporovane image processing operace:

| Operace | Parametry |
|---|---|
| `grayscale` | zadne |
| `invert` | zadne |
| `flip` | zadne |
| `brightness` | `value` |
| `crop` | `x_start`, `y_start`, `width`, `height` |

### Co je dobre vedet

- preview se stahuje pres skryty endpoint a nezvysuje `read requests`
- delete je soft delete
- delete uz nezvysuje `write requests`
- pokud worker nebezi, `process` jen zaradi job a nic obrazoveho se nezmeni
- po uspesnem zpracovani GUI dostane live status pres WebSocket broker

## Databaze a uloziste

Projekt pouziva SQLite databazi `metadata.db`.

Hlavni tabulky:

- `users` - uzivatele
- `auth_sessions` - bearer token sessions
- `buckets` - buckety a billing countery
- `objects` - metadata objektu
- `queued_messages` - durable broker zpravy
- `alembic_version` - aktualni schema verze

Fyzicka data jsou dnes ukladana takto:

- gateway: `storage/<bucket_id>/<object_key>`
- Haystack node: `haystack_volumes/volume_<id>.dat`

## Autentizace

Verejne prihlasovaci endpointy vraceji bearer token.
Vsechny `/me/...` endpointy vyzaduji:

```http
Authorization: Bearer <token>
```

Interni low-level bucket endpointy navic vyzaduji:

```http
x-internal-source: true
x-internal-token: <INTERNAL_API_TOKEN>
```

## Verejna HTTP API

Toto jsou endpointy, ktere jsou urcene pro bezne pouziti z GUI nebo klienta.

### Gateway REST API

Tyto endpointy uvidis v `http://127.0.0.1:8000/docs`.

| Metoda | Cesta | Auth | Popis |
|---|---|---|---|
| `POST` | `/auth/register` | ne | vytvori uzivatele, osobni bucket a vrati bearer token |
| `POST` | `/auth/login` | ne | prihlasi uzivatele a vrati bearer token |
| `GET` | `/me` | ano | vrati profil aktualniho uzivatele |
| `GET` | `/me/objects` | ano | vrati vsechny aktivni objekty v osobnim bucketu |
| `PUT` | `/me/objects/{object_key}` | ano | upload nebo prepis objektu |
| `GET` | `/me/objects/{object_key}` | ano | download objektu |
| `DELETE` | `/me/objects/{object_key}` | ano | soft delete objektu |
| `GET` | `/me/billing` | ano | billing a request statistiky |
| `POST` | `/me/objects/{object_key}/process` | ano | zaradi image processing job do brokeru |

### Hlavni request/response tvary

#### `POST /auth/register`

Body:

```json
{
  "username": "alice",
  "password": "secret123"
}
```

Vraci:

```json
{
  "token": "bearer-token",
  "user": {
    "id": "uuid",
    "username": "alice",
    "bucket_id": "uuid-bucketu"
  }
}
```

#### `POST /auth/login`

Body je stejny jako u registrace.

#### `PUT /me/objects/{object_key}`

- `multipart/form-data`
- pole `file`
- `object_key` se bere z URL

Vraci:

```json
{
  "status": "ok",
  "bucket_id": "uuid-bucketu",
  "object_key": "pejsek.png",
  "record_id": "uuid-zaznamu",
  "size": 12345
}
```

#### `POST /me/objects/{object_key}/process`

Body:

```json
{
  "operation": "grayscale",
  "params": {}
}
```

Priklad pro `brightness`:

```json
{
  "operation": "brightness",
  "params": {
    "value": 35
  }
}
```

Priklad pro `crop`:

```json
{
  "operation": "crop",
  "params": {
    "x_start": 10,
    "y_start": 20,
    "width": 200,
    "height": 150
  }
}
```

Okamzita odpoved:

```json
{
  "status": "processing_started",
  "topic": "image.jobs",
  "bucket_id": "uuid-bucketu",
  "object_key": "pejsek.png",
  "operation": "grayscale"
}
```

### Haystack node API

Tohle je verejny endpoint nove samostatne sluzby `haystack_node.py`.
Neni soucasti gateway docs, ale vlastnich docs na `http://127.0.0.1:8002/docs`.

| Metoda | Cesta | Auth | Popis |
|---|---|---|---|
| `GET` | `/volume/{volume_id}/{offset}/{size}` | ne | precte presne dany byte range z volume souboru |

Poznamka:

- endpoint je v kodu bez autentizace
- defaultne je ale sluzba bindovana jen na `127.0.0.1`
- GUI tento endpoint zatim nepouziva

## Skryta a interni API

Ne vsechno je ve Swagger docs gateway. Cast endpointu je schvalne skryta, protoze slouzi GUI nebo internim sluzbam.

### Skryte, ale bezne pouzivane GUI endpointy

| Metoda | Cesta | Auth | Popis |
|---|---|---|---|
| `GET` | `/` | ne | vrati webove GUI |
| `GET` | `/me/objects/{object_key}/preview` | ano | vrati preview obrazku bez zapocitani user downloadu |

### Interni low-level bucket API

Tyto endpointy jsou skryte z docs a jsou urcene pro interni volani.
Vyuziva je dnes hlavne worker.

| Metoda | Cesta | Popis |
|---|---|---|
| `POST` | `/buckets/` | vytvori bucket |
| `GET` | `/buckets/{bucket_id}/objects/` | vrati vsechny aktivni objekty bucketu |
| `PUT` | `/buckets/{bucket_id}/objects/{object_key}` | upload nebo prepis objektu |
| `GET` | `/buckets/{bucket_id}/objects/{object_key}` | download objektu |
| `DELETE` | `/buckets/{bucket_id}/objects/{object_key}` | soft delete objektu |
| `GET` | `/buckets/{bucket_id}/billing/` | billing bucketu |
| `POST` | `/buckets/{bucket_id}/objects/{object_key}/process` | zaradi processing job pro konkretni bucket/object |

Nutne headers:

```http
x-internal-source: true
x-internal-token: <INTERNAL_API_TOKEN>
```

## Message broker

Broker bezi uvnitr `main.py` jako WebSocket endpoint:

```text
/ws/broker/{topic}
```

Swagger ho nezobrazuje, protoze jde o WebSocket, ne o REST endpoint.

### Query parametry brokeru

| Parametr | Hodnoty | Vyznam |
|---|---|---|
| `mode` | `json`, `msgpack` | format prenesenych zprav |
| `role` | `publisher`, `subscriber` | typ klienta |
| `durable` | `true`, `false` | jestli se zprava ma ulozit do `queued_messages` |

Priklad subscriberu:

```text
ws://127.0.0.1:8000/ws/broker/image.jobs?mode=json&role=subscriber&durable=true
```

Priklad publisheru:

```text
ws://127.0.0.1:8000/ws/broker/image.done?mode=json&role=publisher&durable=false
```

### Zakladni broker protokol

#### Publish

```json
{
  "action": "publish",
  "topic": "image.jobs",
  "payload": {
    "operation": "grayscale",
    "object_key": "pejsek.png",
    "bucket_id": "bucket-uuid",
    "user_id": "user-uuid",
    "params": {}
  }
}
```

#### Deliver

Toto broker posila subscriberovi:

```json
{
  "action": "deliver",
  "topic": "image.jobs",
  "message_id": 42,
  "payload": {
    "...": "..."
  }
}
```

#### ACK

Subscriber po uspesnem zpracovani durable zpravy posila:

```json
{
  "action": "ack",
  "message_id": 42
}
```

Broker vraci odpoved:

```json
{
  "action": "ack",
  "message_id": 42,
  "status": "ok"
}
```

Mozne `status` hodnoty broker ACK odpovedi:

- `ok` - broker zpravu oznacil jako dorucenou
- `ignored` - zprava uz byla ACKnuta driv nebo nebyla relevantni

### Durable vs non-durable

- `durable=true` - broker payload ulozi do tabulky `queued_messages`
- `durable=false` - zprava se jen rozesle aktivnim subscriberum

Durable flow se hodi pro joby, ktere nechces ztratit po restartu nebo reconnectu.

### Temata, ktera dnes v projektu existuji

| Topic | Publisher | Subscriber | Durable | Ucel |
|---|---|---|---|---|
| `image.jobs` | gateway | worker | ano | joby pro image processing |
| `image.done` | worker | GUI | ne | live statusy zpracovani |
| `storage.write` | pripravene pro gateway | Haystack node | ano | binarni write job do volume |
| `storage.ack` | Haystack node | pripravene pro gateway | ano | potvrzeni zapisu s offset metadata |

### Statusy v image processing flow

Existuji tri ruzne "status" vrstvy:

#### 1. REST odpoved po odeslani jobu

`POST /me/objects/{object_key}/process` vraci:

- `processing_started`

To znamena jen to, ze job byl uspesne zadan do brokeru.

#### 2. Worker statusy v topicu `image.done`

Worker publikuje:

- `completed`
- `failed`

Typicky payload pri uspechu:

```json
{
  "status": "completed",
  "operation": "grayscale",
  "bucket_id": "bucket-uuid",
  "object_key": "pejsek.png"
}
```

Typicky payload pri chybe:

```json
{
  "status": "failed",
  "operation": "crop",
  "bucket_id": "bucket-uuid",
  "object_key": "pejsek.png",
  "error": "detail chyby"
}
```

#### 3. Haystack write ACK payload

Haystack node neposila `status`, ale metadata o umisteni objektu:

```json
{
  "object_id": "uuid-z-gateway",
  "volume_id": 1,
  "offset": 10560,
  "size": 1024
}
```

## Billing

Bucket si drzi tyto countery:

- `current_storage_bytes`
- `ingress_bytes`
- `egress_bytes`
- `internal_transfer_bytes`
- `count_write_requests`
- `count_read_requests`

Prakticke chovani dnes:

- upload nebo prepis zvysuje `write requests`
- download zvysuje `read requests`
- preview nezvysuje user `read requests`
- worker download/upload se pocita do `internal_transfer_bytes`
- delete uz nezvysuje `write requests`

## Dulezite poznamky

- `/docs` na `:8000` ukazuje jen verejnou REST cast gateway
- WebSocket broker se ve Swaggeru nezobrazuje
- `/docs` na `:8002` patri Haystack node, ne gateway
- GUI dnes overuje hlavne gateway + worker flow
- Haystack node je zatim oddelena cast pripravena pro dalsi integraci
- broker je dnes integrovany v `main.py`, neni to separatni spustitelny proces

## Uzitecne konfiguracni promenne

### Gateway

- `INTERNAL_API_TOKEN`

### Worker

- `API_BASE_URL`
- `BROKER_BASE_WS`
- `INTERNAL_API_TOKEN`

### Haystack node

- `BROKER_BASE_WS`
- `HAYSTACK_WRITE_TOPIC`
- `HAYSTACK_ACK_TOPIC`
- `HAYSTACK_VOLUME_DIR`
- `HAYSTACK_MAX_VOLUME_SIZE_BYTES`
- `HAYSTACK_READ_MEDIA_TYPE`
- `HAYSTACK_HOST`
- `HAYSTACK_PORT`

## Typicky development flow

1. spust `uvicorn main:app --reload`
2. spust `python worker.py`
3. otevri GUI na `http://127.0.0.1:8000/`
4. zaregistruj se
5. nahraj obrazek
6. vyzkousej `grayscale`, `invert`, `brightness` nebo `crop`
7. sleduj zmeny v galerii a billing kartach

Pokud chces testovat Haystack node samostatne:

1. spust `python haystack_node.py`
2. otevri `http://127.0.0.1:8002/docs`
3. pouzij `GET /volume/{volume_id}/{offset}/{size}` az budes mit data zapsana do volume
