# REPORT - spoluprace s AI na projektu

## 1. Kontext spoluprace

Na projektu jsem pouzival AI asistenta jako partnera pro analyzu kodu, implementaci, ladeni a dokumentaci. Projekt se postupne posunul od puvodniho FastAPI message brokeru k sirsi aplikaci, ktera dnes spojuje:

- osobni cloud nad bucket/object modelem,
- WebSocket message broker,
- durable queue s ACK potvrzenim,
- image worker pro asynchronni zpracovani obrazku,
- testy, benchmark a dokumentaci.

AI mi nepomahala jen psat kod. Stejne dulezita byla pomoc s orientaci v projektu, s interpretaci chyb, s navrhem API a s vysvetlovanim souvislosti mezi REST API, brokerem, databazi, workerem a testy.

## 2. Hlavni zmeny, ktere jsme v projektu provedli

### 2.1 Prostredi a zavislosti

Na zacatku bylo potreba zkontrolovat, jestli projekt vubec jde spustit v `venv`. V ramci toho jsme:

- prosli projekt a dohledali chybejici knihovny,
- doplnili `requirements.txt`,
- resili rozdil mezi systemovym Pythonem a Pythonem z virtualniho prostredi,
- vysvetlili, proc se ma pouzivat `python -m pytest` misto samotneho `pytest`.

To pomohlo hlavne u chyb typu:

```text
ModuleNotFoundError: No module named 'msgpack'
ModuleNotFoundError: No module named 'fastapi'
```

### 2.2 Kontrola a doplneni ukolu 1 az 4

AI nejdriv projekt prosla a porovnala ho se zadanim. Zjistili jsme, ze puvodni aplikace uz mela:

- FastAPI broker pres WebSockety,
- topics,
- publish/subscribe logiku,
- benchmark,
- zakladni testy,
- bucket/file cast projektu.

Zaroven jsme identifikovali mezery, hlavne:

- nekompletni `requirements.txt`,
- nejasnosti kolem endpointu,
- slabejsi testovaci a dokumentacni vrstvu,
- pozdeji potrebu durable queue a ACK flow.

### 2.3 Implementace ukolu 5 - durable queue

Jedna z nejvetsich zmen byla implementace garantovaneho doruceni.

Doplnili jsme:

- model `QueuedMessage`,
- Alembic migraci pro durable zpravy,
- `publish -> deliver -> ack` protokol,
- nacitani nedorucenych zprav po reconnectu,
- oznacovani zprav jako `is_delivered=True`,
- pouziti `run_in_threadpool`, protoze broker cast pouziva synchronni SQLAlchemy session.

To byl dulezity architektonicky posun. Broker uz nebyl jen "preposilac zprav", ale zacal fungovat jako jednoducha persistentni fronta.

### 2.4 Image worker a asynchronni zpracovani obrazku

Pozdeji pribyla worker cast, ktera:

- nasloucha topicu `image.jobs`,
- stahne obrazek pres REST API,
- upravi ho pres NumPy,
- nahraje vysledek zpet,
- posle stav do `image.done`,
- potvrdi broker ACK.

To vyresilo zadani, kde se CPU-bound prace mela presunout mimo hlavni FastAPI server.

### 2.5 Refaktor API z file modelu na bucket/object model

Postupne jsme zjistili, ze stary `files` pristup uz do projektu moc nesedi. Proto jsme projekt uklidili a presli na jednotnejsi bucket/object rozhrani.

Vysledkem je, ze dnes je hlavni API postavene kolem:

- `buckets`,
- `objects`,
- `object_key`,
- `record_id`,
- `process` endpointu,
- billing endpointu.

To znamenalo:

- odstranit stare `files/*` endpointy,
- sjednotit upload na object endpoint,
- premenovat `object_id` na `object_key`,
- premenovat `file_id` na `record_id`,
- pozdeji premenovat i ORM a DB vrstvu z `files` na `objects`.

### 2.6 Databaze, Alembic a audit schematu

Dalsi velka cast byla revize databaze.

V ramci toho jsme:

- zkontrolovali aktivni tabulky,
- odstranili legacy `files` tabulku,
- uklidili schema na `buckets`, `objects`, `queued_messages`, `alembic_version`,
- dopsali a aplikacne pripravili Alembic migrace,
- overili SQLite integrity,
- vycistili databazi od dat,
- resetovali storage a temp adresare.

Soucasti teto casti byla i kontrola soft delete chovani. Zaver byl tento:

- soft delete funguje logicky spravne,
- objekt zmizi z API a z listu,
- billing se prepocita,
- ale fyzicky soubor na disku zustava,
- nejde tedy o verzovany archiv, ale o logicke smazani.

### 2.7 Pydantic validace a dokumentacni komentare

Do projektu jsme doplnili:

- Pydantic validaci broker zprav,
- Pydantic validaci process requestu,
- Pydantic validaci worker payloadu,
- prisnejsi schema pravidla,
- modulove a funkcni docstringy napric soubory.

To zlepsilo citelnost i odolnost aplikace proti spatnym vstupum.

### 2.8 `user_id` jako povinny parametr uploadu

V pozdejsi fazi jsme si vsimli, ze sloupec `objects.user_id` je plnen spatne a muze obsahovat `bucket_id`. To jsi spravne odhalil jako navrhovou chybu.

Proto jsme upload flow upravili tak, aby:

- `user_id` byl explicitni a povinny parametr uploadu,
- novy objekt bez `user_id` nesel ulozit,
- process job predaval `user_id` do workera,
- worker pri internim prepisu zachoval vlastnika objektu.

To byl dulezity navrhovy krok, protoze sloupec `user_id` pak konecne zacal opravdu znamenat vlastnika objektu.

### 2.9 README a dokumentace projektu

README jsme postupne pretvorili z neprehledneho textu na prakticky navod. Dnes obsahuje:

- jak spustit virtualni prostredi,
- instalaci balicku,
- Alembic migrace,
- start serveru,
- start workera,
- prehled endpointu,
- testy,
- benchmark,
- dulezite poznamky k fungovani systemu.

## 3. Priklady mych dotazu a jak AI reagovala

Nektere moje dotazy byly kratke a velmi prakticke. Napriklad:

```text
ahoj projdi tento kod a chci abzch mohl vztvorit venv prostredi
```

Tady AI pomohla rychle zmapovat projekt, doplnit chybejici zavislosti a vysvetlit, jak projekt dostat do spustitelneho stavu.

Dalsi dulezity dotaz byl:

```text
zkontroluj mi tento projek ta jen mi rekni jeho funknost
```

To vedlo k uzitecnemu shrnuti toho, co projekt realne dela. Pomohlo mi to oddelit puvodni bucket/file cast od brokeru a pozdeji i od workera.

Velmi dulezita byla i tato etapa:

```text
ok udelej kotrolu tohoto zadani
```

Po ni jsme porovnavali kod s jednotlivymi ukoly a AI dokazala rict nejen co funguje, ale i co je jen napul hotove a co chybi.

Pozdeji prisel i jeden z nejdulezitejsich dotazu:

```text
ok ted chci po tobe tuto implementaci zadani: Ukol 5 ...
```

Tady uz neslo jen o kontrolu, ale o skutecny navrh architektury a refaktor brokeru.

Dalsi silny moment byl, kdy jsi spravne zpochybnil nazvoslovi a architekturu API. Napriklad:

```text
onject_id se mysli id obrayku?
```

nebo:

```text
aha tak tam je nazev object id tak se mi to plete mohl by jsi provest reviyi takovychle zavadejicich nazvu?
```

To vedlo k refaktoru `object_id -> object_key` a pozdeji i k premenovani DB vrstvy.

A podobne dobre jsi trefil i problem s vlastnictvim objektu:

```text
super jen se te chci zeptat na DS tabulku objects tak tam mi vysvetli ten sloupec user_id proc je stejny jako bucket, tohle jsem nechtel. chci aby bylo user id predem zadano v parametrech
```

To byla uplne legitimni architektonicka pripominka a vedla k dalsimu zlepseni kodu.

## 4. V cem mi AI skutecne pomohla

AI byla nejuzitecnejsi v techto oblastech:

1. Rychla orientace v cizim nebo rozpracovanem kodu.
2. Pojmenovani problemu, ktere se na prvni pohled tvari jako nahodne chyby.
3. Vysvetlovani souvislosti mezi FastAPI, WebSockety, SQLAlchemy, Alembicem a testy.
4. Navrh durable queue logiky.
5. Refaktor API z puvodni file terminologie na object model.
6. Revize databaze, migraci a nepotrebnych tabulek.
7. Zavedeny Pydantic validace a lepsi vstupni kontroly.
8. Doplneni dokumentace a docstringu.
9. Pomoc pri ladeni worker flow a ACK logiky.
10. Sjednoceni projektu kolem bucket/object + broker + worker modelu.

Prakticky nejvic pomohla tam, kde bylo potreba rychle prepnout mezi navrhem, implementaci a vysvetlenim.

## 5. V cem AI selhala nebo nebyla dost dobra

Tohle je dulezita cast, protoze spoluprace nebyla bezchybna.

### 5.1 Nektere opravy byly prilis iterativni

Hlavne kolem testu `test_worker.py` a drive i `test_broker.py` jsem navrhoval nekolik opravnych kroku po sobe. Nektere byly uzitecne, ale nebyly napoprve presne.

Typicky problem byl:

- race condition v testu,
- ACK, ktery byl odeslan, ale jeste nebyl zapsan v DB,
- nebo nevhodne poradi `image.done` a ACK.

To znamenalo, ze jsme museli nekolikrat upravovat testy a worker flow, nez to bylo opravdu stabilni.

### 5.2 Obcas jsem navrhl reseni, ktere bylo funkcni, ale ne dost dobre navrzene

Priklad je prave `user_id`, ktery byl drive fallbackem navazany na `bucket_id`. To sice pomahalo kratkodobe drzet upload flow pri zivote, ale architektonicky to bylo spatne. Musel jsi me na to upozornit.

Stejne tak jsme pozdeji resili, ze `object_id` neni realne ID, ale klic nebo nazev objektu. Taky to nebyla dobra terminologie a bylo spravne ji zmenit.

### 5.3 Dokumentace byla obcas zbytecne dlouha nebo spatne rozdelena

Nejdriv jsem napsal prilis dlouhy `REPORT.md`, ktery byl vic technicka analyza nez rozumna reflexe spoluprace. Az po tvem upozorneni jsme oddelili:

- README jako praktickou dokumentaci,
- REPORT jako reflexi prace s AI.

To je dobry priklad toho, ze technicky spravne neznamena automaticky uzivatelsky dobre.

### 5.4 Nemel jsem stejne prostredi jako ty

Tohle bylo dulezite hlavne u testu a pri spousteni veci ve WSL. Nektere problemy jsem dokazal odvodit, ale ne vzdy jsem je mohl hned sam potvrdit v identickem prostredi. Proto byly nektere opravy spise postupne a potrebovaly tvoji zpetnou vazbu.

Prakticky to bylo videt treba u:

- `pytest` bez virtualniho prostredi,
- worker ACK race problemu,
- restartu serveru a workera po zmene kontraktu,
- benchmarku a jeho vykonnosti.

## 6. Co bylo nejuzitecnejsi z pohledu uceni

Diky te spolupraci jsem si odnesl nekolik dulezitych veci:

- WebSocket endpoint se nezobrazuje ve Swagger `/docs`, protoze to neni REST endpoint.
- `python -m pytest` je spolehlivejsi nez samotny `pytest`, kdyz resim `venv`.
- Durable broker neni jen o broadcastu, ale hlavne o ulozeni zpravy, identifikaci a ACK potvrzeni.
- Worker je samostatna soucast architektury, ne jen "nejaky skript navic".
- Pojmenovani v API je dulezite. Jakmile je matoucni, projekt se hure pouziva i obhajuje.
- Alembic a databazove schema je potreba drzet uklizene, jinak zbyvaji legacy vrstvy, ktere matou.

## 7. Celkove zhodnoceni spoluprace

AI mi pomohla projekt vyrazne posunout. Nejvetsi prinos nebyl jen v tom, ze napsala kusy kodu, ale v tom, ze dokazala:

- rychle analyzovat, co projekt dela,
- navrhnout dalsi kroky,
- vysvetlit chyby,
- reagovat na moje pripominky,
- a postupne srovnat projekt do konzistentnejsi podoby.

Zaroven ale platilo, ze nejlepsi vysledky prisly tehdy, kdyz jsem i ja presne rekl, co mi vadi nebo co chci jinak. Typicky:

- ze endpointy byly zbytecne matoucni,
- ze `user_id` nechci vazat na bucket,
- ze README ma byt prakticke,
- ze report ma popsat i chyby a ne jen uspechy.

Spoluprace tedy nebyla jednostranna. Nejlepe fungovala tehdy, kdyz AI navrhla smer a ja jsem ji korigoval podle realneho zameru projektu.

## 8. Strucne shrnuti

AI mi pomohla:

- zprovoznit projekt,
- doplnit zavislosti,
- zkontrolovat plneni zadani,
- dopsat durable queue,
- doplnit worker flow,
- sjednotit API na bucket/object model,
- uklidit databazi a migrace,
- posilit validaci a testy,
- zlepsit README a dokumentaci.

AI naopak nebyla dokonala v tom, ze:

- nektere opravy prisly az na vice pokusu,
- obcas navrhla technicky funkcni, ale navrhove slabe reseni,
- a bez tve zpetne vazby by nektere veci zustaly zbytecne komplikovane.

I pres tyto slabiny byla spoluprace prinosna a projekt je po ni citelne konzistentnejsi, lepe otestovany a lepe vysvetleny.

