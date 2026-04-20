# Report k použití AI při práci na projektu

## Kontext spolupráce

Při práci na projektu jsem používal AI asistenta jako konzultanta, kontrolora zadání a pomocníka při implementaci. Projekt se týkal FastAPI Message Brokeru přes WebSockety. Postupně jsme řešili vytvoření virtuálního prostředí, kontrolu existujícího kódu, splnění úkolů 1 až 4 a následně implementaci úkolu 5, tedy durable queues s ACK potvrzením.

AI nebyla použita jen na generování kódu, ale hlavně na vysvětlení souvislostí. Pomohla mi pochopit, proč se některé chyby dějí, například proč WebSocket endpoint není vidět ve Swagger dokumentaci, proč `pytest` běžel mimo virtuální prostředí nebo proč bylo potřeba doplnit chybějící balíčky do `requirements.txt`.

## Příklady mých dotazů

Během práce jsem se ptal například na tyto věci:

```text
ahoj projdi tento kod a chci abych mohl vytvorit venv prostredi
```

Na základě toho AI zkontrolovala projekt, našla `requirements.txt`, vysvětlila vytvoření virtuálního prostředí a upozornila na chybějící balíčky jako `msgpack`, `websockets`, `pytest`, `httpx` a `alembic`.

```text
zkontroluj mi tento projekt a jen mi rekni jeho funkcnost
```

AI popsala, že projekt obsahuje dvě hlavní části: bucket/file API a WebSocket Message Broker. Tím mi pomohla oddělit hlavní broker část od vedlejšího souborového API.

```text
ok udelej kontrolu tohoto zadani
```

Po zaslání zadání k úkolům 1 až 4 AI prošla kód a porovnala ho s požadavky. Upozornila, že broker funkčně existuje, ale endpoint je `/ws/broker/{topic}`, zatímco zadání uvádí `/broker`. Také upozornila, že `requirements.txt` neobsahuje všechny použité knihovny.

```text
ok ted chci po tobe tuto implementaci zadani: Úkol 5...
```

Tento dotaz vedl k návrhu a implementaci durable queue logiky. AI nejdříve vytvořila plán a potom podle něj doplnila model `QueuedMessage`, migraci, ACK protokol, úpravy klienta, benchmarku a testů.

## Kde AI výrazně pomohla

Největší pomoc byla při ladění prostředí. Při spuštění klienta se objevila chyba:

```text
ModuleNotFoundError: No module named 'msgpack'
```

AI správně vysvětlila, že problém není v kódu klienta, ale v chybějící závislosti ve virtuálním prostředí a v `requirements.txt`.

Další důležitá pomoc byla při chybě:

```text
websockets.exceptions.InvalidStatus: server rejected WebSocket connection: HTTP 404
```

AI vysvětlila, že WebSocket endpoint se nezobrazuje ve `/docs`, protože Swagger ukazuje HTTP endpointy, ne WebSocket routy. Zároveň pomohla rozlišit, jestli běží správný `uvicorn` server a jestli je spuštěný ze správné složky.

Velmi užitečné bylo také vysvětlení problému s testy:

```text
pytest-7.4.4 ... -- /usr/bin/python3
ModuleNotFoundError: No module named 'fastapi'
```

AI poznala, že příkaz `pytest` se spouští přes systémový Python, zatímco `python` ukazuje do virtuálního prostředí. Správné řešení bylo používat:

```bash
python -m pytest test_broker.py -v
```

To mi pomohlo pochopit rozdíl mezi `pytest` a `python -m pytest`.

## Implementace úkolu 5

U úkolu 5 AI navrhla zavést jednotný protokol:

- `publish` - publisher posílá zprávu brokeru,
- `deliver` - broker doručuje zprávu subscriberovi,
- `ack` - subscriber potvrzuje zpracování zprávy.

Do databáze byl přidán model `QueuedMessage`, který ukládá topic, payload, formát zprávy, čas vytvoření a stav `is_delivered`.

AI také navrhla použít `run_in_threadpool`, protože projekt používá synchronní SQLAlchemy session. Tím se databázové operace v broker flow nespouští přímo v async event loopu.

Toto řešení jsem použil hlavně v těchto operacích:

- načtení nedoručených zpráv při připojení subscribera,
- uložení zprávy při publish,
- označení zprávy jako doručené při ACK.

## Kde AI nebyla dostatečná

AI několikrát navrhla řešení, které bylo teoreticky správné, ale v praxi narazilo na chování testovacího prostředí.

Například při úpravě testů se test:

```text
test_correct_routing
```

začal zasekávat. AI nejdříve předpokládala race condition a přidala timeout helper. To sice zlepšilo diagnostiku, ale problém úplně nevyřešilo. Potom navrhla další úpravy testů a brokeru. Tady bylo vidět, že AI nedokázala testy spustit přímo ve stejném WSL prostředí, takže neměla plnou zpětnou vazbu jako já.

Další slabší místo bylo, že AI původně vytvořila příliš dlouhý `REPORT.md`. Report měl přes 400 řádků a byl spíš technickou dokumentací než stručným reportem. Musel jsem ji požádat, aby text zkrátila a přesunula praktické informace do README.

AI také zpočátku neoddělila dostatečně jasně dvě věci:

- README jako praktický návod ke spuštění a testování,
- REPORT jako reflexi práce, použití AI a průběhu řešení.

Po upřesnění zadání ale dokumenty upravila správným směrem.

## Kde byla AI nejvíce užitečná

Nejvíce mi pomohla v těchto oblastech:

1. Vysvětlení projektu jako celku.
2. Kontrola, jestli kód odpovídá zadání.
3. Doplnění chybějících závislostí.
4. Návrh durable queue architektury.
5. Vysvětlení ACK mechanismu.
6. Vysvětlení, proč použít `run_in_threadpool`.
7. Doplnění testů pro perzistenci zpráv.
8. Vysvětlení rozdílu mezi JSON a MessagePack.
9. Vysvětlení problémů s virtuálním prostředím a pytestem.

AI tedy nebyla jen generátor kódu, ale hlavně průběžný pomocník při rozhodování a ladění.

## Co jsem si z toho odnesl

Díky práci s AI jsem lépe pochopil, jak spolu souvisí WebSocket endpoint, connection manager, klient, databáze a testy. U durable queue jsem pochopil, že nestačí zprávu pouze poslat přes WebSocket. Pokud má být doručení garantované, musí broker zprávu uložit, přidělit jí ID a čekat na potvrzení ACK.

Také jsem si uvědomil, že automatizované testy nejsou jen formalita. Pomohly odhalit problémy s doručováním zpráv, reconnectem a potvrzováním zpráv. Při práci s Python projektem jsem si navíc ověřil, že je důležité spouštět příkazy přes správné virtuální prostředí.

## Shrnutí

AI mi významně pomohla s návrhem, implementací i vysvětlením projektu. Největší přínos byl v rychlé kontrole zadání, vysvětlení chyb a návrhu architektury durable queue. Slabší byla tam, kde nemohla sama spustit testy ve stejném prostředí jako já, a také bylo nutné ji korigovat v rozsahu dokumentace.

Celkově ale spolupráce pomohla projekt dokončit rychleji a s lepším pochopením toho, jak jednotlivé části brokeru fungují.
