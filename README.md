# SKJ Project
## Jak projekt spustit a testovat
1. Spuštění serveru
uvicorn main:app --reload

2. Spuštění klienta (Subscriber)
Odebírání zpráv v tématu 'test' přes MessagePack
python mb_client.py test msgpack subscribe

3. Odeslání zprávy (Publisher)
Publikování zprávy
python mb_client.py test msgpack publish --message "Ahoj z Pythonu!"

4. Spuštění testů (Důležité pro Úkol 5!)
Před odevzdáním nebo po jakékoliv změně v brokeru spusť:
pytest test_broker.py


## Asynchronní komunikace: Vlastní Message Broker (Pub/Sub)
Tento projekt implementuje zjednodušený asynchronní Message Broker postavený na návrhovém vzoru Publish/Subscribe pomocí FastAPI WebSockets.

🛠 Co bylo implementováno
Úkol 1: Jádro Brokera
Asynchronní architektura: Celý systém využívá asyncio pro neblokující I/O operace.

Connection Manager: Třída v broker_utils.py, která spravuje aktivní WebSocket spojení a mapuje je na konkrétní témata (Topics).

Broadcast: Funkce pro rozesílání zpráv všem odběratelům v reálném čase.

Úkol 2: Klient a Serializace
mb_client.py: Univerzální klientský skript podporující dva režimy:

subscribe: Čeká na zprávy v daném tématu.

publish: Odešle zprávu do tématu.

Formáty zpráv: Podpora pro standardní JSON (textový) a efektivní binární MessagePack (přes knihovnu msgpack).

Úkol 3: Benchmarking
benchmark.py: Skript pro zátěžové testování propustnosti systému.

Měření: Provádí srovnání mezi JSON a MessagePack formátem. Výsledky ukazují propustnost v jednotkách msg/s.

Poznámka: Při testování malého objemu dat vykazují oba formáty podobné výsledky kvůli režii čekání na doručení.

Úkol 4: Automatizované Testy
test_broker.py: Integrační testy využívající pytest a FastAPI TestClient.

Pokrytí: 1. Úspěšné připojení a odpojení klienta (cleanup v manageru).
2. Správné směrování zpráv (zpráva dorazí do správného tématu).
3. Izolace témat (zpráva z tématu A nedorazí do tématu B).

