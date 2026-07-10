# Real-Time CDC Data Pipeline

## Opis projektu
Projekt wdraża architekturę Change Data Capture (CDC) do przetwarzania strumieniowego w czasie rzeczywistym. Rejestruje zmiany w relacyjnej bazie operacyjnej i automatycznie buduje analityczny magazyn danych (Data Lake) przy użyciu formatu Parquet, zapewniając zerowe obciążenie dla aplikacji biznesowej.

## Architektura i technologie
* **PostgreSQL 15:** Baza operacyjna z włączoną replikacją logiczną (`pgoutput`).
* **Debezium:** Narzędzie CDC wychwytujące transakcje bezpośrednio z dziennika WAL bazy.
* **Apache Kafka:** Rozproszona szyna danych buforująca zdarzenia (skonfigurowana w trybie KRaft).
* **Apache Spark (PySpark):** Klaster analityczny agregujący dane w trybie Structured Streaming.
* **MinIO:** Lokalny, obiektowy magazyn danych (kompatybilny z Amazon S3) służący jako docelowy Data Lake.
* **Docker Compose:** Orkiestracja i konteneryzacja całego środowiska.

## Struktura projektu
* `docker-compose.yml` - Definicja infrastruktury i sieci.
* `init_db.py` - Skrypt startowy w Pythonie generujący ruch biznesowy.
* `postgres-connector.json` - Plik konfiguracyjny (payload) dla konektora Debezium.
* `spark/app/stream_job.py` - Aplikacja analityczna realizująca transformację zagnieżdżonych struktur JSON na format kolumnowy.

## Wymagania wstępne
* Docker Desktop 
* Python 3.x z zainstalowanymi bibliotekami klienckimi bazy danych

## Instrukcja uruchomienia

### 1. Uruchomienie infrastruktury
```bash
docker compose up -d
```

### 2. Konfiguracja Data Lake
W przeglądarce otwórz adres `http://localhost:9001` (użytkownik: `admin`, hasło: `password123`). 
Utwórz nowe wiadro (Bucket) o nazwie `datalake`.

### 3. Generowanie danych startowych
```bash
python init_db.py
```

### 4. Uruchomienie śledzenia zmian (Debezium)
```bash
curl.exe -i -X POST http://localhost:8083/connectors -H "Content-Type: application/json" --data "@postgres-connector.json"
```

### 5. Aktywacja silnika analitycznego (Spark)
```bash
docker compose restart spark-app
docker logs -f spark-app
```

## Weryfikacja działania
Otwórz interfejs przeglądarkowy MinIO i przejdź do wiadra `datalake`. W katalogu `customers/raw/` znajdziesz skompresowane pliki `.parquet`. Każde kolejne wywołanie skryptu `init_db.py` doda do tej lokalizacji nową paczkę przetworzonych danych po upływie zdefiniowanego interwału (10 sekund).

## Wyłączenie i czyszczenie środowiska
```bash
docker compose down -v
```
