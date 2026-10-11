# prusa-recorder

Zapisuje do plików wszystko, co drukarka Prusa z płytą Buddy (MK4, MK3.9, MINI, XL, CORE One) wysyła o sobie. Cel: debug przerywanego błędu MINTEMP stołu.

Bez zależności. Wystarczy Python 3.9+.

## Źródła danych

1. **Metryki Buddy** po UDP, port 8514. Format syslog + InfluxDB line protocol, wzięty z `src/common/metric_handlers.cpp` w Prusa-Firmware-Buddy. Gęste próbkowanie, każdy punkt ma znacznik czasu drukarki w mikrosekundach.
2. **Logi Buddy** po UDP, port 8515. Zapis surowy. Linie z MINTEMP, error, fault i podobnymi trafiają też do `events.log`.
3. **PrusaLink** (`/api/v1/status`, `/api/v1/job`), odpytywany co sekundę. Działa też po Wi-Fi. Niezależny podgląd stanu drukarki.

## Konfiguracja drukarki

1. Podłącz drukarkę **kablem Ethernet**, jeśli możesz. Część osób zgłasza, że metryki syslog idą tylko po kablu.
2. Na drukarce: Settings → Network → **Metrics & Log**. Ustaw "Allow" na **All** (przy "One" drukarka odrzuci adres, który nie jest już zapisany).
3. Wgraj na pendrive plik `metrics.gcode` i uruchom go jak wydruk. W firmware 6.1.x:
   - `M334 <ip> <port>` ustawia cel metryk (bez trzeciego parametru),
   - `M340 <ip> <port>` ustawia cel logów.
   Potem w menu Metrics & Log stuknij pozycje w sekcji bieżących ustawień ("Current"), żeby zapisać host i porty na stałe.
   Lista włączonych metryk (`M331`) resetuje się po restarcie. Najwygodniej wkleić cały plik do Start G-code w PrusaSlicerze.
4. Hasło do PrusaLink znajdziesz w Settings → Network → PrusaLink. Użytkownik to `maker`.
5. Wpuść przychodzący UDP 8514 i 8515 na firewallu komputera.

## Uruchomienie

```bash
python3 prusa_recorder.py \
  --printer 192.168.1.50 \
  --password 'haslo-z-prusalink' \
  --watch temp_bed=15:130
```

Konsola co 30 s pokazuje liczbę pakietów i ostatnie wartości. Najpierw sprawdź, czy przychodzą pakiety, a dopiero potem puszczaj wydruk. Zatrzymanie: Ctrl+C.

`--watch nazwa=min:max` zapisuje do `events.log` każdą wartość spoza zakresu. Nazwę metryki sprawdź w konsoli po pierwszych pakietach.

## Pliki wyjściowe

Każde uruchomienie tworzy `sessions/RRRRMMDD-GGMMSS/`:

| plik | zawartość |
|---|---|
| `metrics.jsonl` | jeden punkt metryki na linię, `v` to wartość, `t_printer` to czas drukarki w µs |
| `metrics_raw.log` | każdy pakiet metryk w surowej postaci |
| `logs_raw.log` | każdy pakiet logów w surowej postaci |
| `prusalink.jsonl` | jedno odpytanie PrusaLink na linię |
| `events.log` | start, stop, zgubione pakiety, wartości spoza zakresu, alarmy z logów, zmiany stanu drukarki |
| `summary.json` | podsumowanie po zatrzymaniu |

## Analiza

```bash
python3 analyze.py sessions/20261011-120000
python3 analyze.py sessions/20261011-120000 --metric temp_bed --tail 60 --csv bed.csv
```

Pokazuje listę metryk z min/max, największe skoki między kolejnymi próbkami oraz ostatnie sekundy przed końcem strumienia.

## Co przysłać do debugowania

Po awarii przyślij `events.log`, `summary.json`, wynik `analyze.py` i ostatnie ~2000 linii `metrics.jsonl` oraz `logs_raw.log`.

## Ograniczenia

- UDP gubi pakiety. Luki widać w `events.log` jako `PACKET_LOSS`.
- Firmware sprawdza MINTEMP na surowych odczytach ADC co ~164 ms. Metryka temperatury może nie złapać pojedynczego złego pomiaru. Brak szpilki w danych nie wyklucza więc usterki przewodu.
- Wersja formatu (`proto` w JSON) zależy od firmware. Skrypt zapisuje surowe pakiety, więc da się je przeparsować później.
