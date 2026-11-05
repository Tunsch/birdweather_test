# BirdWeather PUC Scraper

Dieses Python-Skript ruft Daten (Vogel-Erkennungen/Detections und Sensordaten) von BirdWeather-PUC-Stationen über die offizielle GraphQL-API ab. Die Ergebnisse werden pro Abfragezeitraum in übersichtlichen CSV-Dateien gespeichert.

## Voraussetzungen

Stelle sicher, dass Python installiert ist und installiere die benötigten Pakete über dein Terminal/Eingabeaufforderung:

```bash
pip install requests timezonefinder
```
*(Hinweis: `timezonefinder` ist optional, verbessert aber die Zuweisung von lokalen Zeitzonen erheblich, falls diese in der API fehlen).*

## Wichtige Eigenschaften

- **Robust bei Abbrüchen (Resume):** Das Skript speichert Daten für jede Station und jeden Sensortyp in kleinen Teildateien ab. Wenn du das Skript abbrechen musst (Strg+C) oder die API kurzzeitig streikt, kannst du exakt denselben Befehl später neu starten. Das Skript überspringt bereits geladene Teile.
- **Dynamisches Rate-Limiting:** Das Skript fängt `429 Too Many Requests` Fehler automatisch ab und wartet die vom Server vorgegebene Zeit, bevor es weiterarbeitet.

## Anwendungsbeispiele

Öffne dein Terminal im Ordner, in dem die Datei `birdweather_puc.py` liegt, und nutze die folgenden Befehle je nach Anwendungsfall.

### 1. Einzelner Tag (Standard: Deutschland)
Ruft alle Sensoren und Detections für deutsche Stationen an einem bestimmten Tag ab:
```bash
python birdweather_puc.py --start-date 2026-10-22
```

### 2. Zeitraum abfragen
Um mehrere Tage oder Monate abzurufen, nutze das Enddatum:
```bash
python birdweather_puc.py --start-date 2026-09-01 --end-date 2026-09-30
```
*Tipp für Langzeitdaten: Rufe Daten für sehr lange Zeiträume am besten monatsweise auf, anstatt ein ganzes Jahr in einem einzigen Befehl zu starten.*

### 3. Gezielt einzelne Stationen (über IDs) abfragen
Wenn du nur bestimmte Stationen prüfen möchtest, kannst du deren interne IDs (nicht die Namen!) übergeben. Der Länderfilter wird dabei automatisch ignoriert.
```bash
python birdweather_puc.py --start-date 2026-10-22 --stations 1234 5678 91011
```

### 4. Nur spezifische Sensoren abrufen (Performance-Boost)
Um Zeit zu sparen und API-Limits zu schonen, solltest du nur die Daten laden, die du auch auswertest. Sensoren wie Magnetometer (`mag`) erzeugen massiv Daten.
```bash
# Lädt Detections sowie Environment- und Licht-Daten
python birdweather_puc.py --start-date 2026-10-01 --end-date 2026-10-31 --sensors environment light
```

### 5. Inaktive Stationen ignorieren
Überspringt Stationen, die im gewählten Zeitraum laut Zeitstempel keinen Vogel registriert haben:
```bash
python birdweather_puc.py --start-date 2026-10-22 --only-active
```

## Ordnerstruktur der Ausgabe

Standardmäßig wird ein Ordner `birdweather_out` erstellt. Darin befindet sich ein Unterordner, der nach dem Zeitraum benannt ist (z. B. `2026-10-01_2026-10-31`).

Darin findest du nach Abschluss:
- `stations.csv`: Metadaten (Standort, Status) zu den abgefragten Stationen
- `detections.csv`: Alle Vogel-Erkennungen
- `sensor_<typ>.csv`: Die jeweiligen Sensordaten (z. B. `sensor_environment.csv`)
- `/parts/`: Ein temporärer Ordner für die Resume-Funktion. Dieser kann gelöscht werden, wenn der Abruf zu 100% fertiggestellt ist.

Alle Befehlszeilen-Argumente und ihre Funktion

Hier ist die vollständige Übersicht aller Parameter, die du beim Aufruf des Skripts verwenden kannst:

    --start-date

        Funktion: Legt den Beginn des Abrufzeitraums fest.

        Format: YYYY-MM-DD (z. B. 2026-10-22).

        Verhalten: Wird kein Enddatum angegeben, lädt das Skript nur die Daten für genau diesen einen Tag herunter.

    --end-date

        Funktion: Legt das Ende des Abrufzeitraums fest (optional).

        Format: YYYY-MM-DD (z. B. 2026-12-31).

        Verhalten: Muss auf oder nach dem Startdatum liegen. Das Skript ruft alle Daten vom Start bis zum Ende dieses Tages ab.

    --stations

        Funktion: Erlaubt den gezielten Abruf einzelner Stationen anhand ihrer ID.

        Eingabe: Eine oder mehrere IDs, getrennt durch Leerzeichen (z. B. --stations 1234 5678).

        Verhalten: Ignoriert jegliche Ortsfilter (--world oder Deutschland). Die globale Stationssuche wird übersprungen und nur die exakten IDs werden abgefragt.

    --sensors

        Funktion: Bestimmt, welche Sensordaten heruntergeladen werden.

        Eingabe: Eine oder mehrere Sensortypen, getrennt durch Leerzeichen (z. B. --sensors environment light).

        Mögliche Werte: environment, light, mag, accel, system, location.

        Verhalten: Standardmäßig werden alle Sensoren abgerufen. Eine gezielte Einschränkung (ohne die massiven mag und accel Daten) beschleunigt den Abruf extrem.

    --no-detections

        Funktion: Schaltet den Abruf der Vogelstimmen/Detections komplett aus.

        Verhalten: Das Skript lädt dann ausschließlich die Stations-Metadaten und die ausgewählten Sensordaten herunter.

    --out

        Funktion: Definiert den Namen des Hauptordners für die CSV-Dateien.

        Eingabe: Pfad/Ordnername (Standard: birdweather_out).

        Verhalten: Das Skript erstellt darin automatisch einen passenden Unterordner für das Datum oder den Zeitraum (z. B. 2026-01-01_2026-12-31).

    --world

        Funktion: Hebt die lokale Beschränkung auf Deutschland auf.

        Verhalten: Das Skript ruft (sofern nicht --stations genutzt wird) alle PUC-Stationen weltweit ab. Die Standard-Zeitzone für fehlende Koordinaten wird dabei automatisch von Europe/Berlin auf UTC umgestellt.

    --no-country-filter

        Funktion: Behält Randgebiete im Deutschland-Modus.

        Verhalten: Die API wird im Standardmodus mit einem Rechteck (Bounding Box) abgefragt, das Deutschland abdeckt, aber auch in Nachbarländer hineinragt. Normalerweise wirft das Skript danach alle Stationen weg, die nicht explizit "Germany" oder "Deutschland" im Land-Feld stehen haben. Dieser Parameter schaltet dieses nachträgliche Löschen ab.

    --only-active

        Funktion: Filtert "tote" Stationen aus der Liste.

        Verhalten: Prüft die Zeitstempel der ersten und letzten jemals gemachten Detection einer Station. Liegt die letzte Detection strikt vor deinem --start-date oder die allererste Detection nach deinem --end-date, wird die Station übersprungen, da sie im gewünschten Zeitraum ohnehin keine Daten geliefert hat.

    --workers

        Funktion: Bestimmt die Anzahl der parallelen Downloads (Threads).

        Eingabe: Zahl (Standard: 1).

        Verhalten: Steuert, wie viele Stationen zeitgleich abgefragt werden. Um den 429 Too Many Requests Fehler der API zu vermeiden, sollte dieser Wert bei 1, maximal bei 2 bis 3 liegen.

    --fresh

        Funktion: Erzwingt einen kompletten Neustart des Abrufs.

        Verhalten: Normalerweise speichert das Skript jede abgerufene Teildatei im parts-Ordner ab und überspringt diese bei einem erneuten Skript-Start (Resume-Funktion). --fresh löscht den parts-Ordner für den gewählten Zeitraum vorab, sodass alle Daten frisch von der API geladen werden.