
WannCannaBis – Termin- und Angebots-App

Start (Windows):
1) Node.js installieren: https://nodejs.org/
2) ZIP entpacken.
3) run_windows.bat starten.
4) Browser: http://localhost:3000

Admin:
- Login: /admin  (Standard: admin / admin1234!)
- Schlüssel erstellen: Code + Gültigkeitstage.
- Verkäufer müssen bei Registrierung den Schlüssel angeben. Zugang läuft zum Schlüssel-Ablauf aus.

### Begrenzung der Zugänge pro Schlüssel
Beim Erstellen eines neuen Schlüssels kann nun neben Code und Gültigkeit auch **Max. Nutzer** festgelegt werden.  
Nur so viele Verkäufer können sich mit diesem Code registrieren, wie unter **Max. Nutzer** angegeben.  
Sobald die maximale Anzahl an Nutzungen erreicht ist, wird der Schlüssel automatisch als *used* markiert und weitere Registrierungen mit diesem Code werden abgelehnt.  
In der Übersichtstabelle der vergebenen Schlüssel im Admin‑Dashboard wird die Anzahl der bereits verwendeten Zugänge sowie der maximal zulässigen Nutzer angezeigt.

## Dauerhafter Betrieb & persistente Speicherung

Wenn die Anwendung auf dem eigenen Rechner läuft, werden Daten in der Datei `data.json` gespeichert.  
Viele kostenlose Hosting‑Plattformen schalten Web‑Services nach kurzer Inaktivität jedoch in einen Ruhemodus und setzen ihr Dateisystem zurück – dann gehen Eingaben verloren.  
Um dies zu vermeiden, sollte eine dauerhafte Datenbank genutzt werden.

Ein möglicher Weg besteht darin, die App bei einem Anbieter wie **Render** zu deployen und eine kostenlose PostgreSQL‑Datenbank zu verknüpfen.  
Render erzeugt dabei automatisch die Umgebungsvariable `DATABASE_URL`, die von der Anwendung genutzt wird, um alle Daten zusätzlich zur lokalen JSON‑Datei in der Datenbank zu speichern.  
Beim erneuten Starten der App wird der aktuelle Zustand aus der Datenbank geladen und die Daten stehen sofort wieder zur Verfügung.

Alternativ können kostenfreie Plattformen wie **Railway** oder **Fly.io** verwendet werden. Diese Anbieter stellen ebenfalls kostenlose Tarife mit integrierten Datenbanken bereit und sind als Alternativen zu Render geeignet【632957663506901†L289-L309】.  
Das Vorgehen ist identisch: Code aus diesem ZIP‑Projekt hochladen, eine Datenbank anlegen und die Variable `DATABASE_URL` in den Umgebungsvariablen hinterlegen.
