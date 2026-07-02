# Påmindelse om indberetning til Danmarks Statistik

Robotten overvåger borgere i KMD Nexus (BSF), som har aktive grundindsatser i Børn og Unge Grundforløb, men mangler den lovpligtige statistikindsats til Danmarks Statistik, og opretter opgaver til sagsbehandlerne.

## Hvad gør robotten?

1. **Fyldning af arbejdskø (`--queue`):** Læser `Regelsæt.xlsx` for at finde relevante organisationer, henter alle borgere fra KMD Nexus for hver organisation og tilføjer berettigede borgere til arbejdskøen (dubletter springes over).
2. **Henter forløb og indsatser:** For hver borger i arbejdskøen hentes aktive forløb (Børn og Unge Grundforløb) samt alle indsatser fra KMD Nexus.
3. **Opretter manglende statistikindsats:** Hvis en borger har aktive grundindsatser men ingen aktiv statistikindsats, oprettes en ny statistikindsats automatisk — `Indberetning til Danmarks Statistik - Anbringelse` for borgere under 18 år, og `Indberetning til Danmarks statistik - Ungestøtte` for borgere på 18 år eller derover — og der ansøges via "Ansøg".
4. **Opgave ved flere statistikindsatser:** Hvis en borger har flere aktive statistikindsatser end grundindsatser, eller der er flere statistikindsatser end grundindsatser, oprettes en opgave til den ansvarlige sagsbehandler.
5. **Opgave ved fejlende leverandør:** For aktive statistikindsatser (tilstand: Iværksat eller Etableret) kontrolleres det, om leverandøren matcher den tilsvarende grundindsats. Hvis ikke, oprettes en opgave.
6. **Duplikatkontrol:** Inden der oprettes en opgave, kontrolleres det, om der allerede eksisterer en aktiv opgave af typen "Indsats til Danmarks Statistik". Sagsbehandleren slås op via Nexus-databasen og tildeles opgaven med 3 dages frist.
7. **Sporingsrapportering:** Afsluttede og delvise sager registreres i ODK-sporingssystemet. Sager uden ansvarlig sagsbehandler rapporteres særskilt.

## Forudsætninger

- Python ≥ 3.13
- [`uv`](https://docs.astral.sh/uv/) til pakkehåndtering
- Adgang til **Automation Server** (arbejdskø)
- Adgang til **KMD Nexus** (produktion og database)
- Adgang til **Odense SQL Server**

## Installation

```sh
uv sync
```

## Konfiguration

Credentials registreres i Automation Server:

- `KMD Nexus - produktion`
- `KMD Nexus - database`
- `Odense SQL Server`

`Regelsæt.xlsx` er inkluderet i repositoriet og definerer tre kolonner: **Organisation** (hvilke Nexus-organisationer der skal scannes), **Grundindsats** (navne på kvalificerende grundindsatser) og **Statistikindsats** (navne på tilsvarende statistikindsatser).

Miljøvariabler ud over de standard ATS-variabler:

| Variabel | Beskrivelse |
|---|---|
| `ATS_URL` | URL til Automation Server API (f.eks. `http://localhost/api`) |
| `ATS_TOKEN` | Bearer-token til Automation Server |
| `ATS_WORKQUEUE_OVERRIDE` | Valgfrit arbejdskø-ID til testbrug |

## Kørsel

```sh
uv run python main.py --queue   # Fyld arbejdskøen
uv run python main.py           # Behandl arbejdskøen
```

## Afhængigheder

| Pakke | Formål |
|---|---|
| `automation-server-client` | Klient til Automation Server — håndterer arbejdskøer, arbejdsemner og credentials |
| `odk-tools` | ODK-værktøjer inkl. opgavesporing (Tracker) og fejlrapportering |
| `kmd-nexus-client` | Klient til KMD Nexus — henter borgere, forløb og indsatser, og opretter opgaver og indsatser |
| `nexus-database-client` | Direkte databaseadgang til KMD Nexus til opslag af sagsbehandlere via aktivitets-ID |
| `openpyxl` | Læser `Regelsæt.xlsx`-konfigurationsfilen |
| `ruff` | Python-linting |

## GDPR og sikkerhed

Processen behandler følgende personoplysninger:

- **CPR-numre** og fødselsdatoer for børn og unge i Børn og Unge Grundforløb
- **Sensitive socialfaglige oplysninger** om anbringelse og ungestøtte
- **Sagsbehandlernavne** og organisatorisk tilhørsforhold

Data behandles udelukkende i KMD Nexus og Automation Server og gemmes ikke lokalt. Adgang bør begrænses til medarbejdere med tjenstligt behov.
