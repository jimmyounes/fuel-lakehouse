# Data dictionary — prix-carburants.gouv.fr feeds

Measured on the daily file of 2026-11-10 and the yearly archive 2025. All three feeds (instant, daily, yearly) share the same XML schema: a `pdv_liste` root with one `pdv` (point de vente, station) element per station.

## Volumes

| File | Stations | `prix` | `rupture` | `fermeture` | Size (XML) |
|------|----------|--------|-----------|-------------|------------|
| Daily 2026-11-10 | 9,954 | 30,870 | 27,421 | 69 | 14 MB |
| Yearly 2025 | 14,420 | 5,297,800 | 62,745 | 14,401 | 386 MB |

**The current-year archive is live.** `annee/2026` downloaded on 2026-11-14 contained 4,757,835 price updates with the latest at 2026-11-13T23:59: the archive of the running year is refreshed daily, not published once in January. There is therefore no gap between the last complete year and today.

The daily and instant files are **snapshots**: one `prix` per station and fuel, the current price with the timestamp of its last update. The yearly archive is an **event log**: every price update of the year (about 440,000 per month in 2025). Stations that closed during the year are still present, which is why the yearly file has more stations than the daily one.

## Station (`pdv` attributes)

| Field | Type in XML | Example | Notes |
|-------|-------------|---------|-------|
| `id` | string | `1000001` | Stable station identifier. First two digits match the department. Primary key of the stations table. |
| `latitude` | integer | `4620100` | WGS84 latitude × 100,000. 7 stations have it empty in the daily file. |
| `longitude` | integer | `519800` | WGS84 longitude × 100,000. |
| `cp` | string | `01000` | Postal code. 4,373 distinct values, 96 departments represented. Keep as string (leading zeros). |
| `pop` | enum | `R` / `A` | R = route (9,510 stations), A = autoroute / highway (444). |

## Station children

| Element | Cardinality | Content |
|---------|-------------|---------|
| `adresse` | 1 | Street address, free text, never empty. Brand sometimes appears here. |
| `ville` | 1 | City name, free text, inconsistent casing (`BOURG-EN-BRESSE`, `Bourg-en-Bresse`). Never empty. |
| `horaires` | 0..1 | Present for 8,610 stations. Attribute `automate-24-24` ("1" for 5,667 stations). Contains 7 `jour` elements (`id` 1..7, `nom`, `ferme`="1" if closed that day), each with 0..n `horaire` (`ouverture`, `fermeture` as `HH.MM`). |
| `services` | 1 | List of `service` text elements. 27 distinct values (see below). |
| `prix` | 0..n | One per fuel currently sold. 648 stations have none in the daily file (closed or not reporting). |
| `rupture` | 0..n | Fuel shortages. 9,211 stations have at least one. |
| `fermeture` | 0..n | Station closures. |

## Price (`prix` attributes)

| Field | Type | Notes |
|-------|------|-------|
| `id` | enum | Fuel id: 1 Gazole, 2 SP95, 3 E85, 4 GPLc, 5 E10, 6 SP98. |
| `nom` | string | Fuel name, redundant with `id`. |
| `maj` | datetime | Last update, `YYYY-MM-DDTHH:MM:SS`, local time. |
| `valeur` | decimal | Price per litre in euros, 3 decimals. |

Fuel distribution and price range in the daily file:

| Fuel | Stations selling it | Min | Median | Max |
|------|---------------------|-----|--------|-----|
| Gazole | 9,071 | 1.530 | 2.179 | 2.900 |
| E10 | 7,071 | 1.880 | 1.990 | 2.590 |
| SP98 | 6,862 | 1.569 | 2.129 | 2.880 |
| E85 | 3,683 | 0.699 | 0.877 | 2.249 |
| SP95 | 2,695 | 1.169 | 2.088 | 2.710 |
| GPLc | 1,488 | 0.600 | 1.074 | 2.191 |

Freshness: 85% of prices were updated within the last 7 days; 46% on the day itself. The oldest `maj` is from 2009: some prices are stale and must be flagged in Silver.

## Shortage (`rupture` attributes)

| Field | Type | Notes |
|-------|------|-------|
| `id`, `nom` | enum | Fuel, same codes as `prix`. |
| `debut` | datetime | Start of the shortage. |
| `fin` | datetime or empty | End. Empty for 93% of entries in the daily file (still ongoing or never closed). |
| `type` | enum | `definitive` (19,636), `temporaire` (6,741), missing (1,044). |

A "definitive" shortage means the station stopped selling that fuel: it is how the feed encodes "this station does not sell GPLc". Only `temporaire` entries are real supply incidents.

## Closure (`fermeture` attributes)

| Field | Type | Notes |
|-------|------|-------|
| `type` | enum | `T` temporary (68), `D` definitive (1) in the daily file. |
| `debut`, `fin` | datetime | Period. |

## Services (27 distinct values, count of stations in the daily file)

Vente de gaz domestique (6,068), Automate CB 24/24 (5,715), Station de gonflage (5,418), Boutique alimentaire (4,277), Piste poids lourds (4,168), DAB (3,887), Boutique non alimentaire (3,572), Carburant additivé (3,394), Lavage automatique (3,350), Toilettes publiques (2,677), Lavage manuel (2,495), Relais colis (2,030), Restauration à emporter (1,948), Laverie (1,845), Vente d'additifs carburants (1,791), Location de véhicule (1,774), Wifi (1,407), Services réparation / entretien (1,219), Restauration sur place (1,189), Bornes électriques (1,015), Vente de fioul domestique (879), Vente de pétrole lampant (815), Espace bébé (584), Bar (572), Douches (467), Aire de camping-cars (434), GNV (40).

## What is NOT in the feed

- **Brand** (TotalEnergies, Leclerc, ...). Must be inferred from the address or joined from an external source (OpenStreetMap `amenity=fuel` with `brand`).
- **Commune INSEE code, department and region names.** Only the postal code. Join with the INSEE reference (postal code → commune → department → region).
- **Volumes sold.** Prices only.
- **Crude oil or wholesale prices.** Needed for pass-through analysis; external daily series (e.g. Brent).

## Quality issues to handle in Silver

- Stale prices (last update years ago) alongside fresh ones: flag with an age, filter by threshold in Gold.
- Outliers: E85 at 2.249 or GPLc at 2.191 are almost certainly typos; price ranges per fuel will be checked.
- 7 stations without coordinates, 648 without any price.
- City names with inconsistent casing and accents (ISO-8859-1 encoding).
- Shortages with empty `fin` for years: a "definitive" shortage is a product catalog fact, not an incident.
- `horaires` missing for 13% of stations.
