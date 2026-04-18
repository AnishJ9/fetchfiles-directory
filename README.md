# Fetch Directory

National pet services directory — built as lead generation for [FetchFiles](https://www.fetch-files.com/).

## Purpose

Pet owners searching for groomers, boarders, daycares, sitters, and vets will find this directory via local SEO. When they view a listing, they'll see a "Store your pet's records with FetchFiles" CTA. When a business claims their listing, they become a B2B lead (groomers/boarders need vaccination records — FetchFiles is a natural fit).

## Scope (v1)

Metros:
- Atlanta, GA
- Tampa, FL
- Austin, TX
- Nashville, TN
- Asheville, NC

Categories:
- Veterinarians
- Groomers
- Boarders / kennels
- Daycares
- Pet sitters

Target: ~4,000–5,400 listings across the five metros.

## Repo Structure

```
pipeline/       Python data pipeline (OSM Overpass + association scrapes)
data/           Canonical JSON output + per-metro files
site/           Next.js 14 directory site
docs/           Schema, deploy notes
```

## Data Schema

See `docs/SCHEMA.md` for the canonical listing schema. All pipeline outputs and site inputs conform.

## Development

```bash
# Pipeline
cd pipeline && python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python run.py --metro atlanta

# Site
cd site && npm install && npm run dev
```

## Deploy

See `docs/DEPLOY.md`.
