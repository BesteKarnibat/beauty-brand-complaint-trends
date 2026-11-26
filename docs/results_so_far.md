# Results so far

All numbers are **before deduplication** (~1% of rows are repeats; expected to change
results very little, to be confirmed by a rerun).

## Data size

| Category | Reviews | Products | Raw size (reviews + meta) |
|---|---|---|---|
| Beauty_and_Personal_Care | 23,911,390 | 1,028,914 | 11.0 GB + 2.8 GB |
| Health_and_Household | 25,631,345 | 797,563 | 11.4 GB + 2.5 GB |
| All_Beauty | 701,528 | 112,590 | 0.33 GB + 0.21 GB |
| **Total** | **50,244,263** | **1,939,067** | **26.3 GiB** |

## Finding 1: low-star reviews rose about 6 points everywhere

Share of 1–2 star reviews, 2019 → 2022:

| Category | 2019 | 2022 | Change |
|---|---|---|---|
| Beauty_and_Personal_Care | 15.2% | 21.3% | +6.05 pts |
| Health_and_Household | 13.5% | 19.8% | +6.3 pts |
| All_Beauty | 19.3% | 25.8% | +6.5 pts |

So a brand only has a real problem if it rose **faster than its category**.

## Finding 2: brands that got worse faster than their category (2019 → 2022)

Beauty_and_Personal_Care, brands with ≥ 1,000 reviews in both years (`queries/02_rise_above_category.sql`):

| Brand | 2019 | 2022 | Rise beyond category (pts) |
|---|---|---|---|
| TecUnite | 17.3% | 63.4% | +40.0 (outlier, check product mix) |
| BTYMS | 12.1% | 38.6% | +20.4 |
| iMethod | 19.4% | 38.8% | +13.3 |
| EAONE | 12.5% | 30.2% | +11.7 |
| LKE | 14.8% | 31.8% | +11.0 |
| CRYSTAL Deodorant | 18.1% | 35.0% | +10.9 |
| HSI PROFESSIONAL | 13.5% | 30.4% | +10.8 |
| Alba Botanica | 19.9% | 36.7% | +10.7 |
| Viking Revolution | 6.3% | 22.5% | +10.2 |
| Irish Spring | 6.5% | 22.8% | +10.2 |
| Clean & Clear | 11.8% | 27.2% | +9.3 |
| Got2B | 12.7% | 27.5% | +8.7 |
| Sky Organics | 10.6% | 25.3% | +8.6 |
| Mitchum | 15.0% | 29.6% | +8.5 |
| Haus Laboratories | 10.8% | 25.2% | +8.3 |

Pattern to explain: deodorant and body-care brands keep showing up.

### After dedupe (Nov 26, `build_gold.py` + query 02 rerun)

Removing duplicates moved every rate by 0.5 points or less, and 14 of the 15 brands are the same.
- **BTYMS** is now #1 at +20.1 (12.0% → 38.2%), followed by iMethod +13.4, EAONE +11.6,
  CRYSTAL Deodorant +10.9, LKE +10.9.
- **TecUnite** leaves the table only because its 2022 count fell from just over 1,000 to 998,
  under the 1,000-review cutoff. Its rate is unchanged (17.5% → 63.4%, +39.9).
  We keep the 1,000 cutoff and footnote TecUnite in the deck and the dashboard.
- **Mitchum** (+8.0) is tied with Gold Bond just outside the top 15. **BeautifyBeauties** (+8.3)
  and **Gold Bond** (+8.0) are new in it.
- The category rose 6.03 points (15.215% → 21.243%), was 6.05.


## Finding 3: ratings are J-shaped

Beauty_and_Personal_Care: 1★ 11.9% · 2★ 5.3% · 3★ 7.2% · 4★ 10.9% · 5★ 64.7%.

## Checks that passed

- All_Beauty: 701,528 rows before and after the product join (no fan-out).
- Spark vs DuckDB: identical counts and rates on all 24 All_Beauty years.
- TecUnite 17.3% → 63.4% in both the Spark mart and DuckDB.
- Review volume grew from 41K (2010) to 3.7M (2021) in Beauty, so compare rates, not counts.
