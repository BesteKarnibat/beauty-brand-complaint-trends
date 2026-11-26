# Data traps, measured

From `spark/eda_traps.py` over the full Parquet data (Nov 16, 2026).

| Trap | Measured | What to do |
|---|---|---|
| No review ID; key is user + product + time | Repeats: All_Beauty 7,276 · Beauty_and_Personal_Care 242,535 · Health_and_Household 245,296 · **total 495,107 (~1.0%)** | dropDuplicates on the key in Silver, rerun marts |
| `asin` is a variant (color, size), not the product | Variant share of reviews: All_Beauty 8.7% · Beauty 51.6% · Health 56.0% | Group on `parent_asin` (done) |
| Brand lives in two fields (per product) | Beauty: 69.6% `details.Brand` · 27.7% `store` only · 2.7% neither | `coalesce(details.Brand, store)`, then brand_alias |
| Price is often the text "None" | Missing: All_Beauty 84.3% · Beauty 63.0% · Health 58.5% of products | Price tier from dim_brand, not product price |
| Many reviews are very short | Under 20 characters: ~10.5% in every category | Too short for Claude to label; use stars only |
| Not every reviewer is a verified buyer | Verified: All_Beauty 90.5% · Beauty 91.1% · Health 92.6% | Rerun the ranking on verified-only as a check |
| A product could be in two category files | 0 of 1,939,067 products | No cross-category double counting |
| Timestamps | Milliseconds since epoch | Divide by 1000 before casting (done) |
| Coverage over time | 2023 ends in September; 2022–2023 thinner | Compare 2019 vs 2022, treat 2023 as partial |
| Brand names are messy | Top-30 examples: Rëvlóñ, Çlíñíque, Conair', Kiss--, Dove® | brand_key strips these; brand_alias for sub-brands |

At review level (after the join), 97.96% of Beauty_and_Personal_Care reviews have a brand
and 100% have product info.
