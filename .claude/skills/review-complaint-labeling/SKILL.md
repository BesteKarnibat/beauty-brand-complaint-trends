---
name: review-complaint-labeling
description: Use when labeling Amazon beauty reviews with complaint types for this project (the complaint_label table in MongoDB), whether through the Claude API script or the MongoDB MCP server. DRAFT - not yet run.
---

# Labeling review complaints (draft)

## Goal
Explain *why* a brand's low-star share rose: label a sample of reviews per
flagged brand and year (2019 and 2022) with one complaint type.

## Input
- Reviews for flagged brands only, loaded into MongoDB Atlas (`reviews` collection).
- Sample: up to 50 low-star (rating ≤ 2) reviews per brand per year.
- Skip reviews with text under 20 characters (10.5% of reviews; too short to label).

## Label set (exactly one)
| Label | Means |
|---|---|
| `irritation` | rash, burning, breakout, allergic reaction |
| `formula_change` | "new formula", "they changed it", "not the same" |
| `packaging_leak` | leaked, broken, pump or cap failed |
| `smell_texture` | bad smell, greasy, sticky, wrong texture |
| `not_effective` | doesn't work, no results |
| `fake_or_wrong` | counterfeit, expired, wrong item, used |
| `other` | none of the above |

## Output (one JSON object per review, nothing else)
```json
{"review_key": "<user_id>|<parent_asin>|<timestamp>", "label": "formula_change",
 "confidence": 0.0-1.0, "evidence": "<short quote from the review, under 15 words>"}
```
Also store `model` and `labeled_at` on each document.

## Rules
- Label only from the review text; do not guess from the brand.
- If two labels fit, pick the one the reviewer complains about most; use `other` if unsure.
- Never send user IDs or anything beyond the review title and text to the model.

## Checks
- Hand-label 100–200 reviews; report agreement with Claude's labels per label.
- Report the label mix per brand for 2019 vs 2022 (query 04).
