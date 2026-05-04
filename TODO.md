# ML Inference DAG Fix - XGBoost Load & Pandas KeyError

## Plan Steps
- [x] Step 1: Create `models/` directory and copy xgboost_demand_high/low.json there
- [ ] Step 2: Update `ml_lab/predict_monthly_demand.py` to use absolute model paths in `models/`
- [ ] Step 3: Fix `historical_demand` mapping by renaming `target_demand` during data read
- [ ] Step 4: Add model download/load validation logs and explicit failure messages
- [ ] Step 5: Run quick validation command for syntax/runtime safety
