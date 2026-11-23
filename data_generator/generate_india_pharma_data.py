import os, csv, random
from datetime import datetime, timedelta

BASE_DIR = "mock_s3/bronze"
for sub in ["clinical", "manufacturing", "sales"]:
    os.makedirs(f"{BASE_DIR}/{sub}", exist_ok=True)

DRUGS = ["CDSCO-ONC-901", "CDSCO-DIA-402", "CDSCO-CAR-705", "CDSCO-RES-118"]
STATES = ["MH", "KA", "DL", "UP", "TN", "GJ", "TG", "HP", "WB", "RJ"]
DATE_FMTS = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"]

def rand_dt():
    d = datetime(2026, 4, 1) + timedelta(days=random.randint(0, 150))
    return d.strftime(random.choice(DATE_FMTS))

# 1. Clinical Trials (CTRI India)
with open(f"{BASE_DIR}/clinical/ctri_clinical_india.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["obs_id", "abha_raw_id", "cdsco_drug_code", "ctri_trial_id", "trial_phase", "state_code", "obs_date", "biomarker_level", "adverse_event", "gender", "age", "comorbidity_risk"])
    for i in range(1, 20001):
        obs_id = f"CTRI-OBS-{i if i % 40 != 0 else i-1}"
        bio = round(random.uniform(15.0, 420.0), 2) if i % 85 != 0 else 88888.8
        age = random.randint(20, 78) if i % 100 != 0 else -5
        w.writerow([obs_id, f"91-{random.randint(1000,6000)}-{random.randint(1000,9999)}-1234", random.choice(DRUGS), "CTRI/2026/04/065120", "Phase III", random.choice(STATES), rand_dt(), bio, random.choice(["Y", "N", "YES", "0"]), random.choice(["M", "F", "Male", ""]), age, random.choice(["Low", "Moderate", "High"])])

# 2. Manufacturing Batches (Zedlay India Plants)
PLANTS = [
    ("FAC-HP-01", "Zedlay Formulations Baddi", "HP"),
    ("FAC-TG-02", "Zedlay Biologics Genome Valley Hyderabad", "TG"),
    ("FAC-GJ-03", "Zedlay API Plant Ankleshwar", "GJ")
]
with open(f"{BASE_DIR}/manufacturing/zedlay_mfg_india.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["batch_lot_id", "cdsco_drug_code", "facility_code", "plant_name", "state_code", "who_gmp_status", "mfg_date", "input_kg", "output_kg", "qc_status", "mfg_cost_inr"])
    for i in range(1, 10001):
        fac, pname, st = random.choice(PLANTS)
        inp = round(random.uniform(500.0, 2000.0), 2)
        out = round(inp * random.uniform(0.88, 0.99), 2) if i % 70 != 0 else inp + 45
        w.writerow([f"ZED-IN-LOT-{i}", random.choice(DRUGS), fac, pname, st, random.choice(["Certified", "Certified", "Under_Audit"]), rand_dt(), inp, out, random.choice(["PASS", "PASSED", "FAIL", "N/A"]), round(random.uniform(450000, 2500000), 2)])

# 3. Cadway Commercial Sales India
with open(f"{BASE_DIR}/sales/cadway_sales_india.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["invoice_id", "cdsco_drug_code", "nmc_reg_number", "doctor_specialty", "prescriber_tier", "state_code", "sale_date", "promo_channel", "strips_sold", "gross_sales_inr", "marketing_spend_inr"])
    # Explicit SCD2 test records for NMC-MH-99999 (Silver on 2026-04-10 -> Platinum on 2026-07-15)
    w.writerow(["CAD-SCD2-TEST-1", "CDSCO-ONC-901", "NMC-MH-99999", "Oncology", "Silver", "MH", "10-04-2026", "MR", 50, 72500.00, 5000.00])
    w.writerow(["CAD-SCD2-TEST-2", "CDSCO-ONC-901", "NMC-MH-99999", "Oncology", "Platinum", "MH", "15-07-2026", "CME", 200, 290000.00, 15000.00])
    for i in range(1, 40001):
        strips = random.randint(10, 500) if i % 95 != 0 else -12
        gross = round(strips * random.uniform(180.0, 950.0), 2)
        spend = round(gross * 0.10, 2) if i % 7 != 0 else ""
        w.writerow([f"CAD-INV-26-{i}", random.choice(DRUGS), f"NMC-IN-{random.randint(10000, 11500)}", random.choice(["Oncology", "Cardiology", "Diabetology"]), random.choice(["Platinum", "Gold", "Silver"]), random.choice(STATES), rand_dt(), random.choice(["MR", "WHATSAPP", "CME", ""]), strips, gross, spend])

print("Indian Pharma Test Datasets generated successfully.")
