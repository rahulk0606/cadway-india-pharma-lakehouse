CREATE DATABASE IF NOT EXISTS cadway_india_dw;
USE cadway_india_dw;

-- 1. Shared Dimension: Indian Geography (All 28 States & 8 UTs)
CREATE TABLE dim_geography_india (
    geo_sk INT AUTO_INCREMENT PRIMARY KEY,
    state_code VARCHAR(5) UNIQUE NOT NULL,
    state_name VARCHAR(60) NOT NULL,
    zone VARCHAR(20) NOT NULL,
    city_tier VARCHAR(15) NOT NULL
);

-- 2. Shared Dimension: Indian Calendar Date (Indian FY: April 1 - March 31)
CREATE TABLE dim_date (
    date_sk INT PRIMARY KEY,
    full_date DATE NOT NULL,
    calendar_year INT,
    calendar_month INT,
    month_name VARCHAR(15),
    indian_fy VARCHAR(12),
    indian_fy_quarter VARCHAR(5),
    is_weekend TINYINT(1)
);

-- 3. Shared Dimension: Drug Product (SCD Type 2)
CREATE TABLE dim_product_scd2 (
    product_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    cdsco_drug_code VARCHAR(30) NOT NULL,
    molecule_name VARCHAR(100) NOT NULL,
    brand_name VARCHAR(100) NOT NULL,
    therapeutic_area VARCHAR(50),
    schedule_category VARCHAR(15),
    mrp_inr DECIMAL(10, 2),
    unit_mfg_cost_inr DECIMAL(10, 2),
    row_hash VARCHAR(64) NOT NULL,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE DEFAULT '9999-12-31',
    is_current TINYINT(1) DEFAULT 1,
    INDEX idx_cdsco_curr (cdsco_drug_code, is_current),
    INDEX idx_cdsco_dates (cdsco_drug_code, effective_start_date, effective_end_date)
);

-- 4. Clinical Dimension: Patient Subject (SCD Type 2)
CREATE TABLE dim_patient_scd2 (
    patient_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    abha_token_id VARCHAR(64) NOT NULL,
    geo_sk INT NOT NULL,
    age_group VARCHAR(20),
    gender VARCHAR(15),
    comorbidity_risk VARCHAR(20),
    row_hash VARCHAR(64) NOT NULL,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE DEFAULT '9999-12-31',
    is_current TINYINT(1) DEFAULT 1,
    FOREIGN KEY (geo_sk) REFERENCES dim_geography_india(geo_sk),
    INDEX idx_abha_curr (abha_token_id, is_current)
);

-- 5. Manufacturing Dimension: Plant Facility (SCD Type 2)
CREATE TABLE dim_facility_scd2 (
    facility_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    facility_code VARCHAR(25) NOT NULL,
    plant_name VARCHAR(100) NOT NULL,
    geo_sk INT NOT NULL,
    who_gmp_status VARCHAR(25),
    row_hash VARCHAR(64) NOT NULL,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE DEFAULT '9999-12-31',
    is_current TINYINT(1) DEFAULT 1,
    FOREIGN KEY (geo_sk) REFERENCES dim_geography_india(geo_sk),
    INDEX idx_fac_curr (facility_code, is_current)
);

-- 6. Commercial Dimension: Prescriber Doctor (SCD Type 2)
CREATE TABLE dim_doctor_scd2 (
    doctor_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
    nmc_reg_number VARCHAR(30) NOT NULL,
    geo_sk INT NOT NULL,
    specialty VARCHAR(50),
    prescriber_tier VARCHAR(15),
    row_hash VARCHAR(64) NOT NULL,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE DEFAULT '9999-12-31',
    is_current TINYINT(1) DEFAULT 1,
    FOREIGN KEY (geo_sk) REFERENCES dim_geography_india(geo_sk),
    INDEX idx_nmc_curr (nmc_reg_number, is_current)
);

-- FACT 1: Clinical Trial Observations
CREATE TABLE fact_clinical_observations (
    obs_id VARCHAR(40) PRIMARY KEY,
    date_sk INT NOT NULL,
    product_sk BIGINT NOT NULL,
    patient_sk BIGINT NOT NULL,
    geo_sk INT NOT NULL,
    ctri_trial_id VARCHAR(35) NOT NULL,
    trial_phase VARCHAR(15),
    biomarker_level DECIMAL(10, 3),
    adverse_event_flag TINYINT(1),
    FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product_scd2(product_sk),
    FOREIGN KEY (patient_sk) REFERENCES dim_patient_scd2(patient_sk),
    FOREIGN KEY (geo_sk) REFERENCES dim_geography_india(geo_sk)
);

-- FACT 2: Manufacturing Batches
CREATE TABLE fact_mfg_batches (
    batch_lot_id VARCHAR(40) PRIMARY KEY,
    date_sk INT NOT NULL,
    product_sk BIGINT NOT NULL,
    facility_sk BIGINT NOT NULL,
    geo_sk INT NOT NULL,
    input_kg DECIMAL(12, 2),
    output_kg DECIMAL(12, 2),
    yield_pct DECIMAL(5, 2),
    qc_passed TINYINT(1),
    mfg_cost_inr DECIMAL(15, 2),
    FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product_scd2(product_sk),
    FOREIGN KEY (facility_sk) REFERENCES dim_facility_scd2(facility_sk),
    FOREIGN KEY (geo_sk) REFERENCES dim_geography_india(geo_sk)
);

-- FACT 3: Commercial Sales India
CREATE TABLE fact_commercial_sales_india (
    invoice_id VARCHAR(45) PRIMARY KEY,
    date_sk INT NOT NULL,
    product_sk BIGINT NOT NULL,
    doctor_sk BIGINT NOT NULL,
    geo_sk INT NOT NULL,
    promo_channel VARCHAR(30),
    strips_sold INT,
    gross_sales_inr DECIMAL(15, 2),
    gst_amount_inr DECIMAL(14, 2),
    marketing_spend_inr DECIMAL(14, 2),
    net_revenue_inr DECIMAL(15, 2),
    FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product_scd2(product_sk),
    FOREIGN KEY (doctor_sk) REFERENCES dim_doctor_scd2(doctor_sk),
    FOREIGN KEY (geo_sk) REFERENCES dim_geography_india(geo_sk)
);

-- Observability & Executive KPI Mart
CREATE TABLE etl_pipeline_audit (
    run_id VARCHAR(50) PRIMARY KEY,
    job_name VARCHAR(60),
    domain VARCHAR(30),
    records_read BIGINT,
    records_valid BIGINT,
    records_quarantined BIGINT,
    status VARCHAR(20),
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mart_cadway_india_kpis (
    indian_fy VARCHAR(12),
    kpi_month INT,
    zone VARCHAR(20),
    brand_name VARCHAR(100),
    therapeutic_area VARCHAR(50),
    active_trial_patients BIGINT,
    adverse_event_rate_pct DECIMAL(5, 2),
    avg_batch_yield_pct DECIMAL(5, 2),
    qc_pass_rate_pct DECIMAL(5, 2),
    total_strips_sold BIGINT,
    net_revenue_lakhs_inr DECIMAL(14, 2),
    marketing_roi DECIMAL(8, 2),
    PRIMARY KEY (kpi_month, zone, brand_name)
);
