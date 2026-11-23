USE cadway_india_dw;

-- 1. Seed All 28 Indian States & 8 Union Territories
INSERT INTO dim_geography_india (state_code, state_name, zone, city_tier) VALUES
('AP', 'Andhra Pradesh', 'South', 'Tier-2'),
('AR', 'Arunachal Pradesh', 'North-East', 'Tier-3'),
('AS', 'Assam', 'North-East', 'Tier-2'),
('BR', 'Bihar', 'East', 'Tier-2'),
('CG', 'Chhattisgarh', 'Central', 'Tier-2'),
('GA', 'Goa', 'West', 'Tier-2'),
('GJ', 'Gujarat', 'West', 'Tier-1'),
('HR', 'Haryana', 'North', 'Tier-1'),
('HP', 'Himachal Pradesh', 'North', 'Tier-2'),
('JH', 'Jharkhand', 'East', 'Tier-2'),
('KA', 'Karnataka', 'South', 'Tier-1'),
('KL', 'Kerala', 'South', 'Tier-2'),
('MP', 'Madhya Pradesh', 'Central', 'Tier-2'),
('MH', 'Maharashtra', 'West', 'Tier-1'),
('MN', 'Manipur', 'North-East', 'Tier-3'),
('ML', 'Meghalaya', 'North-East', 'Tier-3'),
('MZ', 'Mizoram', 'North-East', 'Tier-3'),
('NL', 'Nagaland', 'North-East', 'Tier-3'),
('OD', 'Odisha', 'East', 'Tier-2'),
('PB', 'Punjab', 'North', 'Tier-2'),
('RJ', 'Rajasthan', 'North', 'Tier-2'),
('SK', 'Sikkim', 'North-East', 'Tier-3'),
('TN', 'Tamil Nadu', 'South', 'Tier-1'),
('TG', 'Telangana', 'South', 'Tier-1'),
('TR', 'Tripura', 'North-East', 'Tier-3'),
('UP', 'Uttar Pradesh', 'North', 'Tier-2'),
('UK', 'Uttarakhand', 'North', 'Tier-2'),
('WB', 'West Bengal', 'East', 'Tier-1'),
-- 8 Union Territories
('AN', 'Andaman and Nicobar Islands', 'South', 'Tier-3'),
('CH', 'Chandigarh', 'North', 'Tier-2'),
('DN', 'Dadra and Nagar Haveli and Daman and Diu', 'West', 'Tier-3'),
('DL', 'Delhi', 'North', 'Tier-1'),
('JK', 'Jammu and Kashmir', 'North', 'Tier-2'),
('LA', 'Ladakh', 'North', 'Tier-3'),
('LD', 'Lakshadweep', 'South', 'Tier-3'),
('PY', 'Puducherry', 'South', 'Tier-2');

-- 2. Seed Indian Fiscal Year Date Dimension (FY2025-26 to FY2027-28)
SET SESSION cte_max_recursion_depth = 2000;
INSERT INTO dim_date (date_sk, full_date, calendar_year, calendar_month, month_name, indian_fy, indian_fy_quarter, is_weekend)
WITH RECURSIVE cal AS (
    SELECT DATE('2025-01-01') AS dt
    UNION ALL
    SELECT DATE_ADD(dt, INTERVAL 1 DAY) FROM cal WHERE dt < '2027-12-31'
)
SELECT
    CAST(DATE_FORMAT(dt, '%Y%m%d') AS UNSIGNED) AS date_sk,
    dt AS full_date,
    YEAR(dt) AS calendar_year,
    MONTH(dt) AS calendar_month,
    MONTHNAME(dt) AS month_name,
    CASE
        WHEN MONTH(dt) >= 4 THEN CONCAT('FY', YEAR(dt), '-', RIGHT(YEAR(dt) + 1, 2))
        ELSE CONCAT('FY', YEAR(dt) - 1, '-', RIGHT(YEAR(dt), 2))
    END AS indian_fy,
    CASE
        WHEN MONTH(dt) BETWEEN 4 AND 6 THEN 'Q1'
        WHEN MONTH(dt) BETWEEN 7 AND 9 THEN 'Q2'
        WHEN MONTH(dt) BETWEEN 10 AND 12 THEN 'Q3'
        ELSE 'Q4'
    END AS indian_fy_quarter,
    CASE WHEN DAYOFWEEK(dt) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
FROM cal;

-- 3. Seed Sample CDSCO Approved Drug Products (Zedlay Molecules marketed by Cadway India)
INSERT INTO dim_product_scd2 (
    cdsco_drug_code, molecule_name, brand_name, therapeutic_area,
    schedule_category, mrp_inr, unit_mfg_cost_inr, row_hash, effective_start_date
) VALUES
('CDSCO-ONC-901', 'Zedplatin-40mg', 'CadwayOnco-40', 'Oncology', 'Schedule H1', 1450.00, 320.00, SHA2('CDSCO-ONC-901||1450.00||320.00', 256), '2025-04-01'),
('CDSCO-DIA-402', 'Zedagliptin-50mg', 'CadwayGlip-M', 'Diabetology', 'Schedule H', 285.00, 58.00, SHA2('CDSCO-DIA-402||285.00||58.00', 256), '2025-04-01'),
('CDSCO-CAR-705', 'Zedvastin-20mg', 'CadwayStatin-20', 'Cardiology', 'Schedule H', 340.00, 72.00, SHA2('CDSCO-CAR-705||340.00||72.00', 256), '2025-04-01'),
('CDSCO-RES-118', 'Zedlukast-10mg', 'CadwayAir-L', 'Respiratory', 'Schedule H', 195.00, 41.00, SHA2('CDSCO-RES-118||195.00||41.00', 256), '2025-04-01');
