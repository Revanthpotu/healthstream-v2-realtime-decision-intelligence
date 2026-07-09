-- ============================================================================
-- HealthStream v2 — PostgreSQL Init (Source B: EHR System)
-- ============================================================================
-- Creates patient_conditions table and seeds with 30 patients.
-- CRITICAL: updated_at column is what Kafka Connect JDBC uses to detect changes.
-- ============================================================================

CREATE TABLE IF NOT EXISTS patient_conditions (
    id              SERIAL PRIMARY KEY,
    patient_id      VARCHAR(20) NOT NULL,
    condition_code  VARCHAR(20) NOT NULL,
    condition_name  VARCHAR(100) NOT NULL,
    diagnosed_date  DATE NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'active',
    severity        VARCHAR(20) NOT NULL DEFAULT 'moderate',
    notes           TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_conditions_patient ON patient_conditions(patient_id);
CREATE INDEX idx_conditions_updated ON patient_conditions(updated_at);

-- Auto-update updated_at on any row change (needed for JDBC connector)
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_timestamp
    BEFORE UPDATE ON patient_conditions
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();

-- Seed data: 30 patients with chronic conditions
INSERT INTO patient_conditions (patient_id, condition_code, condition_name, diagnosed_date, status, severity, notes) VALUES
('P001', '73211009',  'Diabetes Mellitus Type 2',              '2018-03-15', 'active',   'moderate', 'HbA1c at 7.2%, controlled with medication'),
('P001', '38341003',  'Hypertension',                          '2016-07-22', 'active',   'mild',     'BP managed with Lisinopril'),
('P002', '13645005',  'Chronic Obstructive Pulmonary Disease',  '2019-01-10', 'active',   'severe',   'FEV1 at 45%, requires supplemental O2'),
('P002', '195967001', 'Asthma',                                '2010-05-20', 'active',   'moderate', 'History of childhood asthma'),
('P003', '84114007',  'Heart Failure',                         '2020-11-05', 'active',   'severe',   'EF 35%, NYHA Class III'),
('P003', '49436004',  'Atrial Fibrillation',                   '2020-11-05', 'active',   'moderate', 'Rate controlled with Metoprolol'),
('P003', '38341003',  'Hypertension',                          '2015-03-18', 'active',   'moderate', 'Contributing to heart failure'),
('P004', '73211009',  'Diabetes Mellitus Type 2',              '2012-06-30', 'active',   'severe',   'HbA1c at 9.1%, poorly controlled'),
('P004', '709044004', 'Chronic Kidney Disease Stage 3',        '2021-02-14', 'active',   'moderate', 'eGFR 42, monitoring quarterly'),
('P004', '38341003',  'Hypertension',                          '2012-06-30', 'active',   'severe',   'Secondary to diabetes'),
('P005', '230690007', 'Cerebrovascular Accident',              '2023-08-20', 'resolved', 'severe',   'Left-sided weakness, recovering'),
('P005', '38341003',  'Hypertension',                          '2018-01-15', 'active',   'moderate', 'Key risk factor for stroke'),
('P005', '44054006',  'Diabetes Mellitus Type 2',              '2019-11-10', 'active',   'mild',     'Well controlled, HbA1c 6.5%'),
('P006', '233604007', 'Pneumonia',                             '2024-01-05', 'active',   'severe',   'Community-acquired, on antibiotics'),
('P007', '197480006', 'Anxiety Disorder',                      '2022-04-15', 'active',   'mild',     'Managed with therapy'),
('P008', '73211009',  'Diabetes Mellitus Type 2',              '2010-01-20', 'active',   'moderate', 'On insulin since 2018'),
('P008', '84114007',  'Heart Failure',                         '2022-09-12', 'active',   'moderate', 'EF 40%, stable'),
('P008', '709044004', 'Chronic Kidney Disease Stage 3',        '2023-01-05', 'active',   'moderate', 'eGFR 38, declining'),
('P008', '396275006', 'Osteoarthritis',                        '2015-06-10', 'active',   'severe',   'Both knees, affecting mobility'),
('P009', '195967001', 'Asthma',                                '2015-09-01', 'active',   'moderate', 'Exercise-induced'),
('P010', '363406005', 'Colon Cancer Stage II',                 '2023-11-20', 'active',   'severe',   'Post-surgery, on chemotherapy'),
('P010', '271737000', 'Anemia',                                '2023-12-01', 'active',   'moderate', 'Secondary to chemotherapy'),
('P011', '38341003',  'Hypertension',                          '2020-03-10', 'active',   'mild',     'Lifestyle management'),
('P012', '73211009',  'Diabetes Mellitus Type 2',              '2017-08-22', 'active',   'moderate', 'Metformin 1000mg BID'),
('P012', '38341003',  'Hypertension',                          '2017-08-22', 'active',   'moderate', 'Losartan 50mg daily'),
('P013', '13645005',  'COPD',                                  '2021-05-15', 'active',   'moderate', 'FEV1 58%, using inhalers'),
('P014', '195967001', 'Asthma',                                '2008-12-01', 'active',   'mild',     'Seasonal triggers'),
('P015', '84114007',  'Heart Failure',                         '2023-06-20', 'active',   'moderate', 'EF 38%, on Entresto'),
('P015', '73211009',  'Diabetes Mellitus Type 2',              '2014-09-15', 'active',   'severe',   'On insulin pump'),
('P016', '49436004',  'Atrial Fibrillation',                   '2022-01-10', 'active',   'moderate', 'On Eliquis'),
('P017', '396275006', 'Osteoarthritis',                        '2019-04-20', 'active',   'moderate', 'Right hip'),
('P018', '73211009',  'Diabetes Mellitus Type 2',              '2020-07-01', 'active',   'mild',     'Diet-controlled'),
('P019', '38341003',  'Hypertension',                          '2021-11-15', 'active',   'severe',   'Resistant, 3 medications'),
('P019', '709044004', 'Chronic Kidney Disease Stage 2',        '2022-08-10', 'active',   'mild',     'eGFR 72, early stage'),
('P020', '233604007', 'Pneumonia',                             '2024-01-08', 'active',   'moderate', 'Hospital-acquired'),
('P021', '73211009',  'Diabetes Mellitus Type 2',              '2016-02-28', 'active',   'moderate', 'HbA1c 7.5%'),
('P021', '38341003',  'Hypertension',                          '2016-02-28', 'active',   'moderate', 'Amlodipine 5mg'),
('P022', '195967001', 'Asthma',                                '2011-07-10', 'active',   'severe',   'Frequent exacerbations'),
('P023', '84114007',  'Heart Failure',                         '2023-03-15', 'active',   'severe',   'EF 25%, max medical therapy'),
('P023', '49436004',  'Atrial Fibrillation',                   '2023-03-15', 'active',   'severe',   'Persistent AFib'),
('P024', '271737000', 'Anemia',                                '2023-09-01', 'active',   'mild',     'Iron deficiency'),
('P025', '38341003',  'Hypertension',                          '2019-05-20', 'active',   'moderate', 'Well controlled'),
('P026', '73211009',  'Diabetes Mellitus Type 1',              '2005-03-10', 'active',   'moderate', 'Insulin pump, CGM'),
('P027', '13645005',  'COPD',                                  '2020-10-01', 'active',   'severe',   'FEV1 40%, home oxygen'),
('P027', '84114007',  'Heart Failure',                         '2022-06-15', 'active',   'moderate', 'Right-sided, due to COPD'),
('P028', '197480006', 'Anxiety Disorder',                      '2021-01-20', 'active',   'moderate', 'On Sertraline'),
('P028', '35489007',  'Depression',                            '2021-01-20', 'active',   'moderate', 'Comorbid with anxiety'),
('P029', '396275006', 'Osteoarthritis',                        '2018-11-05', 'active',   'severe',   'Post knee replacement'),
('P030', '38341003',  'Hypertension',                          '2022-06-01', 'active',   'mild',     'Newly diagnosed');
