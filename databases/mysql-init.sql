-- ============================================================================
-- HealthStream v2 — MySQL Init (Source C: Pharmacy System)
-- ============================================================================
-- Creates patient_medications table and seeds with 30 patients.
-- MySQL binlog (enabled in docker-compose) captures all changes for Debezium.
-- NOTE: Debezium reads the binlog, NOT the table directly.
--       So even this initial INSERT is captured as CDC events.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS healthstream_pharmacy;
USE healthstream_pharmacy;

CREATE TABLE IF NOT EXISTS patient_medications (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    patient_id      VARCHAR(20) NOT NULL,
    medication_name VARCHAR(100) NOT NULL,
    dosage          VARCHAR(50) NOT NULL,
    frequency       VARCHAR(50) NOT NULL,
    route           VARCHAR(30) NOT NULL DEFAULT 'oral',       -- oral/IV/subcutaneous/inhaled
    prescriber      VARCHAR(100),
    start_date      DATE NOT NULL,
    end_date        DATE,                                       -- NULL = ongoing
    status          VARCHAR(20) NOT NULL DEFAULT 'active',      -- active/discontinued/completed
    notes           TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_med_patient (patient_id),
    INDEX idx_med_updated (updated_at)
) ENGINE=InnoDB;

-- ============================================================================
-- GRANT Debezium permissions
-- Debezium needs: SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
-- These let it read the binlog as if it were a MySQL replica
-- ============================================================================
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium_secure_2024';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- ============================================================================
-- Seed data: medications for 30 patients (matching P001-P030)
-- ============================================================================
INSERT INTO patient_medications (patient_id, medication_name, dosage, frequency, route, prescriber, start_date, status, notes) VALUES
-- P001: Diabetic with hypertension
('P001', 'Metformin',        '1000mg',  'twice daily',   'oral',         'Dr. Smith',    '2018-03-20', 'active', 'For diabetes management'),
('P001', 'Lisinopril',       '10mg',    'once daily',    'oral',         'Dr. Smith',    '2016-08-01', 'active', 'For blood pressure'),
('P001', 'Atorvastatin',     '20mg',    'once daily',    'oral',         'Dr. Smith',    '2019-01-15', 'active', 'Cholesterol management'),

-- P002: COPD patient
('P002', 'Tiotropium',       '18mcg',   'once daily',    'inhaled',      'Dr. Johnson',  '2019-01-15', 'active', 'COPD maintenance'),
('P002', 'Albuterol',        '90mcg',   'as needed',     'inhaled',      'Dr. Johnson',  '2019-01-15', 'active', 'Rescue inhaler'),
('P002', 'Prednisone',       '40mg',    'once daily',    'oral',         'Dr. Johnson',  '2024-01-02', 'active', 'COPD exacerbation, taper over 10 days'),

-- P003: Heart failure + AFib
('P003', 'Metoprolol',       '50mg',    'twice daily',   'oral',         'Dr. Williams', '2020-11-10', 'active', 'Heart rate control'),
('P003', 'Furosemide',       '40mg',    'once daily',    'oral',         'Dr. Williams', '2020-11-10', 'active', 'Fluid management'),
('P003', 'Lisinopril',       '20mg',    'once daily',    'oral',         'Dr. Williams', '2020-11-10', 'active', 'Heart failure'),
('P003', 'Warfarin',         '5mg',     'once daily',    'oral',         'Dr. Williams', '2020-11-10', 'active', 'Anticoagulation for AFib'),

-- P004: Diabetic with CKD
('P004', 'Insulin Glargine',  '30 units', 'once daily',  'subcutaneous', 'Dr. Brown',    '2018-01-10', 'active', 'Basal insulin'),
('P004', 'Insulin Lispro',    '10 units', 'with meals',  'subcutaneous', 'Dr. Brown',    '2018-01-10', 'active', 'Rapid-acting insulin'),
('P004', 'Losartan',          '100mg',   'once daily',   'oral',         'Dr. Brown',    '2021-03-01', 'active', 'Renal protection'),

-- P005: Post-stroke
('P005', 'Aspirin',           '81mg',    'once daily',   'oral',         'Dr. Davis',    '2023-08-25', 'active', 'Stroke prevention'),
('P005', 'Clopidogrel',       '75mg',    'once daily',   'oral',         'Dr. Davis',    '2023-08-25', 'active', 'Dual antiplatelet'),
('P005', 'Amlodipine',        '5mg',     'once daily',   'oral',         'Dr. Davis',    '2023-08-25', 'active', 'BP control post-stroke'),
('P005', 'Metformin',         '500mg',   'twice daily',  'oral',         'Dr. Davis',    '2019-11-15', 'active', 'Diabetes'),

-- P006: Pneumonia
('P006', 'Azithromycin',      '500mg',   'once daily',   'oral',         'Dr. Wilson',   '2024-01-05', 'active', 'Antibiotic for pneumonia'),
('P006', 'Acetaminophen',     '650mg',   'every 6 hours','oral',         'Dr. Wilson',   '2024-01-05', 'active', 'Fever management'),

-- P007: Anxiety (no heavy meds)
('P007', 'Melatonin',         '3mg',     'at bedtime',   'oral',         'Dr. Taylor',   '2022-05-01', 'active', 'Sleep aid'),

-- P008: Multiple conditions
('P008', 'Insulin Glargine',  '40 units', 'once daily',  'subcutaneous', 'Dr. Anderson', '2018-06-01', 'active', 'Basal insulin'),
('P008', 'Carvedilol',        '25mg',    'twice daily',  'oral',         'Dr. Anderson', '2022-09-15', 'active', 'Heart failure'),
('P008', 'Furosemide',        '20mg',    'once daily',   'oral',         'Dr. Anderson', '2022-09-15', 'active', 'Fluid retention'),
('P008', 'Acetaminophen',     '500mg',   'as needed',    'oral',         'Dr. Anderson', '2015-06-15', 'active', 'Arthritis pain'),

-- P009: Asthma
('P009', 'Fluticasone',       '250mcg',  'twice daily',  'inhaled',      'Dr. Thomas',   '2015-09-10', 'active', 'Maintenance inhaler'),
('P009', 'Albuterol',         '90mcg',   'as needed',    'inhaled',      'Dr. Thomas',   '2015-09-10', 'active', 'Rescue inhaler'),

-- P010: Cancer + anemia
('P010', 'Ondansetron',       '8mg',     'as needed',    'oral',         'Dr. Jackson',  '2023-12-01', 'active', 'Anti-nausea for chemo'),
('P010', 'Ferrous Sulfate',   '325mg',   'once daily',   'oral',         'Dr. Jackson',  '2023-12-05', 'active', 'Iron for anemia'),

-- P011-P020: Various medications
('P011', 'Hydrochlorothiazide','25mg',   'once daily',   'oral',         'Dr. White',    '2020-03-15', 'active', 'Mild hypertension'),
('P012', 'Metformin',         '1000mg',  'twice daily',  'oral',         'Dr. Harris',   '2017-09-01', 'active', 'Diabetes'),
('P012', 'Losartan',          '50mg',    'once daily',   'oral',         'Dr. Harris',   '2017-09-01', 'active', 'Hypertension'),
('P013', 'Fluticasone/Salmeterol','250/50mcg','twice daily','inhaled',   'Dr. Martin',   '2021-06-01', 'active', 'COPD maintenance'),
('P014', 'Montelukast',       '10mg',    'once daily',   'oral',         'Dr. Garcia',   '2008-12-10', 'active', 'Asthma prevention'),
('P015', 'Sacubitril/Valsartan','97/103mg','twice daily', 'oral',        'Dr. Martinez', '2023-07-01', 'active', 'Heart failure (Entresto)'),
('P015', 'Insulin Pump',      'variable','continuous',   'subcutaneous', 'Dr. Martinez', '2020-01-01', 'active', 'Type 2 on pump'),
('P016', 'Apixaban',          '5mg',     'twice daily',  'oral',         'Dr. Robinson', '2022-01-15', 'active', 'AFib anticoagulation'),
('P017', 'Ibuprofen',         '400mg',   'as needed',    'oral',         'Dr. Clark',    '2019-05-01', 'active', 'Arthritis pain'),
('P018', 'None',              'N/A',     'N/A',          'N/A',          'Dr. Rodriguez','2020-07-01', 'active', 'Diet-controlled diabetes, no meds'),
('P019', 'Amlodipine',        '10mg',    'once daily',   'oral',         'Dr. Lewis',    '2021-12-01', 'active', 'Hypertension'),
('P019', 'Lisinopril',        '40mg',    'once daily',   'oral',         'Dr. Lewis',    '2021-12-01', 'active', 'Hypertension'),
('P019', 'Chlorthalidone',    '25mg',    'once daily',   'oral',         'Dr. Lewis',    '2022-03-01', 'active', 'Resistant hypertension'),
('P020', 'Vancomycin',        '1g',      'every 12 hours','IV',          'Dr. Lee',      '2024-01-08', 'active', 'Hospital-acquired pneumonia'),

-- P021-P030
('P021', 'Glipizide',         '5mg',     'once daily',   'oral',         'Dr. Walker',   '2016-03-10', 'active', 'Diabetes'),
('P021', 'Amlodipine',        '5mg',     'once daily',   'oral',         'Dr. Walker',   '2016-03-10', 'active', 'Hypertension'),
('P022', 'Fluticasone/Salmeterol','500/50mcg','twice daily','inhaled',   'Dr. Hall',     '2011-08-01', 'active', 'Severe asthma (Advair)'),
('P023', 'Sacubitril/Valsartan','97/103mg','twice daily', 'oral',        'Dr. Allen',    '2023-03-20', 'active', 'Severe HF'),
('P023', 'Amiodarone',        '200mg',   'once daily',   'oral',         'Dr. Allen',    '2023-04-01', 'active', 'AFib rhythm control'),
('P024', 'Ferrous Sulfate',   '325mg',   'twice daily',  'oral',         'Dr. Young',    '2023-09-05', 'active', 'Iron deficiency anemia'),
('P025', 'Lisinopril',        '20mg',    'once daily',   'oral',         'Dr. King',     '2019-06-01', 'active', 'Hypertension'),
('P026', 'Insulin Pump',      'variable','continuous',   'subcutaneous', 'Dr. Wright',   '2010-05-01', 'active', 'Type 1 diabetes'),
('P027', 'Tiotropium',        '18mcg',   'once daily',   'inhaled',      'Dr. Lopez',    '2020-10-10', 'active', 'COPD'),
('P027', 'Furosemide',        '40mg',    'once daily',   'oral',         'Dr. Lopez',    '2022-06-20', 'active', 'Right-sided HF'),
('P028', 'Sertraline',        '100mg',   'once daily',   'oral',         'Dr. Hill',     '2021-02-01', 'active', 'Anxiety and depression'),
('P029', 'Acetaminophen',     '500mg',   'as needed',    'oral',         'Dr. Scott',    '2018-11-10', 'active', 'Post-surgical pain'),
('P030', 'Lifestyle only',    'N/A',     'N/A',          'N/A',          'Dr. Green',    '2022-06-05', 'active', 'No meds, lifestyle changes for mild HTN');
