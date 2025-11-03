-- Sample data for pharmaceutical compliance demonstration
-- This SQL file provides mock data for testing the validation workflow

-- Note: This is for reference only. The validation_example.py script
-- creates its own in-memory database. This file shows the structure
-- and sample data used in production-style testing.

-- ==================== Warehouse Data ====================
-- Distribution centers with DEA registration numbers

INSERT INTO warehouse_data (
    dea_number,
    warehouse_name,
    corporate_name,
    tin_number,
    ny_cs_license_number,
    ny_sed_license_number,
    address,
    city,
    state,
    zip,
    business_activity,
    corporate_address,
    corporate_city,
    corporate_state,
    corporate_zip
) VALUES
    ('RW1234567', 'Main Distribution Center', 'Sample Pharma Corp', '123456789',
     '1234567', '7654321', '123 Pharma Way', 'Sample City', 'ST1', '10001',
     'Wholesale Distribution', '123 Corporate Blvd', 'Sample City', 'ST1', '10001'),
    ('RW7654321', 'Secondary Distribution Center', 'Sample Pharma Corp', '123456789',
     '1234568', '7654322', '456 Distribution St', 'Sample City 2', 'ST1', '12207',
     'Wholesale Distribution', '123 Corporate Blvd', 'Sample City', 'ST1', '10001');

-- ==================== Controlled Substance Master Data ====================
-- Product catalog with regulatory flags

INSERT INTO controlled_substance_master (
    ndc,
    ndc_no_dashes,
    material_num,
    five_digit_product,
    label_description,
    include_in_arcos_reports,
    include_in_dscsa_reports,
    include_in_mi_state_reports,
    include_in_ny_state_and_excise_tax_reports,
    cs_strength_mg,
    rx_otc,
    size,
    unit,
    form,
    strength,
    wac,
    items_ea_per_case_min_order_qty,
    items_per_inner_pack,
    mme_conv_factor
) VALUES
    ('12345-678-90', '12345678901', 'MAT001', '12345', 'Sample Oxycodone 5mg Tablet',
     'Y', 'Y', 'Y', 'Y', 5.0, 'RX', 100.0, 'Tablet', 'Tablet', '5mg',
     125.50, 100.0, 10.0, 1.5),
    ('10987-654-32', '10987654321', 'MAT002', '10987', 'Sample Hydrocodone 10mg Tablet',
     'Y', 'Y', 'N', 'Y', 10.0, 'RX', 100.0, 'Tablet', 'Tablet', '10mg',
     200.75, 100.0, 10.0, 1.0),
    ('55555-555-55', '55555555555', 'MAT003', '55555', 'Sample Morphine 15mg Tablet',
     'Y', 'N', 'N', 'Y', 15.0, 'RX', 50.0, 'Tablet', 'Tablet', '15mg',
     300.00, 50.0, 5.0, 1.0);

-- ==================== Transaction Data ====================
-- Sample controlled substance distribution transactions

INSERT INTO transaction_data (
    transaction_id,
    reporting_freq,
    reporting_registrant_num,
    transaction_code,
    action_id,
    unit,
    correction_num,
    strength,
    transaction_date,
    ship_to_customer,
    ship_to_name,
    address,
    city,
    state,
    zip_code,
    dea_reg_nbr,
    xfer_means,
    material_document,
    material_doc_year,
    delivery_num,
    delivery_item_num,
    sales_order_num,
    sales_order_item,
    material,
    material_description,
    formula,
    formula_description,
    quantity,
    ndc_num,
    controlled_substance_class,
    dea_order_form_num,
    customer_group_description,
    char_date,
    char_qty,
    pkg_typ,
    packaging_option,
    grams_per_pkg,
    base_of_basic_class,
    rmqty_grams,
    base_qty_grams,
    sold_to_customer,
    sold_to_name,
    sold_to_address,
    sold_to_city,
    sold_to_state,
    sold_to_zip_code,
    sold_to_ctry,
    reg_name,
    reg_address,
    reg_city,
    reg_state,
    reg_zip_code
) VALUES
    -- Transaction 1: Oxycodone shipment to State-1 pharmacy
    ('TXN001', 1.0, 'RW1234567', 'SALE', 'ACT001', 100.0, 0.0, '5mg',
     '2025-01-15', 'CUST001', 'Sample Pharmacy 1', '789 Pharmacy Ave',
     'Sample City', 'ST1', '10002', 'BC1234567', 'Ground', 'MD001', '2025',
     'DLV001', '001', 'SO001', '001', 'MAT001', 'Sample Oxycodone 5mg Tablet',
     'FORM001', 'Controlled Substance Form', 1000.0, '12345678901', 'C-II',
     'DEA222001', 'Retail Pharmacy', '20250115', '1000', 'BTL', 'Bottle',
     0.5, 2.0, 500.0, 500.0, 'CUST001', 'Sample Pharmacy 1', '789 Pharmacy Ave',
     'Sample City', 'ST1', '10002', 'USA', 'Sample Pharmacy 1', '789 Pharmacy Ave',
     'Sample City', 'ST1', '10002'),

    -- Transaction 2: Hydrocodone shipment to State-2 pharmacy
    ('TXN002', 1.0, 'RW7654321', 'SALE', 'ACT002', 100.0, 0.0, '10mg',
     '2025-01-16', 'CUST002', 'Sample Pharmacy 2', '321 Medical St',
     'Sample City 3', 'ST2', '48201', 'BC7654321', 'Ground', 'MD002', '2025',
     'DLV002', '001', 'SO002', '001', 'MAT002', 'Sample Hydrocodone 10mg Tablet',
     'FORM002', 'Controlled Substance Form', 500.0, '10987654321', 'C-II',
     'DEA222002', 'Retail Pharmacy', '20250116', '500', 'BTL', 'Bottle',
     1.0, 2.0, 500.0, 500.0, 'CUST002', 'Sample Pharmacy 2', '321 Medical St',
     'Sample City 3', 'ST2', '48201', 'USA', 'Sample Pharmacy 2', '321 Medical St',
     'Sample City 3', 'ST2', '48201'),

    -- Transaction 3: Morphine shipment to State-1 hospital
    ('TXN003', 1.0, 'RW1234567', 'SALE', 'ACT003', 50.0, 0.0, '15mg',
     '2025-01-17', 'CUST003', 'Sample Hospital 1', '555 Hospital Dr',
     'Sample City', 'ST1', '10003', 'BH1234567', 'Ground', 'MD003', '2025',
     'DLV003', '001', 'SO003', '001', 'MAT003', 'Sample Morphine 15mg Tablet',
     'FORM003', 'Controlled Substance Form', 250.0, '55555555555', 'C-II',
     'DEA222003', 'Hospital', '20250117', '250', 'BTL', 'Bottle',
     0.75, 2.0, 187.5, 187.5, 'CUST003', 'Sample Hospital 1', '555 Hospital Dr',
     'Sample City', 'ST1', '10003', 'USA', 'Sample Hospital 1', '555 Hospital Dr',
     'Sample City', 'ST1', '10003');

-- ==================== Customer License Data ====================
-- Customer controlled substance licenses (for State-1 reporting)

INSERT INTO customer_license_data (
    client,
    sales_organization,
    distribution_channel,
    division,
    customer,
    license_type,
    license_number,
    valid_from,
    valid_to,
    date_time_stamp,
    comments,
    created_by,
    created_on,
    changed_by,
    changed_on,
    print_invoice_id,
    print_packslip_id,
    retail_customer,
    product_type,
    region
) VALUES
    ('CLIENT001', 'ORG001', 'DIST001', 'DIV001', 'CUST001', 'CS',
     '1234567', '2024-01-01', '2025-12-31', '2024-01-01 10:00:00',
     'Valid State-1 CS License', 'ADMIN', '2024-01-01', 'ADMIN', '2024-01-01',
     'Y', 'Y', 'Y', 'Controlled Substances', 'Northeast'),
    ('CLIENT001', 'ORG001', 'DIST001', 'DIV001', 'CUST003', 'CS',
     '7654321', '2024-06-01', '2026-05-31', '2024-06-01 10:00:00',
     'Valid State-1 CS License', 'ADMIN', '2024-06-01', 'ADMIN', '2024-06-01',
     'Y', 'Y', 'N', 'Controlled Substances', 'Northeast');

-- ==================== NDC MME Data ====================
-- Morphine Milligram Equivalent conversion factors

INSERT INTO ndc_mme_data (
    nine_digit_ndc,
    strength_per_unit,
    mme_conversion_factor
) VALUES
    ('123456789', 5.0, 1.5),   -- Oxycodone 5mg: 1.5x morphine equivalent
    ('109876543', 10.0, 1.0),  -- Hydrocodone 10mg: 1.0x morphine equivalent
    ('555555555', 15.0, 1.0);  -- Morphine 15mg: 1.0x (baseline)

-- ==================== Notes ====================
-- This sample data demonstrates:
--
-- 1. Multiple warehouses with same TIN (tax grouping for State-1 Excise)
-- 2. Products with different regulatory flags (include_in_*_reports)
-- 3. Transactions across multiple states (ST1, ST2)
-- 4. Valid customer licenses covering transaction dates
-- 5. MME conversion factors for opioid reporting
--
-- Production system processes 20,000+ transactions per quarter
-- with this same structure and validation workflow.
