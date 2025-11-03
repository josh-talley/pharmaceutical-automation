"""
Database models for pharmaceutical compliance reporting application.

This module defines SQLAlchemy ORM models for managing controlled substance
transactions, product catalogs, warehouse information, and regulatory compliance data.
Each model represents a table in the database with proper relationships and constraints.

The schema is designed for pharmaceutical compliance reporting systems that must
track controlled substance transactions and generate regulatory reports (ARCOS, DSCSA,
state-level reporting).

Tables:
- TransactionData: Sales/distribution transaction records
- ControlledSubstanceMaster: Product catalog with regulatory flags
- CustomerLicenseData: Customer license information for compliance
- NdcMmeData: MME (Morphine Milligram Equivalent) conversion factors
- WarehouseData: Distribution center information and DEA numbers
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, DateTime
from sqlalchemy.orm import relationship, declarative_base

logger = logging.getLogger(__name__)

Base = declarative_base()


class TransactionData(Base):
    """
    Represents pharmaceutical transaction records including product details,
    shipping information, and customer data.

    This model is central to regulatory report generation, storing individual
    distribution transactions with full audit trail information.

    Relationships:
    - `controlled_substance`: Links to ControlledSubstanceMaster for product details
    - `warehouse`: Links to WarehouseData for distributor information

    Foreign Key Handling:
    - Foreign key validation is handled manually in the validation layer due to
      SQLite limitations with constraint enforcement
    """

    __tablename__ = "transaction_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String)
    reporting_freq = Column(Float)
    reporting_registrant_num = Column(
        String, ForeignKey("warehouse_data.dea_number"), nullable=False, index=True
    )
    transaction_code = Column(String, nullable=False)
    action_id = Column(String)
    unit = Column(Float)
    correction_num = Column(Float)
    strength = Column(String)
    transaction_date = Column(Date, index=True, nullable=False)
    ship_to_customer = Column(String, index=True, nullable=False)
    ship_to_name = Column(String, nullable=False)
    address = Column(String, nullable=False)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    zip_code = Column(String, nullable=False)
    dea_reg_nbr = Column(String, nullable=False)
    xfer_means = Column(String)
    material_document = Column(String)
    material_doc_year = Column(String)
    delivery_num = Column(String)
    delivery_item_num = Column(String)
    sales_order_num = Column(String)
    sales_order_item = Column(String)
    material = Column(String)
    material_description = Column(String, nullable=False)
    formula = Column(String)
    formula_description = Column(String)
    quantity = Column(Float, nullable=False)
    ndc_num = Column(
        String,
        ForeignKey("controlled_substance_master.ndc_no_dashes"),
        nullable=False,
        index=True,
    )
    controlled_substance_class = Column(String)
    dea_order_form_num = Column(String)
    customer_group_description = Column(String)
    char_date = Column(String)
    char_qty = Column(String)
    pkg_typ = Column(String)
    packaging_option = Column(String)
    grams_per_pkg = Column(Float)
    base_of_basic_class = Column(Float)
    rmqty_grams = Column(Float)
    base_qty_grams = Column(Float)
    sold_to_customer = Column(String)
    sold_to_name = Column(String)
    sold_to_address = Column(String)
    sold_to_city = Column(String)
    sold_to_state = Column(String)
    sold_to_zip_code = Column(String)
    sold_to_ctry = Column(String)
    reg_name = Column(String)
    reg_address = Column(String)
    reg_city = Column(String)
    reg_state = Column(String)
    reg_zip_code = Column(String)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    controlled_substance = relationship("ControlledSubstanceMaster", back_populates="transactions")
    warehouse = relationship("WarehouseData", back_populates="transactions")


class ControlledSubstanceMaster(Base):
    """
    Master product catalog for controlled substances.

    Contains classification, strength, pricing, and regulatory reporting
    requirements for each controlled substance product. This serves as the
    source of truth for product data and determines which reports each
    product should be included in.

    Key Features:
    - Boolean flags (Y/N) control inclusion in specific report types
    - Indexed columns for efficient querying during report generation
    - MME conversion factor for opioid-related reporting

    Relationships:
    - `transactions`: Links to TransactionData records

    Note:
    - Foreign key validation is handled manually in the validation layer
    """

    __tablename__ = "controlled_substance_master"

    ndc = Column(String)
    ndc_no_dashes = Column(String, primary_key=True, nullable=False, index=True)
    material_num = Column(String)
    five_digit_product = Column(String)
    label_description = Column(String)
    include_in_arcos_reports = Column(String, index=True, nullable=False)
    include_in_dscsa_reports = Column(String, index=True, nullable=False)
    include_in_mi_state_reports = Column(String, index=True, nullable=False)
    include_in_ny_state_and_excise_tax_reports = Column(
        String, index=True, nullable=False
    )
    cs_strength_mg = Column(Float, index=True, nullable=False)
    rx_otc = Column(String)
    size = Column(Float, index=True, nullable=False)
    unit = Column(String, index=True, nullable=False)
    form = Column(String)
    strength = Column(String, index=True, nullable=False)
    wac = Column(Float, index=True, nullable=False)
    items_ea_per_case_min_order_qty = Column(Float, nullable=False)
    items_per_inner_pack = Column(Float)
    mme_conv_factor = Column(Float)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships for easier querying
    transactions = relationship("TransactionData", back_populates="controlled_substance")


class CustomerLicenseData(Base):
    """
    Customer license records for regulatory compliance.

    Stores customer license information including license numbers, validity
    dates, and license types. This data is critical for state-level reporting
    and compliance verification, ensuring all transactions involve properly
    licensed customers.

    Key Features:
    - Indexed customer and license number columns for fast lookups
    - Valid_from/valid_to dates enable time-based license validation
    - Audit trail with created_by, changed_by timestamps
    """

    __tablename__ = "customer_license_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client = Column(String)
    sales_organization = Column(String)
    distribution_channel = Column(String)
    division = Column(String)
    customer = Column(String, index=True, nullable=False)
    license_type = Column(String)
    license_number = Column(String, index=True, nullable=False)
    valid_from = Column(Date, index=True, nullable=False)
    valid_to = Column(Date, index=True, nullable=False)
    date_time_stamp = Column(String)
    comments = Column(String)
    created_by = Column(String)
    created_on = Column(Date)
    changed_by = Column(String)
    changed_on = Column(String)
    print_invoice_id = Column(String)
    print_packslip_id = Column(String)
    retail_customer = Column(String)
    product_type = Column(String)
    region = Column(String)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class NdcMmeData(Base):
    """
    MME (Morphine Milligram Equivalent) conversion factors by NDC.

    Stores standardized conversion factors for calculating morphine milligram
    equivalents, which are required for opioid-related state reporting. Each
    NDC has a specific strength and conversion factor used in regulatory
    calculations.

    Key Features:
    - Nine-digit NDC as primary key (standard format without dashes)
    - Indexed strength and conversion factor columns
    - Essential for New York State and Excise Tax reporting

    Columns:
    - nine_digit_ndc: Standard 9-digit NDC identifier (primary key)
    - strength_per_unit: Milligrams of active ingredient per dosage unit
    - mme_conversion_factor: Multiplier for MME calculations
    """

    __tablename__ = "ndc_mme_data"

    nine_digit_ndc = Column(String, primary_key=True, index=True, nullable=False)
    strength_per_unit = Column(Float, index=True)
    mme_conversion_factor = Column(Float, index=True, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class WarehouseData(Base):
    """
    Distribution center and facility information.

    Contains DEA numbers, license numbers, tax identification numbers (TINs),
    and address information for pharmaceutical distribution facilities. This
    data is critical for report generation, particularly for grouping
    transactions by facility.

    Key Features:
    - DEA number as primary key (unique facility identifier)
    - TIN for tax-related reporting (State-1 Excise Tax)
    - Corporate information for regulatory submissions
    - Multiple license numbers (state-specific requirements)

    Relationships:
    - `transactions`: Links to TransactionData records originating from this facility
    """

    __tablename__ = "warehouse_data"

    corporate_name = Column(String, nullable=False)
    tin_number = Column(String, nullable=False)
    dea_number = Column(String, primary_key=True, index=True, nullable=False)
    ny_cs_license_number = Column(String, nullable=False)
    ny_sed_license_number = Column(String, nullable=False)
    address = Column(String, nullable=False)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    zip = Column(String, nullable=False)
    business_activity = Column(String, nullable=False)
    corporate_address = Column(String, nullable=False)
    corporate_city = Column(String, nullable=False)
    corporate_state = Column(String, nullable=False)
    corporate_zip = Column(String, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    transactions = relationship("TransactionData", back_populates="warehouse")
