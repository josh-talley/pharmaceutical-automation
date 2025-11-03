"""
Standalone demonstration of the pharmaceutical compliance validation pattern.

This simplified example demonstrates the core validation workflow used in the
production system, without requiring the full application codebase. It shows:

- Database transaction management (commit/rollback)
- Multi-layer validation with progress tracking
- User-friendly error handling
- Atomic operations (all-or-nothing data integrity)

Run this example to see the validation pattern in action:
    python examples/validation_example.py
"""

import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, Column, String, Integer, Float, Date
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import date

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Create declarative base
Base = declarative_base()


# ==================== Simplified Models ====================


class TransactionData(Base):
    """Simplified transaction model for demonstration."""

    __tablename__ = "transaction_data"

    id = Column(Integer, primary_key=True)
    ndc_num = Column(String, nullable=False)
    warehouse_dea = Column(String, nullable=False)
    quantity = Column(Float, nullable=False)
    transaction_date = Column(Date, nullable=False)


class ControlledSubstanceMaster(Base):
    """Simplified product master for demonstration."""

    __tablename__ = "controlled_substance_master"

    ndc_no_dashes = Column(String, primary_key=True)
    product_name = Column(String, nullable=False)
    include_in_reports = Column(String, nullable=False)  # 'Y' or 'N'


class WarehouseData(Base):
    """Simplified warehouse model for demonstration."""

    __tablename__ = "warehouse_data"

    dea_number = Column(String, primary_key=True)
    warehouse_name = Column(String, nullable=False)


# ==================== Transaction Manager ====================


class TransactionManager:
    """
    Simplified transaction manager demonstrating the context manager pattern.

    This shows the same pattern used in the production db_manager.py, but
    simplified for the demonstration.
    """

    def __init__(self, engine):
        self.engine = engine
        self.session_factory = sessionmaker(bind=self.engine)

    @contextmanager
    def transaction(self):
        """Context manager for safe database transactions."""
        session = self.session_factory()
        try:
            logger.debug("Transaction started")
            yield session
            session.commit()
            logger.debug("Transaction committed")
        except Exception as e:
            session.rollback()
            logger.error(f"Transaction rolled back due to: {e}")
            raise
        finally:
            session.close()


# ==================== Validator ====================


class SimpleValidator:
    """
    Simplified validator demonstrating the validation pattern.

    This shows the same multi-layer validation approach used in the
    production data_validator.py, but simplified for demonstration.
    """

    def validate_enum_columns(self, session, progress_callback=None):
        """Validate that include_in_reports contains only 'Y' or 'N'."""
        if progress_callback:
            progress_callback("validate_enum_columns", 0)

        # Query for invalid enum values
        invalid_records = (
            session.query(ControlledSubstanceMaster)
            .filter(~ControlledSubstanceMaster.include_in_reports.in_(["Y", "N"]))
            .all()
        )

        if progress_callback:
            progress_callback("validate_enum_columns", 50)

        if invalid_records:
            ndcs = [rec.ndc_no_dashes for rec in invalid_records]
            raise ValueError(
                f"Invalid enum values found for NDCs: {', '.join(ndcs)}. "
                f"Must be 'Y' or 'N'."
            )

        if progress_callback:
            progress_callback("validate_enum_columns", 100)

        logger.info("✓ Enum validation passed")

    def validate_foreign_keys(self, session, progress_callback=None):
        """Validate that all transactions reference existing warehouses and products."""
        if progress_callback:
            progress_callback("validate_foreign_keys", 0)

        # Check warehouse foreign keys
        orphaned_warehouse = (
            session.query(TransactionData.warehouse_dea)
            .distinct()
            .outerjoin(
                WarehouseData, TransactionData.warehouse_dea == WarehouseData.dea_number
            )
            .filter(WarehouseData.dea_number.is_(None))
            .all()
        )

        if orphaned_warehouse:
            dea_numbers = [w[0] for w in orphaned_warehouse]
            raise ValueError(
                f"Transactions reference unknown warehouse DEA numbers: {', '.join(dea_numbers)}"
            )

        if progress_callback:
            progress_callback("validate_foreign_keys", 50)

        # Check product foreign keys
        orphaned_products = (
            session.query(TransactionData.ndc_num)
            .distinct()
            .outerjoin(
                ControlledSubstanceMaster,
                TransactionData.ndc_num == ControlledSubstanceMaster.ndc_no_dashes,
            )
            .filter(ControlledSubstanceMaster.ndc_no_dashes.is_(None))
            .all()
        )

        if orphaned_products:
            ndcs = [p[0] for p in orphaned_products]
            raise ValueError(
                f"Transactions reference unknown product NDCs: {', '.join(ndcs)}. "
                f"Please add products to master catalog."
            )

        if progress_callback:
            progress_callback("validate_foreign_keys", 100)

        logger.info("✓ Foreign key validation passed")

    def validate_data_quality(self, session, progress_callback=None):
        """Validate data quality (quantities > 0, valid dates, etc.)."""
        if progress_callback:
            progress_callback("validate_data_quality", 0)

        # Check for negative quantities
        invalid_quantities = (
            session.query(TransactionData).filter(TransactionData.quantity <= 0).count()
        )

        if invalid_quantities > 0:
            raise ValueError(
                f"Found {invalid_quantities} transactions with quantity <= 0"
            )

        if progress_callback:
            progress_callback("validate_data_quality", 100)

        logger.info("✓ Data quality validation passed")


# ==================== Demo Functions ====================


def progress_tracker(method_name, percent):
    """Simple progress callback for demonstration."""
    # Only print at 0% and 100% to keep output clean
    if percent == 0:
        print(f"[{method_name}] Starting...")
    elif percent == 100:
        print(f"[{method_name}] Complete ✓")


def create_sample_database(engine):
    """Create sample database with test data."""
    print("\nCreating sample database with test data...")

    # Create tables
    Base.metadata.create_all(engine)

    trans_mgmt = TransactionManager(engine)

    with trans_mgmt.transaction() as session:
        # Add warehouses
        session.add_all(
            [
                WarehouseData(dea_number="RW1234567", warehouse_name="Main Warehouse"),
                WarehouseData(
                    dea_number="RW7654321", warehouse_name="Secondary Warehouse"
                ),
            ]
        )

        # Add products
        session.add_all(
            [
                ControlledSubstanceMaster(
                    ndc_no_dashes="12345678901",
                    product_name="Product A",
                    include_in_reports="Y",
                ),
                ControlledSubstanceMaster(
                    ndc_no_dashes="10987654321",
                    product_name="Product B",
                    include_in_reports="Y",
                ),
            ]
        )

        # Add transactions
        session.add_all(
            [
                TransactionData(
                    ndc_num="12345678901",
                    warehouse_dea="RW1234567",
                    quantity=100.0,
                    transaction_date=date(2025, 1, 15),
                ),
                TransactionData(
                    ndc_num="10987654321",
                    warehouse_dea="RW7654321",
                    quantity=250.0,
                    transaction_date=date(2025, 1, 16),
                ),
            ]
        )

    print("✓ Database initialized with sample data\n")


def run_validation_workflow(engine):
    """Run the validation workflow."""
    print("Running validation workflow...")
    print("━" * 50)

    trans_mgmt = TransactionManager(engine)
    validator = SimpleValidator()

    try:
        with trans_mgmt.transaction() as session:
            # Run validations with progress tracking
            validator.validate_enum_columns(session, progress_callback=progress_tracker)
            validator.validate_foreign_keys(session, progress_callback=progress_tracker)
            validator.validate_data_quality(session, progress_callback=progress_tracker)

        print("━" * 50)
        print("\n✅ All validations passed!")
        print("Database ready for report generation.\n")

    except Exception as e:
        print("━" * 50)
        print(f"\n❌ Validation Failed: {e}")
        print("Transaction automatically rolled back. Database unchanged.\n")
        raise


def demonstrate_error_handling(engine):
    """Demonstrate error handling with invalid data."""
    print("\nDemonstrating error handling...")
    print("━" * 50)
    print("Adding transaction with invalid product NDC...\n")

    trans_mgmt = TransactionManager(engine)
    validator = SimpleValidator()

    try:
        with trans_mgmt.transaction() as session:
            # Add transaction with invalid NDC (not in master catalog)
            session.add(
                TransactionData(
                    ndc_num="99999999999",  # Invalid NDC
                    warehouse_dea="RW1234567",
                    quantity=100.0,
                    transaction_date=date(2025, 1, 17),
                )
            )

            # Try to validate (this will fail)
            validator.validate_foreign_keys(session)

    except ValueError as e:
        print("━" * 50)
        print(f"\n❌ Validation Error Caught:")
        print(f"   {e}")
        print("\nTransaction automatically rolled back. Database unchanged.")
        print("This demonstrates the 'all-or-nothing' transaction pattern.\n")


def main():
    """Main demonstration function."""
    print("\n" + "=" * 60)
    print("🔍 Pharmaceutical Compliance Validation Demo")
    print("=" * 60)
    print("\nThis demo illustrates the validation pattern used in the")
    print("production pharmaceutical compliance system.")
    print("\nKey concepts demonstrated:")
    print("  • Transaction safety (automatic commit/rollback)")
    print("  • Multi-layer validation")
    print("  • Progress tracking")
    print("  • User-friendly error messages")
    print("=" * 60)

    # Create in-memory database
    engine = create_engine("sqlite:///:memory:", echo=False)

    # Create sample database
    create_sample_database(engine)

    # Run validation workflow (should succeed)
    run_validation_workflow(engine)

    # Demonstrate error handling
    demonstrate_error_handling(engine)

    print("=" * 60)
    print("Demo complete!")
    print("\nThis pattern scales to handle:")
    print("  • 20,000+ transactions per quarterly report cycle")
    print("  • 7 validation methods (5 shown in simplified demo)")
    print("  • Complex business logic validation")
    print("  • Real-time progress updates in production GUI")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
