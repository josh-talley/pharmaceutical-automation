"""
Custom exception hierarchy for pharmaceutical compliance application.

This module defines a comprehensive exception hierarchy for handling errors
throughout the data pipeline: data loading, validation, database operations,
and report generation.

Key Design Features:
- Base ApplicationError class with user-friendly message support
- Separate exception hierarchies for each layer (data loading, validation, reporting)
- User-facing messages separate from technical error details
- Friendly table name mapping for non-technical users

Exception Hierarchy:
- ApplicationError (base)
  ├── DataLoaderError (Excel/file operations)
  ├── ExcelUtilitiesError (header validation)
  ├── DataValidationError (database validation)
  ├── ReportGenUtilsError (report utilities)
  └── Various report-specific errors
"""

import logging

logger = logging.getLogger(__name__)

USER_TABLE_NAMES = {
    "transaction_data": "Transaction Data",
    "customer_license_data": "Customer License Data",
    "controlled_substance_master": "Controlled Substance Master",
    "ndc_mme_data": "NDC MME Data",
    "warehouse_data": "Warehouse Data",
}


class ApplicationError(Exception):
    """
    Base exception class for the application.

    All application-level exceptions inherit from this class to enable
    consistent error handling and user-friendly message generation.

    Args:
        message (str): Technical error message for logging
        user_message (str, optional): User-friendly message for display in GUI
        table_name (str, optional): Database table name (will be converted to friendly name)

    Attributes:
        user_message (str): Message suitable for display to end users
    """

    def __init__(self, message, user_message=None, table_name=None):
        super().__init__(message)
        # Use user-friendly table name if available
        friendly_name = USER_TABLE_NAMES.get(table_name, table_name)
        # If user_message is not provided, fallback to the default message
        self.user_message = user_message or message
        if friendly_name and self.user_message:
            # Replace technical table_name with friendly_name in the user_message
            self.user_message = self.user_message.replace(table_name, friendly_name)


# ===================== Data Loading Exceptions ========================


class DataLoaderError(ApplicationError):
    """Base exception class for data loading operations from Excel files."""

    def __init__(
        self,
        message="An error occurred while loading data.",
        user_message=None,
        table_name=None,
    ):
        super().__init__(message, user_message=user_message, table_name=table_name)


class SourceFileOpenOrLocked(DataLoaderError):
    """Raised when the source Excel file is open or locked."""

    def __init__(self, message, table_name=None, file_name=None):
        user_message = (
            f"Excel File {file_name} is open or locked in the directory. \n \n "
            "Please close it and try again."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


class WorkbookReadError(DataLoaderError):
    """Exception raised for errors in reading the workbook."""

    def __init__(self, message, table_name=None, file_name=None):
        user_message = (
            f"While searching for {table_name}, an unexpected "
            f"error occurred while reading {file_name}. "
            "Please check file is not corrupt and try again."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


class DuplicateSheetError(DataLoaderError):
    """
    Exception raised when duplicate sheets matching validation criteria are found.

    This prevents ambiguity when multiple source files contain sheets with
    similar structures, ensuring data is loaded from the correct source.

    Attributes:
        detailed_info (list of tuples): File name and sheet name pairs of duplicates
    """

    def __init__(self, message, detailed_info, table_name):
        if not all(isinstance(i, tuple) and len(i) == 2 for i in detailed_info):
            raise ValueError(
                "detailed_info must be a list of tuples with exactly two elements each"
            )

        self.detailed_info = detailed_info

        detailed_info_str = ", ".join(
            [
                f"\n\n Sheet named '{sheet}' in file named '{file}'"
                for file, sheet in detailed_info
            ]
        )

        user_message = (
            f"Multiple matches were found when searching for the '{table_name}': {detailed_info_str} \n\n"
            "Please ensure there are no duplicate sheets in the directory."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


class NoValidFilesFound(DataLoaderError):
    """Exception raised when no valid Excel files match the expected format."""

    def __init__(self, message, user_message=None, table_name=None):
        user_message = (
            f"No valid files found for the {table_name}."
            " Please ensure the file containing the source"
            " data is in the correct directory. If the file"
            " exists, the column names or order may have"
            " changed and can't be imported."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


class DtypeGenerationError(DataLoaderError):
    """
    Exception raised for errors in generating the dtype dictionary for pandas DataFrame.

    Ensures that DataFrame dtypes match the ORM model before database insertion.
    """

    def __init__(self, message, user_message=None, table_name=None):
        super().__init__(message, user_message=user_message, table_name=table_name)


class NonNullableColumnError(DataLoaderError):
    """
    Exception raised when non-nullable columns contain null or missing values.

    Enforces data integrity before database insertion by catching missing
    required values early in the pipeline.
    """

    def __init__(
        self, message, user_message=None, table_name=None, missing_columns=None
    ):
        if missing_columns:
            missing_columns_str = "\n".join(missing_columns)
            user_message = (
                f"The '{USER_TABLE_NAMES.get(table_name, table_name)}' "
                "sheet has missing values "
                f"in the following required column(s):\n\n{missing_columns_str}"
                "\n\nPlease ensure there are valid values in these columns "
                "and try again."
            )
        else:
            user_message = (
                "There are missing values in required fields. "
                "Please check your data and try again."
            )
        super().__init__(
            message=message, user_message=user_message, table_name=table_name
        )


class SqliteInsertionError(DataLoaderError):
    """Exception raised for errors in inserting data into SQLite database."""

    def __init__(self, message, user_message=None, table_name=None):
        super().__init__(message, user_message=user_message, table_name=table_name)


class DataframeValidationError(DataLoaderError):
    """Exception raised for errors in validating dataframes before database insertion."""

    def __init__(self, message, user_message=None, table_name=None):
        super().__init__(message, user_message=user_message, table_name=table_name)


# ================ Excel Utilities Exceptions ================


class ExcelUtilitiesError(ApplicationError):
    """Base exception class for Excel utility operations."""

    pass


class CustomHeaderError(ExcelUtilitiesError):
    """Exception raised when custom headers are not generated properly."""

    pass


# =================== Data Validation Exceptions ==================


class DataValidationError(ApplicationError):
    """Base exception class for data validation operations."""

    def __init__(
        self,
        message="An error occurred while validating data.",
        user_message=None,
        table_name=None,
    ):
        super().__init__(message, user_message=user_message, table_name=table_name)


class EnumValidationError(DataValidationError):
    """
    Exception raised for enum validation errors in controlled substance master data.

    Validates that boolean flag columns (include_in_*_reports) contain only 'Y' or 'N'
    values, ensuring report inclusion logic operates correctly.

    Attributes:
        invalid_columns_details (dict): Mapping of column names to lists of invalid NDCs
    """

    user_friendly_column_names = {
        "include_in_arcos_reports": "For ARCOS Reports",
        "include_in_mi_state_reports": "For Michigan State Reports",
        "include_in_ny_state_and_excise_tax_reports": "For New York State and Excise Tax Reports",
        "include_in_dscsa_reports": "For DSCSA Reports",
    }

    def __init__(self, message, table_name=None, invalid_columns_details=None):
        self.invalid_columns_details = (
            invalid_columns_details if isinstance(invalid_columns_details, dict) else {}
        )

        # Aggregate NDCs across columns
        ndcs_set = set()
        for ndcs in invalid_columns_details.values():
            ndcs_set.update(ndcs)

        # Prepare user-friendly column names list
        columns_list = [
            self.user_friendly_column_names.get(col, col)
            for col in invalid_columns_details.keys()
        ]

        # Construct the user-friendly message
        if ndcs_set:
            ndcs_str = ", ".join(sorted(ndcs_set))
            columns_str = ", ".join(sorted(set(columns_list)))
            detailed_user_message = (
                f"Invalid values detected in the master data for the following NDCs: \n\n{ndcs_str}\n\n"
                f"Please ensure the values in the following columns contain a 'Y' or 'N' value: \n\n{columns_str}"
            )
        else:
            detailed_user_message = "No invalid values detected."

        self.user_message = detailed_user_message
        super().__init__(
            message, user_message=detailed_user_message, table_name=table_name
        )


class DeaNumberViolationError(DataValidationError):
    """
    Exception raised for DEA number foreign key violations.

    Ensures all warehouse DEA numbers in transaction data correspond to valid
    warehouse records. Critical for regulatory compliance and report accuracy.
    """

    def __init__(self, message, user_message=None, table_name=None):
        super().__init__(message, user_message=user_message, table_name=table_name)


class NdcValidationError(DataValidationError):
    """
    Exception raised for NDC foreign key validation errors.

    Ensures all NDC numbers in transaction data exist in the controlled
    substance master catalog, preventing orphaned transaction records.
    """

    def __init__(self, message, user_message=None, table_name=None):
        super().__init__(message, user_message=user_message, table_name=table_name)


class CustomerLicenseValidationError(DataValidationError):
    """
    Exception raised for customer license validation errors.

    Validates that all customer transactions have valid, active licenses
    for the transaction date. Essential for regulatory compliance.
    """

    def __init__(self, message, user_message=None, table_name=None):
        super().__init__(message, user_message=user_message, table_name=table_name)


class TINNumInconsistencyError(DataValidationError):
    """
    Exception raised for TIN number inconsistency in warehouse data.

    Ensures TIN numbers (Tax Identification Numbers) are consistent across
    warehouse records and properly formatted (9 digits, no dashes).
    """

    def __init__(self, message, user_message=None, table_name=None):
        user_message = message
        super().__init__(message, user_message=user_message, table_name=table_name)


class MmeError(DataValidationError):
    """
    Exception raised for MME (Morphine Milligram Equivalent) calculation errors.

    Validates that all opioid products have valid MME conversion factors
    and that strength values match between data sources.
    """

    def __init__(self, message, user_message=None, table_name=None):
        super().__init__(message, user_message=user_message, table_name=table_name)


# ================ Report Generation Utilities Exceptions ================


class ReportGenUtilsError(ApplicationError):
    """Base Exception for Report Generation Utilities"""

    def __init__(self, message, user_message=None):
        super().__init__(message, user_message=user_message)


class TimeframeCalculationError(ReportGenUtilsError):
    """Exception raised for errors in calculating report timeframes."""

    def __init__(self, message, user_message=None):
        self.user_message = message or user_message
        super().__init__(message, user_message=user_message)


# ======================== Report Generation Exceptions ========================


class ArcosStyleReportError(ApplicationError):
    """Base Exception for ARCOS-style report generation"""

    pass


class InitializationError(ArcosStyleReportError):
    """Exception raised for unexpected errors during report initialization."""

    def __init__(self, original_error=None):
        message = "An unexpected error occurred during report initialization. "
        logger.error(f"ReportInitializationError: {original_error}")
        user_message = (
            "An unexpected error occurred during initializing the report. "
            "Please try again."
        )
        super().__init__(message, user_message=user_message)


class NoDistinctDeaNumbers(ArcosStyleReportError):
    """Exception raised when no distinct DEA numbers found in the database."""

    def __init__(self, message, user_message=None, table_name=None):
        user_message = message
        super().__init__(message, user_message=user_message, table_name=table_name)


class ReportDateError(ArcosStyleReportError):
    """Exception raised for errors in calculating report dates."""

    def __init__(self, message, user_message=None, table_name=None):
        user_message = (
            "Unexpected error occurred while generating report dates. Contact support."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


class FetchNdcError(ArcosStyleReportError):
    """Exception raised for errors in fetching NDC numbers from the database."""

    def __init__(self, message, user_message=None, table_name=None):
        user_message = (
            "An error occurred while fetching relevant NDCs. Please try again."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


class FetchDeaError(ArcosStyleReportError):
    """Exception raised for errors in fetching DEA numbers from the database."""

    def __init__(self, message, user_message=None, table_name=None):
        user_message = (
            user_message
            or "An error occurred while fetching relevant DEA numbers. Please try again."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


class ReportWriteError(ArcosStyleReportError):
    """Exception raised for errors in writing report output files."""

    def __init__(self, message, user_message=None, table_name=None):
        user_message = (
            user_message
            or "An error occurred while writing the report. Please try again."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


# ======================== State Report Generation Exceptions ========================


class StateReportError(ApplicationError):
    """Base Exception for state-level report generation"""

    def __init__(
        self,
        message,
        user_message=None,
        table_name=None,
    ):
        super().__init__(message, user_message=user_message, table_name=table_name)


class StateDateRangeError(StateReportError):
    """Exception raised for errors in calculating state report date ranges."""

    def __init__(
        self,
        report_type,
        year,
        month=None,
        quarter=None,
        state=None,
        original_error=None,
    ):
        message = (
            "An error occurred while gathering the report dates for the following "
            f"report: {state}, year={year}, month={month}, quarter={quarter}, "
            f"report_type={report_type}: {original_error}."
        )
        user_message = (
            "An error occurred while gathering the report dates. Please try again."
        )
        super().__init__(message, user_message=user_message)


class StateSalesDataFetchError(StateReportError):
    """Exception raised for errors fetching sales data for state reports."""

    def __init__(self, state, start_date, end_date, original_error=None):
        message = (
            f"An error occurred while fetching sales data for {state} "
            f"from {start_date} to {end_date}: {original_error}."
        )
        user_message = "An error occurred while fetching sales data. Please try again."
        super().__init__(message, user_message=user_message)


class StateNoSalesDataFoundError(StateReportError):
    """Exception raised when no sales data exists for the specified timeframe."""

    def __init__(self, state, start_date, end_date):
        message = (
            f"No sales data found for {state} report from {start_date} to {end_date}."
        )
        user_message = (
            "No sales data available for the selected timeframe. Please "
            "verify the timeframe and try again."
        )
        super().__init__(message, user_message=user_message)


class StateRelevantNDCsFetchError(StateReportError):
    """Exception raised for errors fetching relevant NDCs for state reports."""

    def __init__(self, state_col, e=None):
        message = f"Error fetching relevant NDCs based on column: {state_col}: {e}."
        user_message = (
            "An error occurred while fetching relevant NDCs. Please try again."
        )
        super().__init__(message, user_message=user_message)


class StateTemplateNotFoundError(StateReportError):
    """Exception raised when Excel report template file is not found."""

    def __init__(self, message, user_message=None, table_name=None):
        user_message = (
            "An error occurred while finding the template file. Please try again."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


# ================ Database Interaction Exceptions =================


class DbInteractionError(ApplicationError):
    """Base Exception for database interaction operations"""

    def __init__(
        self, message, user_message=None, table_name=None, original_error=None
    ):
        super().__init__(
            message,
            user_message=user_message,
            table_name=table_name,
        )
        self.original_error = original_error
        logger.error(f"DB Interaction Error: {original_error}")


class FetchRelevantNdcError(DbInteractionError):
    """Exception raised for errors in fetching relevant NDC numbers."""

    def __init__(self, state_col, state_report_flag, original_error=None):
        self.state_col = state_col
        self.state_report_flag = state_report_flag
        self.original_error = original_error
        detail = f"Fetching relevant NDCs failed for '{state_col}' with flag '{state_report_flag}'."
        user_message = "An error occurred while trying to fetch relevant NDC data. Please try again or contact support."
        super().__init__(message=detail, user_message=user_message)


class FetchSalesDataframeError(DbInteractionError):
    """Exception raised for errors in fetching sales dataframes."""

    def __init__(self, state, start_date, end_date, original_error=None):
        message = f"An error occurred while fetching sales data for {state} from {start_date} to {end_date}: {original_error}."
        user_message = f"An error occurred while fetching sales data for {state} from {start_date} to {end_date}. Please try again."
        super().__init__(message, user_message=user_message)


class MergeSalesWithWarehouseError(DbInteractionError):
    """
    Exception raised for errors in merging sales data with warehouse data.

    This error should hypothetically never occur due to foreign key validations,
    but provides a safety net for unexpected data scenarios.
    """

    def __init__(self, message, user_message=None, table_name=None):
        user_message = (
            "An unexpected error occurred while matching sales data with "
            "warehouse license information for report generation. "
            "Please try again or contact support."
        )
        super().__init__(message, user_message=user_message, table_name=table_name)


class CSMergeMissingDataError(DbInteractionError):
    """Exception raised for missing calculation data when merging with CS Master."""

    def __init__(self, message, user_message=None, table_name=None):
        user_message = "An error occurred while trying to fetch calculation data. Please try again or contact support."
        super().__init__(message, user_message=user_message, table_name=table_name)


class SalesDataDateRangeError(DbInteractionError):
    """Exception raised for errors in getting sales data date range."""

    pass


class MergeSalesWithCSDataError(DbInteractionError):
    """Exception raised for errors in merging sales data with CS master data."""

    def __init__(self, message, user_message=None, table_name=None):
        super().__init__(message, user_message=user_message, table_name=table_name)


class DeaNumberLicenseError(DbInteractionError):
    """Exception raised for errors in fetching DEA number license information."""

    def __init__(self, message, user_message=None):
        super().__init__(message, user_message=user_message)


class MultipleValidCustomerLicensesFoundError(DbInteractionError):
    """
    Exception raised when multiple valid licenses exist for a customer on a date.

    Indicates ambiguity in license data that must be resolved for compliance.
    """

    def __init__(self, message, user_message=None, table_name=None):
        user_message = message
        super().__init__(message, user_message=user_message, table_name=table_name)


class NoValidCustomerLicensesFoundError(DbInteractionError):
    """
    Exception raised when no valid customer licenses are found.

    Indicates transactions occurred without valid customer licensing,
    a critical compliance issue.
    """

    def __init__(self, message, user_message=None, table_name=None):
        user_message = message
        super().__init__(message, user_message=user_message, table_name=table_name)


class IsFirstSaleDataError(DbInteractionError):
    """Exception raised for errors in calculating first sale data."""

    def __init__(self, message, user_message=None, original_error=None):
        super().__init__(message, user_message=user_message)
        self.original_error = original_error
