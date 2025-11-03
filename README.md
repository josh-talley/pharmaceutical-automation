# Pharmaceutical Compliance Automation

**Production-grade Python systems extracted from a commercial pharmaceutical compliance application.**

> **Production Metrics:** 1+ years in production | 20,000+ transactions quarterly | **Zero errors** in production runtime

> **Note:** State identifiers have been anonymized for client confidentiality.

---

## 🎯 Overview

This repository showcases **core system designs** extracted from a production pharmaceutical compliance automation application. The full system processes controlled substance transaction data, performs multi-layer validation, and generates regulatory reports (ARCOS, DSCSA, state-level reporting) for DEA and state regulatory agencies.

**Why this project matters:** In pharmaceutical compliance, errors have serious consequences - incorrect regulatory submissions can result in DEA violations, fines, or loss of controlled substance licenses. The production system has processed over **80,000+ controlled substance transactions** with **zero errors** over 1+ years of production use.

### What's Included

This repository contains **reference implementations** of the core subsystems:

- **Database models** (SQLAlchemy ORM with complex relationships)
- **Multi-layer validation system** (foreign key, enum, date-range, business logic validation)
- **Worker thread pattern** (non-blocking GUI with background processing)
- **Transaction management** (context manager pattern for ACID compliance)
- **Exception hierarchy** (user-friendly error messages for non-technical users)
- **Working example** (standalone validation demo you can run)

**Note:** These are extracted system designs from a commercial application. Core modules demonstrate production code patterns and design decisions. See `examples/` for a simplified working demonstration.

---

## 📁 Repository Structure

```
pharmaceutical-compliance-automation/
├── README.md                   # This file
├── requirements.txt            # Core dependencies
├── LICENSE                     # MIT License
│
├── models.py                   # SQLAlchemy ORM models (reference)
├── exceptions.py               # Custom exception hierarchy (reference)
├── worker_threads.py           # Multi-threaded task management (reference)
├── data_validator.py           # Multi-layer validation system (reference)
├── db_manager.py               # Transaction management (reference)
│
├── docs/
│   ├── architecture.md         # System design patterns
│   └── validation-workflow.md  # Validation layer deep-dive
│
└── examples/
    ├── validation_example.py   # 🟢 Standalone working demo
    └── sample_data.sql         # Mock database for demo
```

---

## 🚀 Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/josh-talley/pharmaceutical-compliance-automation.git
cd pharmaceutical-compliance-automation

# Install dependencies
pip install -r requirements.txt

# Run working validation demo
python examples/validation_example.py
```

The validation demo creates a sample database and demonstrates the validation workflow with progress tracking and error handling.

---

## 💡 System Architecture Highlights

### Database Schema (models.py)

The system models pharmaceutical distribution transactions with 5 core tables:

1. **TransactionData** - Individual controlled substance shipments (DEA-reportable transactions)
2. **ControlledSubstanceMaster** - Product catalog with regulatory flags
3. **WarehouseData** - Distribution center information (DEA numbers, licenses, TIN)
4. **CustomerLicenseData** - Customer license validity dates
5. **NdcMmeData** - Morphine Milligram Equivalent conversion factors

**Key Design Challenge:** SQLite doesn't enforce foreign key constraints by default. Manual validation layer prevents orphaned records.

### Air-Gapped Deployment Model

**Standalone executable compiled with PyInstaller** - Deployed directly on client workstations with no server or network dependencies.

**Why air-gapped?**
- Controlled substance transaction data is DEA-regulated and highly sensitive
- Client security policy requires all processing within their secure network perimeter
- Eliminates attack surface (no network connections, no external APIs, no cloud dependencies)
- Simplifies deployment (no server infrastructure, no authentication layer, no network configuration)

**Security-first architecture:**
- All processing occurs locally (data never leaves client's secure environment)
- SQLite database (local file-based, no database server required)
- Reports generated as local Excel files (no data transmission)
- Suitable for regulated industries requiring data isolation (HIPAA, DEA compliance)

**SOC parallel:** Similar to air-gapped SIEM deployments in classified or high-security environments where threat intelligence must be processed without external connectivity.

### Design Patterns Demonstrated

#### 1. Context Manager Pattern (db_manager.py)

Ensures transaction safety with automatic commit/rollback:

```python
with trans_mgmt.transaction() as session:
    # All operations here are atomic
    session.add(record)
    session.query(...).update(...)
    # Auto-commit on success, auto-rollback on exception
```

**Production Impact:** Zero database corruption incidents in 1+ years of production. All-or-nothing data integrity.

#### 2. Worker Thread Pattern (worker_threads.py)

Isolates long-running operations from GUI thread:

- DataLoaderWorker (separate thread for import operations)
- ValidationWorker (chained after load completes)
- ReportGenThread (thread pool for parallel report generation)
- Progress tracking via PyQt signals (thread-safe communication)

**Production Impact:** 20K+ transactions imported in ~15 seconds with responsive GUI.

#### 3. Multi-Layer Validation (data_validator.py)

Validates data integrity before regulatory submission:

- Enum validation (boolean flags)
- Foreign key validation (manual enforcement)
- License date-range validation
- TIN consistency validation
- MME calculation validation

**Production Impact:** Catches data quality issues before regulatory submission.

#### 4. Custom Exception Hierarchy (exceptions.py)

Separates technical errors from user-friendly messages:

```python
raise NdcValidationError(
    # Technical message (logged)
    "Foreign key violations in TransactionData",
    # User-friendly message (displayed in GUI)
    user_message="Unrecognized product NDC. Please add to master catalog."
)
```

**Production Impact:** Non-technical users can self-service errors without developer intervention.

---

## 📊 Production Reliability

### Verified Production Metrics
- **Total runtime:** 1+ years in production (July 2023 - Present)
- **Error rate:** **Zero errors** (zero failed transactions, zero data corruption)
- **Transaction volume:** 20,000+ per quarter (80,000+ total)
- **Validation effectiveness:** 100% catch rate (catches issues before submission)
- **Process improvement:** Reduced quarterly compliance reporting from 40 hours to 4 hours

### System Capabilities
- **Validation methods:** 7 independent validation layers
- **Database tables:** 5-table schema modeling pharmaceutical distribution
- **Report types:** Multiple regulatory reports (DEA ARCOS, DSCSA, state-level)
- **Threading:** Multi-threaded report generation for parallel processing
- **Deployment:** PyInstaller standalone executable, air-gapped security architecture

### Reliability Features
- Automatic rollback on validation failure
- Comprehensive logging (timestamps, error traces)
- Real-time progress tracking
- User-friendly error messages with actionable guidance

---

## 🎓 Skills Demonstrated

### For Software Engineering Roles
- **Python OOP** - Classes, inheritance, context managers, decorators
- **SQLAlchemy ORM** - Complex relationships, query optimization, transaction management
- **Multi-threading** - Worker threads, thread-safe communication, progress tracking
- **Design Patterns** - Context manager, worker thread, signal/slot, custom exceptions
- **Data Validation** - Integrity checks, business logic validation, error recovery
- **Production Reliability** - 1+ years zero-error runtime, comprehensive logging

### For SOC/Security Roles

This project demonstrates **security operations mindset** applicable to SOC work:

| Compliance System | SOC Analogy | Skill Transfer |
|-------------------|-------------|----------------|
| **Multi-layer validation** | Alert validation | Reduce false positives through multi-stage verification |
| **Foreign key validation** | Threat intel correlation | Validate indicators exist in threat feeds before acting |
| **Transaction rollback** | Incident response rollback | Safely revert changes if automation fails |
| **Progress tracking** | Investigation tracking | Real-time status during long operations |
| **Error classification** | Alert triage | Categorize issues by severity and type |
| **Audit logging** | SIEM logging | Comprehensive event logging for investigations |
| **Data integrity checks** | Log validation | Ensure data quality before analysis |
| **Automated workflows** | SOAR playbooks | Background automation with human oversight |

**Key Insight:** Building reliable automation systems requires the same mindset as SOC operations - validate inputs, log everything, fail safely, provide actionable alerts.

---

## 🔍 Code Highlights

### Transaction Safety (db_manager.py:60-82)

```python
@contextmanager
def transaction(self):
    """Automatic commit/rollback with guaranteed cleanup."""
    session = self.session_factory()
    try:
        yield session
        session.commit()  # Success: persist changes
    except Exception as e:
        session.rollback()  # Error: undo all changes
        raise
    finally:
        session.close()  # Always: release connection
```

### Multi-threaded Progress Tracking (worker_threads.py:242-281)

```python
class ValidationWorker(QObject):
    progressUpdate = pyqtSignal(str, int)  # Thread-safe signal

    def run(self):
        for validation_method in self.validation_methods:
            self.progressUpdate.emit(validation_method, 0)
            method(session, progress_callback=self.progressUpdate.emit)
            self.validationSuccess.emit(validation_method)
```

### User-Friendly Errors (exceptions.py:253-298)

```python
class EnumValidationError(DataValidationError):
    """Validates boolean flags with user-friendly column name mapping."""

    user_friendly_column_names = {
        "include_in_arcos_reports": "For ARCOS Reports",
        "include_in_state_reports": "For State-1 Reports",
    }

    # Converts technical errors to plain-language guidance
```

### Foreign Key Validation (data_validator.py:162-227)

```python
def val_warehouse_dea_numbers(self, session, progress_callback=None):
    """Manual FK validation - SQLite doesn't enforce by default."""

    # LEFT OUTER JOIN to find orphaned records
    warehouse_violations = (
        session.query(TransactionData.reporting_registrant_num)
        .distinct()
        .outerjoin(WarehouseData, ...)
        .filter(WarehouseData.dea_number.is_(None))
        .all()
    )

    if warehouse_violations:
        raise DeaNumberViolationError(...)
```

---

## 🧪 Working Example

The repository includes a **standalone validation demo** that you can actually run:

```bash
python examples/validation_example.py
```

**What it demonstrates:**
- Creating an in-memory SQLite database
- Inserting sample transaction and master data
- Running validation methods with progress tracking
- Handling validation errors with user-friendly messages
- Transaction rollback on validation failure

**Sample Output:**

```
🔍 Pharmaceutical Compliance Validation Demo
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Creating sample database with test data...
✓ Database initialized

Running validation workflow...
[1/3] Validating enum columns... ✓ (100%)
[2/3] Validating warehouse DEA numbers... ✓ (100%)
[3/3] Validating product NDC numbers... ✓ (100%)

✅ All validations passed!
Database ready for report generation.

Testing error handling...
❌ Validation Error Caught:
   Unrecognized product NDC found in transactions.
   Please add product to master catalog.

Transaction automatically rolled back. Database unchanged.
```

This simplified demo illustrates the core validation pattern without the full application complexity.

---

## 📚 Documentation

- **[Architecture Guide](docs/architecture.md)** - Deep dive into design patterns and system architecture
- **[Validation Workflow](docs/validation-workflow.md)** - Detailed explanation of multi-layer validation system

---

## 🤝 About This Repository

This is a **portfolio showcase** repository containing reference implementations extracted from a commercial pharmaceutical compliance application. The code demonstrates:

- Production-quality design patterns
- Reliability engineering practices (1+ years zero-error runtime)
- Professional documentation standards
- Systems thinking and problem-solving approach

**What's included:**
- Core system modules (models, validation, threading, transactions, exceptions)
- Comprehensive documentation
- Working validation demo

**What's NOT included:**
- Full application source code
- Proprietary business logic
- Client-specific information
- Report generation implementations

If you find these patterns useful for learning or reference, feel free to star the repository or reach out with questions about implementation decisions.

---

## 📄 License

MIT License - See [LICENSE](LICENSE) for details.

Code provided for educational and portfolio purposes.

---

## 👤 About the Developer

**Joshua J. Talley**
Security+ Certified | Python Developer | Transitioning to SOC Operations

- 📧 Email: josh@joshtalley.com
- 💼 LinkedIn: [linkedin.com/in/josh-talley](https://linkedin.com/in/josh-talley)
- 🐙 GitHub: [github.com/josh-talley](https://github.com/josh-talley)

**Background:**
- Built production automation with **1+ years zero-error runtime**
- Processed **80,000+ pharmaceutical transactions** with 100% accuracy
- PCEP Certified Python Programmer (July 2023)
- CompTIA Security+ (October 2025)
- Building SOC-focused homelab (ELK stack processing 200K+ security events)

**Seeking:** SOC Analyst Tier 1 roles where I can apply automation, validation, and reliability engineering skills to security operations.

---

**⭐ If you found these design patterns useful, please star the repository!**

*Last Updated: November 2025*
