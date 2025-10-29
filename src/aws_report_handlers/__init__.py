"""
AWS Cost and Usage Report Handlers

Supports both legacy and modern report formats:
- Legacy: ZIP files, date-based folders (YYYYMMDD-YYYYMMDD)
- Modern: GZIP files, billing period partitions (BILLING_PERIOD=YYYY-MM)
"""

from .handler_factory import ReportHandlerFactory
from .version_detector import ReportVersionDetector

__all__ = ["ReportHandlerFactory", "ReportVersionDetector"]
