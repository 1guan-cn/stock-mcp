from pydantic import BaseModel

from stock_service.models import FinanceReportType


class FinanceRecord(BaseModel):
    period: str
    ann_date: str | None = None
    data: dict


class FinanceResponse(BaseModel):
    symbol: str
    report_type: FinanceReportType
    total: int
    records: list[FinanceRecord]
