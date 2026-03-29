"""Service 层 — 财务报表查询。薄委托到 Data 层。"""

from stock_service.data import finance as finance_data
from stock_service.models import FinanceReportType
from stock_service.models.finance import FinanceRecord, FinanceResponse


def get_financial(
    symbol: str,
    report_type: FinanceReportType,
    start_period: str,
    end_period: str,
) -> FinanceResponse:
    rows = finance_data.get_reports(symbol, report_type, start_period, end_period)
    records = [
        FinanceRecord(
            period=row["period"],
            ann_date=row["ann_date"],
            data=row["data"],
        )
        for row in rows
    ]
    return FinanceResponse(
        symbol=symbol,
        report_type=report_type,
        total=len(records),
        records=records,
    )
