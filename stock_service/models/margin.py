from pydantic import BaseModel


class MarginBar(BaseModel):
    date: str
    rzye: float | None = None      # 融资余额（元）
    rzmre: float | None = None     # 融资买入额（元）
    rzche: float | None = None     # 融资偿还额（元）
    rqye: float | None = None      # 融券余额（元）
    rqmcl: float | None = None     # 融券卖出量（股）
    rqchl: float | None = None     # 融券偿还量（股）
    rqyl: float | None = None      # 融券余量（股）


class MarginItem(BaseModel):
    code: str
    name: str
    as_of: str
    data: list[MarginBar]
