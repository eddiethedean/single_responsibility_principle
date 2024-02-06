from dataclasses import dataclass


@dataclass
class TradeRecord:
    source_currency: str
    destination_currency: str
    lots: float
    price: float
