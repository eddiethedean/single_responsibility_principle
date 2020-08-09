class TradeRecord:
    def __init__(self, source_currency: str, destination_currency: str, lots: float, price: float) -> None:
        self.source_currency = source_currency
        self.destination_currency = destination_currency
        self.lots = lots
        self.price = price

    def __repr__(self) -> str:
        return f'''TradeRecord(source_currency={self.source_currency},
                               destination_currency={self.destination_currency},
                               lots={self.lots},
                               price={self.price}'''