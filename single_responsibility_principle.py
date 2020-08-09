from ignore_exception import int_try_parse, float_try_parse
from typing import List, Optional, Sequence

from sqlalchemy import Table, MetaData, create_engine
from trade_record import TradeRecord

# Listing 1
class TradeProcessor:
    lots_size: float = 100000.0

    def process_trades(self, stream: Sequence[str]) -> None:
        # read rows
        lines: List[str] = []
        line: str
        for line in stream:
            lines.append(line)

        trades: List[TradeRecord] = []

        line_count: int = 1
        for line in lines:
            fields: List[str] = line.split(',')
            if len(fields) != 3:
                print(f'WARN: Line {line_count} malformed. Only {len(fields)} fields(s) found.')
                continue
            if len(fields[0]) !=6:
                print(f"WARN: Trade currencies on line {line_count} malformed: '{len(fields)}'")
                continue

            trade_amount: Optional[int] = int_try_parse(fields[1])
            if trade_amount is None:
                print(f"WARN: Trade amount on line {line_count} not a valid integer: '{fields[1]}'")

            trade_price: Optional[float] = float_try_parse(fields[2])
            if trade_price is None:
                print(f"WARN: Trade price on line {line_count} not a valid decimal: '{fields[2]}'")
                trade_price = -1.0

            source_currency_code: str = fields[0][:3]
            destination_currency_code: str = fields[0][3:6]

            # calculate values
            trade = TradeRecord(source_currency_code,
                                destination_currency_code,
                                trade_amount/self.lots_size,
                                trade_price)
            trades.append(trade)

            line_count += 1
        
        engine = create_engine('sqlite:///trades.db', echo=False)
        metadata = MetaData(engine)
        tbl = Table('trade_table', metadata, autoload=True, autoload_with=engine)

        for trade in trades:
            ins = tbl.insert(None).values(source_currency=trade.source_currency,
                                       destination_currency=trade.destination_currency,
                                       lots=trade.lots,
                                       price=trade.price)
            conn = engine.connect()
            conn.execute(ins)

        print(f'INFO: {len(trades)} trades processed')



