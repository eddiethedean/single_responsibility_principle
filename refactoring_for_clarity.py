from typing import List, Optional, Tuple, Sequence

from sqlalchemy import Table, MetaData, create_engine
from trade_record import TradeRecord
from ignore_exception import int_try_parse, float_try_parse


class TradeProcessor:
    lots_size: float = 100000.0

    # Listing 2
    def process_trades(self, stream: Sequence[str]) -> None:
        lines: Tuple[str, ...] = self.__read_trade_data(stream)
        trades: List[TradeRecord] = self.__parse_trades(lines)
        self.__store_trades(trades)

    # Listing 3
    def __read_trade_data(self, stream: Sequence[str]) -> Tuple[str, ...]:
        return tuple(_line for _line in stream)

    # Listing 4
    def __parse_trades(self, trade_data: Sequence[str]) -> List[TradeRecord]:
        trades: List[TradeRecord] = []
        line_count: int = 1
        for line in trade_data:
            fields: List[str] = line.split(',')
            if not self.__validate_trade_data(fields, line_count):
                continue
            trade: TradeRecord = self.__map_trade_data_to_trade_record(fields)
            trades.append(trade)
            line_count += 1
        return trades

    # Listing 5
    def __validate_trade_data(self, fields: Sequence[str], line_count: int) -> bool:
        if len(fields) != 3:
            self.__log_message(f'WARN: Line {line_count} malformed. Only {len(fields)} fields(s) found.')
            return False

        if len(fields[0]) != 3:
            self.__log_message(f"WARN: Trade currencies on line {line_count} malformed: '{len(fields)}'")
            return False

        trade_amount: Optional[int] = int_try_parse(fields[1])
        if trade_amount is None:
            self.__log_message(f"WARN: Trade amount on line {line_count} not a valid integer: '{fields[1]}'")
            return False

        trade_price: Optional[float] = float_try_parse(fields[2])
        if trade_price is None:
            self.__log_message(f"WARN: Trade price on line {line_count} not a valid decimal: '{fields[2]}'")
            return False

        return True

    # Listing 6
    def __log_message(self, message: str, *args, **kwargs) -> None:
        print(message, *args, **kwargs)
    
    # Listing 7
    def __map_trade_data_to_trade_record(self, fields: Sequence[str]) -> TradeRecord:
        source_currency_code: str = fields[0][:3]
        destination_currency_code: str = fields[0][3:6]
        trade_amount = int(fields[1])
        trade_price = float(fields[2])
        trade_record = TradeRecord(source_currency=source_currency_code,
                                   destination_currency=destination_currency_code,
                                   lots=trade_amount/self.lots_size,
                                   price=trade_price)
        return trade_record

    # Listing 8
    def __store_trades(self, trades: Sequence[TradeRecord]) -> None:
        engine = create_engine('sqlite:///trades.db', echo=False)
        metadata = MetaData()
        metadata.reflect(engine)
        tble = Table('trade_table', metadata, autoload=True, autoload_with=engine)

        for trade in trades:
            ins = tble.insert().values(source_currency=trade.source_currency,
                                           destination_currency=trade.destination_currency,
                                           lots=trade.lots,
                                           price=trade.price)
            conn = engine.connect()
            conn.execute(ins)

        self.__log_message(f'INFO: {len(trades)} trades processed')