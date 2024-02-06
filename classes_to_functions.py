from typing import List, Optional, Sequence, Protocol, Tuple

from sqlalchemy import MetaData, Table, create_engine

from trade_record import TradeRecord
from ignore_exception import int_try_parse, float_try_parse


# Replace ITradeProvider.get_trade_data
class CGetTradeData(Protocol):
    def __call__(self) -> Sequence[str]:
        ...
    
# Replace ITradeParser.parse
class CParseTradeData(Protocol):
    def __call__(self, trade_data: Sequence[str]) -> Sequence[TradeRecord]:
        ...
    
# Replace ITradeValidator.validate
class CValidateTradeFields(Protocol):
    def __call__(self, fields: Sequence[str]) -> bool:
        ...
    
# Replace ITradeMapper.map
class CMapTrade(Protocol):
    def __call__(self, fields: Sequence[str]) -> TradeRecord:
        ...
    
# Replace ITradeStorage.persist
class CPersistTrade(Protocol):
    def __call__(self, trades: Sequence[TradeRecord]) -> None:
        ...


class ILogger(Protocol):
    def log_warning(self, warning: str) -> None:
        ...
    
    def log_info(self, info: str) -> None:
        ...

    def log_error(self, error: str) -> None:
        ...


# Replace TradeProcessor.process_trades
def process_trades(
    get_trade_data: CGetTradeData,
    parse_trade_data: CParseTradeData,
    persist_trade: CPersistTrade
) -> None:
    lines: Sequence[str] = get_trade_data()
    trades: Sequence[TradeRecord] = parse_trade_data(trade_data=lines)
    persist_trade(trades=trades)
    
# Replaces StreamTradeProvider.get_trade_data
# Implements CGetTradeData interface
def get_stream_trade_data(
    stream: Sequence[str] # pass with partial
) -> Tuple[str, ...]:
    return tuple(_line for _line in stream)
    
# Replace SimpleTradeParser.parse
# Implements CParseTradeData interface
def parse_simple_trades(
    validate_trade_fields: CValidateTradeFields, # pass with partial
    map_trade: CMapTrade,                        # pass with partial
    trade_data: Sequence[str]
) -> List[TradeRecord]:
    trades: List[TradeRecord] = []
    line: str
    for line in trade_data:
        fields: List[str] = line.split(',')
        if not validate_trade_fields(fields=fields):
            continue
        trade = map_trade(fields=fields)
        trades.append(trade)
    return trades
    
# Replace SimpleTradeValidator.validate
# Implements CValidateTradeFields
def validate_simple_trade(
    logger: ILogger, # pass with partial
    fields: Sequence[str]
) -> bool:
    field_len: int = len(fields)
    if field_len != 3:
        logger.log_warning(f'Line malformed. Only {field_len} field(s) found.')
        return False
    field_one: str = fields[0]
    if len(field_one) != 3:
        logger.log_warning(f"Trade currency malformed: '{field_one}'")
        return False

    trade_amount: Optional[int] = int_try_parse(fields[1])
    if trade_amount is None:
        logger.log_warning(f"WARN: Trade amount not a valid integer: '{fields[1]}'")
        return False

    trade_price: Optional[float] = float_try_parse(fields[2])
    if trade_price is None:
        logger.log_warning(f"WARN: Trade price not a valid decimal: '{fields[2]}'")
        return False
        
    return True

# Replace SqlAlchemyTradeStorage.persist
# Implements CPersistTrade
def persist_trade_sqlalchemy(
    connection_str: str, # pass with partial
    logger: ILogger,     # pass with partial
    trades: Sequence[TradeRecord]
) -> None:
    engine = create_engine(connection_str, echo=False)
    metadata = MetaData()
    metadata.reflect(engine)
    tble = Table('trade_table', metadata, autoload=True, autoload_with=engine)

    for trade in trades:
        with engine.connect() as conn:
            ins = tble.insert().values(
                source_currency=trade.source_currency,
                destination_currency=trade.destination_currency,
                lots=trade.lots,
                price=trade.price
            )
            conn.execute(ins)
            conn.commit()

    logger.log_info(f'INFO: {len(trades)} trades processed')
    

# Replace SimpleTradeMapper.map
# Implements CMapTrade interface
def map_simple_trade(fields: Sequence[str]) -> TradeRecord:
    lots_size: float = 100000.0
    source_currency_code: str = fields[0][:3]
    destination_currency_code: str = fields[0][3:6]
    trade_amount = int(fields[1])
    trade_price = float(fields[2])
    trade_record = TradeRecord(
                        source_currency=source_currency_code,
                        destination_currency=destination_currency_code,
                        lots=trade_amount/lots_size,
                        price=trade_price
                    )
    return trade_record


class SimpleLogger:
    def log_warning(self, warning: str) -> None:
        print('WARNING:', warning)
    
    def log_info(self, info: str) -> None:
        print('INFO:', info)

    def log_error(self, error: str) -> None:
        print('ERROR:', error)