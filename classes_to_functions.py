from functools import partial
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
def get_stream_trade_data(
    stream: Sequence[str]
) -> Tuple[str, ...]:
    return tuple(_line for _line in stream)


def init_get_stream_trade_data(
    stream: Sequence[str]
) -> CGetTradeData:
    return partial(get_stream_trade_data, stream)
    
# Replace SimpleTradeParser.parse
def parse_simple_trades(
    validate_trade_fields: CValidateTradeFields,
    map_trade: CMapTrade,
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


def init_parse_simple_trades(
    validate_trade_fields: CValidateTradeFields,
    map_trade: CMapTrade,
) -> CParseTradeData:
    return partial(parse_simple_trades, validate_trade_fields, map_trade)
    
# Replace SimpleTradeValidator.validate
def validate_simple_trade(
    logger: ILogger,
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


def init_validate_simple_trade(
    logger: ILogger
) -> CValidateTradeFields:
    return partial(validate_simple_trade, logger)

# Replace SqlAlchemyTradeStorage.persist
def persist_trade_sqlalchemy(
    connection_str: str,
    table_name: str,
    logger: ILogger,
    trades: Sequence[TradeRecord]
) -> None:
    engine = create_engine(connection_str, echo=False)
    metadata = MetaData()
    metadata.reflect(engine)
    tble = Table(table_name, metadata, autoload=True, autoload_with=engine)

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


def init_persist_trade_sqlalchemy(
    connection_str: str,
    table_name: str,
    logger: ILogger
) -> CPersistTrade:
    return partial(persist_trade_sqlalchemy. connection_str, table_name, logger)
    

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

# Implements ILogger interface
class SimpleLogger:
    def log_warning(self, warning: str) -> None:
        print('WARNING:', warning)
    
    def log_info(self, info: str) -> None:
        print('INFO:', info)

    def log_error(self, error: str) -> None:
        print('ERROR:', error)


def process_simple_trades(
    stream: Sequence[str],
    connection_str: str,
    table_name: str
) -> None:
    get_trade_data = init_get_stream_trade_data(stream=stream)
    logger = SimpleLogger()
    validate_trade_fields = init_validate_simple_trade(logger=logger)
    parse_trade_data = init_parse_simple_trades(validate_trade_fields=validate_trade_fields, map_trade=map_simple_trade)
    persist_trade = init_persist_trade_sqlalchemy(connection_str=connection_str, table_name=table_name, logger=logger)
    process_trades(get_trade_data, parse_trade_data, persist_trade)