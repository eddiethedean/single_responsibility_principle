from typing import List, Optional, Sequence, Tuple
import abc

from sqlalchemy import Table, MetaData, create_engine
from trade_record import TradeRecord
from ignore_exception import int_try_parse, float_try_parse


class ITradeProvider(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_trade_data(self) -> Sequence[str]:
        raise NotImplementedError


class ITradeParser(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def parse(self, trade_data: Sequence[str]) -> Sequence[TradeRecord]:
        raise NotImplementedError


class ITradeValidator(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def validate(self, fields: Sequence[str]) -> bool:
        raise NotImplementedError


class ITradeMapper(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def map(self, trade: Sequence[str]) -> TradeRecord:
        raise NotImplementedError


class ITradeStorage(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def persist(self, trades: Sequence[TradeRecord]) -> None:
        raise NotImplementedError


class ILogger(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def log_warning(self, warning: str) -> None:
        raise NotImplementedError
    
    @abc.abstractmethod
    def log_info(self, info: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def log_error(self, error: str) -> None:
        raise NotImplementedError

# Listing 9
class TradeProcessor:
    def __init__(self,
                 trade_data_provider: ITradeProvider,
                 trade_parcer: ITradeParser,
                 trade_storage: ITradeStorage) -> None:
        self.trade_data_provider = trade_data_provider
        self.trade_parcer = trade_parcer
        self.trade_storage = trade_storage

    def process_trades(self) -> None:
        lines: Sequence[str] = self.trade_data_provider.get_trade_data()
        trades: Sequence[TradeRecord] = self.trade_parcer.parse(lines)
        self.trade_storage.persist(trades)

# Listing 10
class StreamTradeProvider(ITradeProvider):
    def __init__(self, stream: Sequence[str]):
        self.stream = stream

    def get_trade_data(self) -> Tuple[str]:
        return Tuple(_line for _line in self.stream)


# Listing 11
class SimpleTradeParser(ITradeParser):
    def __init__(self,
                 tradeValidator: ITradeValidator,
                 tradeMapper: ITradeMapper) -> None:
        self.tradeValidator = tradeValidator
        self.tradeMapper = tradeMapper
    
    def parse(self, trade_data: Sequence[str]) -> List[TradeRecord]:
        trades: List[TradeRecord] = []
        line: str
        for line in trade_data:
            fields: List[str] = line.split(',')
            if not self.tradeValidator.validate(fields):
                continue
            trade = self.tradeMapper.map(fields)
            trades.append(trade)
        return trades

# Listing 12
class SimpleTradeValidator(ITradeValidator):
    def __init__(self, logger: ILogger) -> None:
        self.logger = logger

    def validate(self, fields: Sequence[str]) -> bool:
        field_len: int = len(fields)
        if field_len != 3:
            self.logger.log_warning(f'Line malformed. Only {field_len} field(s) found.')
            return False
        field_one: str = fields[0]
        if len(field_one) != 6:
            self.logger.log_warning(f"Trade currency malformed: '{field_one}'")
            return False

        trade_amount: Optional[int] = int_try_parse(fields[1])
        if trade_amount is None:
            self.logger.log_warning(f"WARN: Trade amount not a valid integer: '{fields[1]}'")
            return False

        trade_price: Optional[float] = float_try_parse(fields[2])
        if trade_price is None:
            self.logger.log_warning(f"WARN: Trade amount not a valid decimal: '{fields[2]}'")
            return False
            
        return True


class SqlAlchemyTradeStorage(ITradeStorage):
    def __init__(self, logger: ILogger) -> None:
        self.logger = logger

    def persist(self, trades: Sequence[TradeRecord]) -> None:
        engine = create_engine('sqlite:///trades.db', echo=False)
        metadata = MetaData(engine)
        tble = Table('trade_table', metadata, autoload=True, autoload_with=engine)

        for trade in trades:
            ins = tble.insert(None).values(source_currency=trade.source_currency,
                                           destination_currency=trade.destination_currency,
                                           lots=trade.lots,
                                           price=trade.price)
            conn = engine.connect()
            conn.execute(ins)

        self.logger.log_warning(f'INFO: {len(trades)} trades processed')


    
class SimpleTradeMapper(ITradeMapper):
    lots_size: float = 100000.0
    def map(self, fields: Sequence[str]) -> TradeRecord:
        source_currency_code: str = fields[0][:3]
        destination_currency_code: str = fields[0][3:6]
        trade_amount = int(fields[1])
        trade_price = float(fields[2])
        trade_record = TradeRecord(source_currency=source_currency_code,
                                   destination_currency=destination_currency_code,
                                   lots=trade_amount/self.lots_size,
                                   price=trade_price)
        return trade_record



    



