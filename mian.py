from functools import partial
from classes_to_functions import map_simple_trade, persist_trade_sqlalchemy, process_trades
from classes_to_functions import get_stream_trade_data, map_simple_trade
from classes_to_functions import parse_simple_trades, validate_simple_trade
from classes_to_functions import SimpleLogger

if __name__ == '__main__':
    connection_str = 'sqlite:///trades.db'
    trades = [
        'USA,100,45.98',
        'EUR,105,56.78',
        'USA,300,50.00',
        'USA,160,7.66',
        'EUR,550,900.80',
        'USA,170,36.29',
    ]
    get_trade_data = partial(get_stream_trade_data, stream=trades)
    logger = SimpleLogger()
    validate_trade_fields = partial(validate_simple_trade, logger=logger)
    parse_trade_data = partial(parse_simple_trades, validate_trade_fields=validate_trade_fields, map_trade=map_simple_trade)
    persist_trade = partial(persist_trade_sqlalchemy, connection_str=connection_str, logger=logger)

    process_trades(get_trade_data, parse_trade_data, persist_trade)