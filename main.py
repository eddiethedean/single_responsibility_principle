from classes_to_functions import process_simple_trades


if __name__ == '__main__':
    connection_str = 'sqlite:///trades.db'
    table_name = 'trade_table'
    trades = [
        'USA,100,45.98',
        'EUR,105,56.78',
        'USA,300,50.00',
        'USA,160,7.66',
        'EUR,550,900.80',
        'USA,170,36.29',
    ]
    
    process_simple_trades(trades, connection_str, table_name)