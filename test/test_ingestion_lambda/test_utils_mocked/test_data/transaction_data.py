import datetime

rows = [[1,
         'PURCHASE',
         None,
         2,
         datetime.datetime(2022, 11, 3, 14, 20, 52, 186000),
         datetime.datetime(2022, 11, 3, 14, 20, 52, 186000)],
        [2,
         'PURCHASE',
         None,
         3,
         datetime.datetime(2022, 11, 3, 14, 20, 52, 187000),
         datetime.datetime(2022, 11, 3, 14, 20, 52, 187000)],
        [3,
         'SALE',
         1,
         None,
         datetime.datetime(2022, 11, 3, 14, 20, 52, 186000),
         datetime.datetime(2022, 11, 3, 14, 20, 52, 186000)],
        [4,
         'PURCHASE',
         None,
         1,
         datetime.datetime(2022, 11, 3, 14, 20, 52, 187000),
         datetime.datetime(2022, 11, 3, 14, 20, 52, 187000)],
        [5,
         'PURCHASE',
         None,
         4,
         datetime.datetime(2022, 11, 3, 14, 20, 52, 187000),
         datetime.datetime(2022, 11, 3, 14, 20, 52, 187000)]]

columns = [{'column_attrnum': 1,
           'format': 0,
            'name': 'transaction_id',
            'table_oid': 16547,
            'type_modifier': -1,
            'type_oid': 23,
            'type_size': 4},
           {'column_attrnum': 2,
           'format': 0,
            'name': 'transaction_type',
            'table_oid': 16547,
            'type_modifier': -1,
            'type_oid': 16426,
            'type_size': 4},
           {'column_attrnum': 3,
           'format': 0,
            'name': 'sales_order_id',
            'table_oid': 16547,
            'type_modifier': -1,
            'type_oid': 23,
            'type_size': 4},
           {'column_attrnum': 4,
           'format': 0,
            'name': 'purchase_order_id',
            'table_oid': 16547,
            'type_modifier': -1,
            'type_oid': 23,
            'type_size': 4},
           {'column_attrnum': 5,
           'format': 0,
            'name': 'created_at',
            'table_oid': 16547,
            'type_modifier': 3,
            'type_oid': 1114,
            'type_size': 8},
           {'column_attrnum': 6,
           'format': 0,
            'name': 'last_updated',
            'table_oid': 16547,
            'type_modifier': 3,
            'type_oid': 1114,
            'type_size': 8}]
