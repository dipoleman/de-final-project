import datetime

rows = [[1,
         'John',
         'Doe',
         2,
         'john.doe@example.com',
         datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
         datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)],
        [2,
         'Jane',
         'Smith',
         6,
         'jane.smith@example.com',
         datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
         datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]]

columns = [{'column_attrnum': 1,
            'format': 0,
            'name': 'staff_id',
            'table_oid': 16466,
            'type_modifier': -1,
            'type_oid': 23,
            'type_size': 4},
           {'column_attrnum': 2,
            'format': 0,
            'name': 'first_name',
            'table_oid': 16466,
            'type_modifier': -1,
            'type_oid': 25,
            'type_size': -1},
           {'column_attrnum': 3,
            'format': 0,
            'name': 'last_name',
            'table_oid': 16466,
            'type_modifier': -1,
            'type_oid': 25,
            'type_size': -1},
           {'column_attrnum': 4,
            'format': 0,
            'name': 'department_id',
            'table_oid': 16466,
            'type_modifier': -1,
            'type_oid': 23,
            'type_size': 4},
           {'column_attrnum': 5,
            'format': 0,
            'name': 'email_address',
            'table_oid': 16466,
            'type_modifier': -1,
            'type_oid': 25,
            'type_size': -1},
           {'column_attrnum': 6,
            'format': 0,
            'name': 'created_at',
            'table_oid': 16466,
            'type_modifier': 3,
            'type_oid': 1114,
            'type_size': 8},
           {'column_attrnum': 7,
            'format': 0,
            'name': 'last_updated',
            'table_oid': 16466,
            'type_modifier': 3,
            'type_oid': 1114,
            'type_size': 8}]