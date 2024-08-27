import sys
import os
import pytest
import pandas as pd
from datetime import datetime

# Add the directory containing transformations.py to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl')))

from transformations import standardize_room_type, format_phone_number, transform_fact_table, transform_dim_tables

# Test for standardize_room_type function
def test_standardize_room_type():
    assert standardize_room_type('Single Earth') == 'single earth'
    assert standardize_room_type('single-earth') == 'single earth'
    assert standardize_room_type('SINGLE EARTH') == 'single earth'
    assert standardize_room_type('single_earth') == 'single earth'
    assert standardize_room_type(None) == None

# Test for format_phone_number function
def test_format_phone_number():
    assert format_phone_number('08123456789') == '+62-8123456789'
    assert format_phone_number('+62-8123456789') == '+62-8123456789'
    assert format_phone_number('123456789') != '+62-123456789'
    assert format_phone_number(None) == None

# Prepare test data
@pytest.fixture
def test_data():
    return {
        'reservations': pd.DataFrame({
            'id': [1001, 1002],
            'reservation_datetime': ['2024-06-01 12:00:00', '2024-06-02 16:00:00'],
            'check_in_date': ['2024-06-15', '2024-07-01'],
            'check_out_date': ['2024-06-20', '2024-07-05'],
            'status': ['Booked', 'Pending'],
            'hotel_id': [1, 2],
            'booker_id': [1, 2],
            'total_room_price': [500.00, 600.00],
            'voucher_code': ['SUMMER20', 'WINTER15'],
            'total_discount': [20.00, 15.00]
        }),
        'reservation_items': pd.DataFrame({
            'id': [1, 2],
            'reservation_id': [1001, 1002],
            'reservation_datetime': ['2024-06-01 12:00:00', '2024-06-02 16:00:00'],
            'check_in_date': ['2024-06-15', '2024-07-01'],
            'check_out_date': ['2024-06-20', '2024-07-05'],
            'room_type': ['Single', 'Suite'],
            'total_room_price': [300.00, 400.00],
            'total_discount': [10.00, 15.00]
        }),
        'stays': pd.DataFrame({
            'id': [1, 2],
            'date': ['2024-06-16', '2024-07-02'],
            'reference_reservation_id': [1001, 1002],
            'room_id': [1, 3],
            'guest_id': [1, 2]
        }),
        'payments': pd.DataFrame({
            'id': [1, 2],
            'reservation_id': [1001, 1002],
            'payment_method_id': [1, 2],
            'amount': [100.00, 150.00],
            'status': ['Paid', 'Pending'],
            'created_datetime': ['2024-07-01 10:00:00', '2024-07-02 14:00:00'],
            'payment_datetime': ['2024-07-01 10:30:00', None]
        }),
        'hotels': pd.DataFrame({
            'id': [1, 2],
            'name': ['Seaside Resort', 'Mountain Lodge'],
            'type': ['Resort', 'Hotel']
        }),
        'rooms': pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Room 101', 'Room 102', 'Room 201'],
            'room_type': ['Single', 'Double', 'Suite'],
            'floor': [1, 1, 2],
            'hotel_id': [1, 1, 2]
        }),
        'users': pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice Smith', 'Bob Johnson'],
            'birth_date': ['1990-01-15', '1985-02-20'],
            'gender': ['Female', 'Male'],
            'email': ['alice@example.com', 'bob@example.com'],
            'phoneNumber': ['08123456789', '+628123456789']
        }),
        'stay_users': pd.DataFrame({
            'id': [1, 2],
            'stay_id': [1, 2]
        }),
        'payment_methods': pd.DataFrame({
            'id': [1, 2],
            'name': ['Credit Card', 'Bank Transfer'],
            'third_party_id': [1, 2]
        }),
        'payment_third_parties': pd.DataFrame({
            'id': [1, 2],
            'name': ['PayPal', 'Stripe']
        }),
        'campaigns': pd.DataFrame({
            'id': [1, 2],
            'name': ['Summer Sale', 'Winter Wonderland'],
            'description': ['Discounts on summer stays', 'Special offers for winter stays'],
            'cover_pic_url': ['http://example.com/summer_sale.jpg', 'http://example.com/winter_wonderland.jpg']
        }),
        'vouchers': pd.DataFrame({
            'id': [1, 2, 3, 4],
            'campaign_id': [1, 1, 2, 2],
            'code': ['SUMMER20', 'SUMMER30', 'WINTER15', 'WINTER20'],
            'discount_type': [0.20, 0.30, 0.15, 0.20],
            'discount_value': [20.00, 30.00, 15.00, 20.00],
            'visible_from': [pd.Timestamp('2024-06-01 00:00:00')] * 4,
            'visible_to': [pd.Timestamp('2024-08-31 23:59:59')] * 2 + [pd.Timestamp('2024-12-31 23:59:59')] * 2,
            'valid_from': [pd.Timestamp('2024-06-01 00:00:00')] * 4,
            'valid_to': [pd.Timestamp('2024-08-31 23:59:59')] * 2 + [pd.Timestamp('2024-12-31 23:59:59')] * 2,
            'hotel_types': ['["Resort"]', '["Resort"]', '["Hotel"]', '["Hotel"]'],
            'hotel_ids': ['["1", "2"]', '["1"]', '["3"]', '["3"]'],
            'room_types': ['["Single", "Double"]', '["Suite"]', '["Suite"]', '["Single", "Double"]']
        })
    }

# Test for transform_fact_table function
def test_transform_fact_table(test_data):
    fact_table = transform_fact_table(test_data)
    assert fact_table.shape == (2, 17)  # Check if the fact table has the expected number of rows and columns
    assert 'room_type' in fact_table.columns  # Check if 'room_type' column exists
    assert fact_table['room_type'].iloc[0] == 'single'  # Check if room type was standardized

# Test for transform_dim_tables function
def test_transform_dim_tables(test_data):
    dim_tables = transform_dim_tables(test_data)
    assert 'users' in dim_tables
    assert 'rooms' in dim_tables
    assert 'hotels' in dim_tables
    assert dim_tables['users'].shape == (2, 6)  # Check if users table has the expected number of rows and columns
    assert dim_tables['rooms'].shape == (3, 5)  # Check if rooms table has the expected number of rows and columns
    assert dim_tables['hotels'].shape == (2, 3)  # Check if hotels table has the expected number of rows and columns
    assert dim_tables['users']['phoneNumber'].iloc[0] == '+62-8123456789'  # Check if phone numbers were formatted correctly
