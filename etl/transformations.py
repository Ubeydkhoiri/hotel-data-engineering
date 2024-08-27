import pandas as pd
import logging
import re

logger = logging.getLogger(__name__)

def standardize_room_type(room_type):
    if pd.isna(room_type):
        return room_type
    # Normalize the room type
    room_type = room_type.lower().replace('_', ' ').replace('-', ' ')
    room_type = ' '.join(room_type.split())  # Remove extra spaces
    return room_type

def format_phone_number(phone_number):
    if pd.isna(phone_number):
        return phone_number
    phone_number = str(phone_number)
    # Ensure it starts with +62
    if phone_number.startswith('62'):
        phone_number = '+62-' + phone_number[2:]
    elif phone_number.startswith('0'):
        phone_number = '+62-' + phone_number[1:]
    return phone_number

def transform_fact_table(data):
    logger.info("Transforming fact table.")
    
    reservations = data['reservations']
    reservation_items = data['reservation_items']
    stays = data['stays']
    payments = data['payments']
    
    res_items = pd.merge(reservations, reservation_items, left_on='id', right_on='reservation_id', how='left', suffixes=('','_items'))
    
    # Handle cases where reference_reservation_id is missing in reservations
    res_items_stays = pd.merge(res_items, stays, left_on='id', right_on='reference_reservation_id', how='left', suffixes=('','_stays'))
    
    # Standardize the room_type column
    res_items_stays['room_type'] = res_items_stays['room_type'].apply(standardize_room_type)
    
    fact_table = pd.merge(res_items_stays, payments, left_on='id', right_on='reservation_id', how='left', suffixes=('','_payments'))

    fact_table = fact_table[[
        'id','reservation_datetime','check_in_date','check_out_date','status','hotel_id','booker_id','total_room_price',
        'voucher_code','total_discount','room_type','room_id','guest_id','payment_method_id','amount','status_payments',
        'payment_datetime'
    ]]

    logger.info("Fact table transformation complete.")
    return fact_table

def transform_dim_tables(data):
    logger.info("Transforming dimension tables.")
    
    hotels = data['hotels'].drop_duplicates().reset_index(drop=True)
    rooms = data['rooms'].drop_duplicates().reset_index(drop=True)
    users = data['users'].drop_duplicates().reset_index(drop=True)
    stay_users = data['stay_users'].drop_duplicates().reset_index(drop=True)
    payment_methods = data['payment_methods'].drop_duplicates().reset_index(drop=True)
    payment_third_parties = data['payment_third_parties'].drop_duplicates().reset_index(drop=True)
    campaign = data['campaigns'].drop_duplicates().reset_index(drop=True)
    voucher = data['vouchers'].drop_duplicates().reset_index(drop=True)

    # Merge users with stay_users and format the phone number
    users = pd.merge(users, stay_users, on='id', how='left', suffixes=('','_stay')).drop_duplicates().reset_index(drop=True)
    users['phoneNumber'] = users['phoneNumber'].apply(format_phone_number)
    
    users = users[['id','name','birth_date','gender','email','phoneNumber']]

    logger.info("Dimension table transformations complete.")
    return {
        'hotels': hotels,
        'rooms': rooms,
        'users': users,
        'payment_methods': payment_methods,
        'payment_third_parties': payment_third_parties,
        'campaign': campaign,
        'voucher': voucher
    }
