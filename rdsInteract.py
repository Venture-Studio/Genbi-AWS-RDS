#file to upload data through an SQL query, using field names, data types
import boto3
import pandas as pd
import psycopg2
import json
from botocore.exceptions import ClientError

# Load the CSV file into a pandas DataFrame
df = pd.read_csv('./MutualFunds.csv')


def get_secret():
    secret_name = "rds!cluster-5dfdda0f-7553-48a3-ac44-83595bb9fd10"  # Replace with your actual secret name
    region_name = "us-west-2"  # Replace with your AWS region

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

    # Parse and return the secret string
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)  # Convert secret string to a dictionary

# Fetch the credentials from Secrets Manager
credentials = get_secret()

if credentials:
    # Extract the details
    db_host = "database-1-instance-1.c3qm0wkmu4zb.us-west-2.rds.amazonaws.com"
    db_name = "postgres"
    db_user = credentials['username']
    db_pass = credentials['password']
    db_port = credentials.get('port', '5432')  # Default PostgreSQL port

    # Connect to the RDS PostgreSQL database using the credentials
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_pass,
        port=db_port
    )
    cur = conn.cursor()

    # # Insert data into the table row by row
    # for i, row in df.iterrows():
    #     cur.execute("""
    #     INSERT INTO MutualFunds (
    #         fund_symbol, quote_type, region, fund_short_name, fund_long_name, currency, 
    #         initial_investment, subsequent_investment, fund_category, fund_family, exchange_code, 
    #         exchange_name, exchange_timezone, management_name, management_bio, management_start_date, 
    #         total_net_assets, year_to_date_return, day50_moving_average, day200_moving_average, 
    #         week52_high_low_change, week52_high_low_change_perc, week52_high, week52_high_change, 
    #         week52_high_change_perc, week52_low, week52_low_change, week52_low_change_perc, 
    #         investment_strategy, fund_yield, morningstar_overall_rating, morningstar_risk_rating, 
    #         inception_date, last_dividend, last_cap_gain, annual_holdings_turnover, investment_type, 
    #         size_type, fund_annual_report_net_expense_ratio, category_annual_report_net_expense_ratio, 
    #         fund_prospectus_net_expense_ratio, fund_prospectus_gross_expense_ratio, fund_max_12b1_fee, 
    #         fund_max_front_end_sales_load, category_max_front_end_sales_load, fund_max_deferred_sales_load, 
    #         category_max_deferred_sales_load, fund_year3_expense_projection, fund_year5_expense_projection, 
    #         fund_year10_expense_projection, asset_cash, asset_stocks, asset_bonds, asset_others, 
    #         asset_preferred, asset_convertible, fund_sector_basic_materials, fund_sector_communication_services, 
    #         fund_sector_consumer_cyclical, fund_sector_consumer_defensive, fund_sector_energy, 
    #         fund_sector_financial_services, fund_sector_healthcare, fund_sector_industrials, 
    #         fund_sector_real_estate, fund_sector_technology, fund_sector_utilities, fund_price_book_ratio, 
    #         category_price_book_ratio, fund_price_cashflow_ratio, category_price_cashflow_ratio, 
    #         fund_price_earning_ratio, category_price_earning_ratio, fund_price_sales_ratio, 
    #         category_price_sales_ratio, fund_median_market_cap, category_median_market_cap, 
    #         fund_year3_earnings_growth, category_year3_earnings_growth, fund_bond_maturity, 
    #         category_bond_maturity, fund_bond_duration, category_bond_duration, fund_bonds_us_government, 
    #         fund_bonds_aaa, fund_bonds_aa, fund_bonds_a, fund_bonds_bbb, fund_bonds_bb, fund_bonds_b, 
    #         fund_bonds_below_b, fund_bonds_others, top10_holdings, top10_holdings_total_assets, 
    #         morningstar_return_rating, returns_as_of_date
    #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    #             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    #             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    #             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #     """, (
    #         row['fund_symbol'], row['quote_type'], row['region'], row['fund_short_name'], row['fund_long_name'], 
    #         row['currency'], row['initial_investment'], row['subsequent_investment'], row['fund_category'], 
    #         row['fund_family'], row['exchange_code'], row['exchange_name'], row['exchange_timezone'], 
    #         row['management_name'], row['management_bio'], row['management_start_date'], row['total_net_assets'], 
    #         row['year_to_date_return'], row['day50_moving_average'], row['day200_moving_average'], 
    #         row['week52_high_low_change'], row['week52_high_low_change_perc'], row['week52_high'], 
    #         row['week52_high_change'], row['week52_high_change_perc'], row['week52_low'], row['week52_low_change'], 
    #         row['week52_low_change_perc'], row['investment_strategy'], row['fund_yield'], row['morningstar_overall_rating'], 
    #         row['morningstar_risk_rating'], row['inception_date'], row['last_dividend'], row['last_cap_gain'], 
    #         row['annual_holdings_turnover'], row['investment_type'], row['size_type'], row['fund_annual_report_net_expense_ratio'], 
    #         row['category_annual_report_net_expense_ratio'], row['fund_prospectus_net_expense_ratio'], 
    #         row['fund_prospectus_gross_expense_ratio'], row['fund_max_12b1_fee'], row['fund_max_front_end_sales_load'], 
    #         row['category_max_front_end_sales_load'], row['fund_max_deferred_sales_load'], row['category_max_deferred_sales_load'], 
    #         row['fund_year3_expense_projection'], row['fund_year5_expense_projection'], row['fund_year10_expense_projection'], 
    #         row['asset_cash'], row['asset_stocks'], row['asset_bonds'], row['asset_others'], row['asset_preferred'], 
    #         row['asset_convertible'], row['fund_sector_basic_materials'], row['fund_sector_communication_services'], 
    #         row['fund_sector_consumer_cyclical'], row['fund_sector_consumer_defensive'], row['fund_sector_energy'], 
    #         row['fund_sector_financial_services'], row['fund_sector_healthcare'], row['fund_sector_industrials'], 
    #         row['fund_sector_real_estate'], row['fund_sector_technology'], row['fund_sector_utilities'], 
    #         row['fund_price_book_ratio'], row['category_price_book_ratio'], row['fund_price_cashflow_ratio'], 
    #         row['category_price_cashflow_ratio'], row['fund_price_earning_ratio'], row['category_price_earning_ratio'], 
    #         row['fund_price_sales_ratio'], row['category_price_sales_ratio'], row['fund_median_market_cap'], 
    #         row['category_median_market_cap'], row['fund_year3_earnings_growth'], row['category_year3_earnings_growth'], 
    #         row['fund_bond_maturity'], row['category_bond_maturity'], row['fund_bond_duration'], row['category_bond_duration'], 
    #         row['fund_bonds_us_government'], row['fund_bonds_aaa'], row['fund_bonds_aa'], row['fund_bonds_a'], 
    #         row['fund_bonds_bbb'], row['fund_bonds_bb'], row['fund_bonds_b'], row['fund_bonds_below_b'], row['fund_bonds_others'], 
    #         row['top10_holdings'], row['top10_holdings_total_assets'], row['morningstar_return_rating'], row['returns_as_of_date']
    #     ))


    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()

    print("Connected and data uploaded successfully.")
else:
    print("Failed to retrieve database credentials.")
