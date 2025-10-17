import logging
import requests
from datetime import datetime, timedelta
import pandas as pd
from keboola.component.base import ComponentBase
import time
import gzip
import io
import json
import xml.etree.ElementTree as ET
import warnings
import re
import random
import inspect

# Suppress FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Configuration variables for API access and other settingss
KEY_REFRESH_TOKEN = '#refresh_token'
KEY_APP_ID = '#app_id'
KEY_CLIENT_SECRET_ID = '#client_secret_id'
KEY_MARKETPLACE_ID = 'marketplace_id'  # FBM marketplace ID for orders/returns/finances
KEY_DATE_RANGE = 'date_range'
KEY_REFRESH_TOKEN_ADS = '#refresh_token_ads'
KEY_APP_ID_ADS = '#app_id_ads'
KEY_CLIENT_SECRET_ID_ADS = '#client_secret_id_ads'
KEY_STORES = 'stores'

# Amazon marketplaces configuration keys
KEY_MARKETPLACES = 'marketplaces'
KEY_MARKETPLACES_MARKETPLACE_IDS = 'marketplace_ids'  # list of Amazon marketplaces
KEY_MARKETPLACES_STRATEGIC_PRODUCTS = 'strategic_products' 

# Execution flags
KEY_RUN_INVENTORY = 'run_inventory'
KEY_RUN_INVENTORY_PLANNING = 'run_inventory_planning'
KEY_RUN_ORDERS = 'run_orders'
KEY_RUN_RETURNS = 'run_returns'
KEY_RUN_FINANCES = 'run_finances'
KEY_RUN_ADS = 'run_ads'
KEY_RUN_LEDGER = 'run_ledger'
KEY_RUN_STRATEGIC_PRODUCTS = 'run_startegic_products'

class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.setup_logging()
        self.all_ads_data = pd.DataFrame()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def get_date_days_ago(days, date_format='%Y-%m-%dT%H:%M:%S.%fZ'):
        # Return a formatted string of the datetime days ago from now
        date = datetime.utcnow() - timedelta(days=days)
        return date.strftime(date_format)

    @staticmethod
    def camel_to_snake(name: str) -> str:
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    @staticmethod
    def shorten_column(name: str) -> str:
        token = re.split(r'[._]', name)[-1]
        return Component.camel_to_snake(token)

    def run(self):
        params = self.configuration.parameters
        # Seller Central credentials
        self.refresh_token = params.get(KEY_REFRESH_TOKEN)
        self.app_id = params.get(KEY_APP_ID)
        self.client_secret_id = params.get(KEY_CLIENT_SECRET_ID)
        # Marketplace and date range
        self.marketplace_id = params.get(KEY_MARKETPLACE_ID)
        self.date_range = int(params.get(KEY_DATE_RANGE, 7))
        exec_cfg = params.get('execution', {})
        self.run_inventory = exec_cfg.get(KEY_RUN_INVENTORY, True)
        self.run_inventory_planning = exec_cfg.get(KEY_RUN_INVENTORY_PLANNING, True)
        self.run_orders = exec_cfg.get(KEY_RUN_ORDERS, True)
        self.run_returns = exec_cfg.get(KEY_RUN_RETURNS, True)
        self.run_finances = exec_cfg.get(KEY_RUN_FINANCES, True)
        self.run_ads = exec_cfg.get(KEY_RUN_ADS, True)
        self.run_ledger = exec_cfg.get(KEY_RUN_LEDGER, True)
        self.run_startegic_products = exec_cfg.get(KEY_RUN_STRATEGIC_PRODUCTS, True)
        # Ads credentials
        self.refresh_token_ads = params.get(KEY_REFRESH_TOKEN_ADS)
        self.app_id_ads = params.get(KEY_APP_ID_ADS)
        self.client_secret_id_ads = params.get(KEY_CLIENT_SECRET_ID_ADS)
        self.stores = params.get(KEY_STORES, [])
        # Marketplaces
        self.marketplaces_cfg = params.get(KEY_MARKETPLACES, [])
        self.marketplace_ids = [m['marketplace_id'] for m in self.marketplaces_cfg]

        # Refresh tokens
        self.refresh_amazon_token()
        self.refresh_amazon_ads_token()

        if not getattr(self, 'access_token', None):
            logging.error('Failed to refresh Seller Central token.')
            return

        # Core flows
        if self.run_inventory:
            logging.info('Executing FBA inventory snapshot...')
            self.handle_inventory()
        if self.run_inventory_planning:
            logging.info('Executing FBA inventory planning snapshot...')
            self.handle_inventory_planning()
        if self.run_orders:
            logging.info('Executing FBM orders...')
            self.handle_orders()
        if self.run_returns:
            logging.info('Executing FBM returns...')
            self.handle_returns()
        if self.run_finances:
            logging.info('Executing FBM finances...')
            self.handle_finances()
        if self.run_startegic_products:
            logging.info('Executing Amazon strategic products...')
            self.handle_strategic_products()
        
        # FBA ledger reports (detail and summary) need correct date ordering
        if self.run_ledger:
            logging.info('Generating FBA ledger detail and summary view reports...')
            start_dt = datetime.utcnow() - timedelta(days=self.date_range)
            end_dt = datetime.utcnow()
            
            all_details = []
            all_summaries = []

            for mp in self.marketplace_ids:
                detail_id = self.create_ledger_report(start_dt, end_dt, 'GET_LEDGER_DETAIL_VIEW_DATA', marketplace_id=mp)
                
                if detail_id:
                    df_detail = self.poll_report_status_and_download(detail_id, pd.DataFrame(), f'inventory_ledger_detail_{mp}.csv', False, [])
                    
                    if not df_detail.empty:
                        df_detail['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                        
                        logging.info(f"Original ledger detail rows for {mp}: {len(df_detail)}")
                        deduplicated_df = df_detail.drop_duplicates(keep='first')
                        logging.info(f"After deduplication, ledger detail rows for {mp}: {len(deduplicated_df)}")
                        
                        all_details.append(deduplicated_df)

                summary_id = self.create_ledger_report(start_dt, end_dt, 'GET_LEDGER_SUMMARY_VIEW_DATA', marketplace_id=mp)

                if summary_id:
                    df_summary = self.poll_report_status_and_download(summary_id, pd.DataFrame(), f'inventory_ledger_summary_{mp}.csv', False, [])
                    
                    if not df_summary.empty:
                        df_summary['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                        all_summaries.append(df_summary)

            # After the loop, combine and process the aggregated data
            if all_details:
                final_detail_df = pd.concat(all_details, ignore_index=True)
                final_detail_df.drop_duplicates(keep='first', inplace=True) # Maybe redundant, but at least we will be safe
                logging.info(f"Total processed ledger detail rows from all marketplaces after deduplication: {len(final_detail_df)}")
                self.process_data(final_detail_df, 'inventory_ledger_detail.csv', [])
            
            if all_summaries:
                final_summary_df = pd.concat(all_summaries, ignore_index=True)
                final_summary_df.drop_duplicates(keep='first', inplace=True) # Maybe redundant, but at least we will be safe
                logging.info(f"Total processed ledger summary rows from all marketplaces after deduplication: {len(final_summary_df)}")
                self.process_data(final_summary_df, 'inventory_ledger_summary.csv', [])

        # Ads reports flow
        if self.run_ads and getattr(self, 'ads_access_token', None):
            logging.info('Executing Amazon Ads reports...')
            for store in self.stores:
                self.create_and_download_ads_report(store['scope'], store['name'], 'SPONSORED_PRODUCTS')
                self.create_and_download_ads_report(store['scope'], store['name'], 'SPONSORED_BRANDS')
                self.create_and_download_ads_report(store['scope'], store['name'], 'SPONSORED_DISPLAY')
            self.save_ads_data_to_csv()
        elif self.run_ads:
            logging.error('Failed to refresh Ads token.')
        else:
            logging.info('Skipping Amazon Ads reports as per configuration.')

    def handle_orders(self):
        order_segments = self.split_date_range(self.date_range, 15)
        
        output_file_name = 'orders.csv'
        primary_keys = ['amazon-order-id', 'sku', 'asin']
        table_path = self.create_out_table_definition(
            output_file_name, incremental=True, primary_key=primary_keys
        ).full_path
        
        is_first_chunk = True

        for mp in self.marketplace_ids:
            for start_date, end_date in order_segments:
                logging.info(f"Creating report for marketplace: {mp}")
                report_id = self.create_report(
                    start_date, end_date, "GET_XML_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL", mp)
                
                if report_id:
                    report_generator = self.poll_report_status_and_download(
                        report_id, None, 'orders.csv', is_xml=True, primary_keys=primary_keys
                    )
                    
                    for df_chunk in report_generator:
                        if not df_chunk.empty:
                            df_chunk.to_csv(
                                table_path, 
                                mode='a',
                                header=is_first_chunk, 
                                index=False
                            )
                            is_first_chunk = False
            break
        
        if is_first_chunk:
            logging.warning("No order data was processed, skipping deduplication.")
            return

    def handle_inventory(self):
            """
            Fetch and process daily FBA inventory for each configured marketplace.
            Handles pagination via NextToken to retrieve all pages.
            """
            logging.info("Fetching daily FBA inventory for marketplaces: %s", self.marketplace_ids)
            all_dfs = []
            for mp in self.marketplace_ids:
                logging.info("Starting inventory fetch for marketplace: %s", mp)
                next_token = None
                while True:
                    url = "https://sellingpartnerapi-eu.amazon.com/fba/inventory/v1/summaries"
                    headers = {
                        'x-amz-access-token': self.access_token,
                        'Content-Type': 'application/json'
                    }
                    params = {
                        'marketplaceIds': mp,
                        'granularityType': 'Marketplace',
                        'granularityId': mp,
                        'details': 'true'
                    }
                    if next_token:
                        params['nextToken'] = next_token
                    response = self.controlled_request('get', url, headers=headers, params=params)
                    if not response or response.status_code != 200:
                        logging.error(
                            "FBA inventory fetch failed for %s: %s",
                            mp,
                            response.text if response else 'No response'
                        )
                        break

                    data = response.json()
                    summaries = data.get('payload', {}).get('inventorySummaries', [])
                    if summaries:
                        df = pd.json_normalize(summaries,sep='_')
                        df.rename(columns=lambda x: self.shorten_column(x), inplace=True)
                        df['marketplace_id'] = mp
                        df['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                        all_dfs.append(df)

                    # Check for pagination
                    next_token = data.get('pagination', {}).get('nextToken')
                    if not next_token:
                        logging.info("Completed inventory pages for %s", mp)
                        break

            if all_dfs:
                result = pd.concat(all_dfs, ignore_index=True)
                self.process_data(
                    result,
                    'inventory.csv',
                    primary_keys=['seller_sku', 'asin', 'marketplace_id']
                )
                logging.info("Total FBA inventory records: %d", len(result))
            else:
                logging.warning("No FBA inventory data fetched.")

    def handle_inventory_planning(self):
        """
        Fetch and process FBA Inventory Planning data for each configured marketplace.
        Uses SP-API report generation and download.
        """
        logging.info("Fetching FBA Inventory Planning data for marketplaces: %s", self.marketplace_ids)
        all_dfs = []

        for mp in self.marketplace_ids:
            logging.info("Starting Inventory Planning report for marketplace: %s", mp)
            planning_segments = self.split_date_range(self.date_range, 30)

            for start_date, end_date in planning_segments:
                report_id = self.create_report(
                    start_date,
                    end_date,
                    "GET_FBA_INVENTORY_PLANNING_DATA", 
                    marketplace_id=mp
                )

                if report_id:
                    df = self.poll_report_status_and_download(
                        report_id,
                        pd.DataFrame(),
                        'inventory_planning.csv',
                        is_xml=False,
                        primary_keys=['sku', 'asin']
                    )

                    if not df.empty:
                        df.rename(columns=lambda x: self.shorten_column(x), inplace=True)
                        df['marketplace_id'] = mp
                        df['extracted_at'] = datetime.utcnow().isoformat() + 'Z'
                        all_dfs.append(df)
                    else:
                        logging.warning(f"No data for planning report in marketplace {mp} from {start_date} to {end_date}")
                else:
                    logging.warning(f"Failed to create planning report for marketplace {mp}")

                report_id = None

        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            logging.info("Total inventory planning records across marketplaces: %d", len(combined))
            self.process_data(combined, 'inventory_planning.csv', ['snapshot-date', 'sku', 'asin', 'marketplace_id'])
        else:
            logging.warning("No FBA Inventory Planning data fetched from any marketplace.")


    def handle_returns(self):
        # Fetch and process return data
        self.all_returns_data = pd.DataFrame()
        return_segments = self.split_date_range(self.date_range, 50)
        all_returns_data = pd.DataFrame()
        for mp in self.marketplace_ids:
            for start_date, end_date in return_segments:
                report_id = self.create_report(
                    start_date, end_date, "GET_XML_RETURNS_DATA_BY_RETURN_DATE", mp)
                if report_id:
                    temp_data = self.poll_report_status_and_download(report_id, pd.DataFrame(
                    ), 'returns.csv', is_xml=True, primary_keys=['return-id', 'order-id'])
                    if not temp_data.empty:
                        all_returns_data = pd.concat(
                            [all_returns_data, temp_data], ignore_index=True)
        # Log before processing data
        logging.info(
            f"Number of records to process for returns: {len(all_returns_data)}")
        if not all_returns_data.empty:
            # In case of endpoint not being marketplace-sensitive
            all_returns_data.drop_duplicates(keep='first', inplace=True)
            self.process_data(all_returns_data, 'returns.csv', [
                              'return-id', 'order-id'])
        else:
            logging.warning("No return data to process.")

    def handle_finances(self):
        # Fetch and process financial data
        self.all_financial_data = pd.DataFrame()
        financial_data = self.fetch_financial_events()
        # Initialize the dataframe to hold all data.
        all_financial_data = pd.DataFrame()

        while financial_data:
            processed_data = self.process_financial_data(financial_data)
            # Ensure data is concatenated correctly.
            all_financial_data = pd.concat(
                [all_financial_data, processed_data], ignore_index=True)

            next_token = financial_data.get('payload', {}).get('NextToken')
            if next_token:
                logging.info(
                    f"Fetching next page of financial events with NextToken.")
                financial_data = self.fetch_financial_events(next_token)
            else:
                break

        # Only write to CSV after all data is gathered.
        if not all_financial_data.empty:
            self.process_data(all_financial_data, 'finance.csv', [
                              'amazon_order_id', 'seller_sku', 'order_item_id', "posted_date"])
        else:
            logging.info("No financial data to process.")

    def listings_extract(self, table_path: str) -> dict:
        """
        Extracting ASIN for each listing from source table
        """
        data = pd.read_csv(table_path)
        df_filtered = data.dropna(subset=['products_asin'])
        distinct_asin_list = list(df_filtered['products_asin'].unique())

        if len(distinct_asin_list) > 1:
            logging.info("Country words were retrieved.")
            return distinct_asin_list
        else:
            raise Exception("Unable to retrieve country words")

    def handle_strategic_products(self):
        # Fetch and process catalog item data
        all_dfs = []

        # Fetch input table with ASIN for Amazon products
        input_tables = self.get_input_tables_definitions()

        strategic_products = self.listings_extract(table_path=input_tables[0].full_path)

        # Outer loop for each marketplace
        for marketplace_data in self.marketplaces_cfg:
            mp_id = marketplace_data['marketplace_id']

            if not strategic_products:
                continue

            for i in range(0, len(strategic_products), 20):
                asin_batch = strategic_products[i:i + 20]
                
                next_token = None
                while True:
                    url = "https://sellingpartnerapi-eu.amazon.com/catalog/2022-04-01/items"
                    headers = {
                        'x-amz-access-token': self.access_token,
                        'Content-Type': 'application/json'
                    }
                    params = {
                        'marketplaceIds': mp_id,
                        'keywords': ','.join(asin_batch),
                        'includedData': 'salesRanks'
                    }
                    if next_token:
                        params['pageToken'] = next_token

                    response = self.controlled_request('get', url, headers=headers, params=params)

                    if not response or response.status_code != 200:
                        logging.error(
                            f"Catalog item fetch failed for batch starting with {asin_batch[0]} in {mp_id}: {response.text}" if response else 'No response'
                        )
                        continue
                
                    data = response.json()
                    extracted_time = datetime.utcnow().isoformat() + 'Z'
                    
                    for item in data.get('items', []):
                        asin = item.get('asin')
                        dfs_for_asin = []

                        if item.get('salesRanks'):
                            df_class = pd.json_normalize(
                                item['salesRanks'],
                                record_path=['classificationRanks'],
                                meta=['marketplaceId']
                            )
                            if not df_class.empty:
                                df_class['rank_type'] = 'classification'
                                dfs_for_asin.append(df_class)

                            df_display = pd.json_normalize(
                                item['salesRanks'],
                                record_path=['displayGroupRanks'],
                                meta=['marketplaceId']
                            )
                            if not df_display.empty:
                                df_display['rank_type'] = 'display_group'
                                dfs_for_asin.append(df_display)

                        if dfs_for_asin:
                            asin_df = pd.concat(dfs_for_asin, ignore_index=True)
                            asin_df['asin'] = asin
                            asin_df['extracted_at'] = extracted_time
                            all_dfs.append(asin_df)
                            logging.info(f"Successfully processed ranks for ASIN {asin} in {mp_id}.")
                        else:
                            logging.warning(f"No sales rank data found for ASIN {asin} in {mp_id}.")

                    pagination_data = data.get('pagination', {})
                    next_token = pagination_data.get('nextToken')

                    if not next_token:
                        break

        # Combine and save the final results
        cols_order = ['asin', 'marketplaceId', 'rank_type', 'title', 'rank', 'link', 'classificationId', 'websiteDisplayGroup', 'extracted_at']
        if all_dfs:
            result = pd.concat(all_dfs, ignore_index=True)
            # Reorder columns for clarity
            result = result.reindex(columns=[col for col in cols_order if col in result.columns])
            
            logging.info(f"Total strategic product rank records processed: {len(result)}")
        else:
            result = pd.DataFrame(columns=cols_order)
            logging.warning("No strategic product rank data was fetched.")

        self.process_data(
                result,
                'amazon_strategic_products_rank.csv',
                primary_keys=['asin', 'marketplaceId', 'rank_type', 'title', 'extracted_at'], 
                process_empty=True
            )

    def refresh_amazon_token(self):
        # Refresh the Amazon API token
        logging.info("Attempting to refresh the Amazon token.")
        url = "https://api.amazon.com/auth/o2/token"
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.app_id,
            'client_secret': self.client_secret_id
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(url, data=payload, headers=headers)
        if response.ok:
            self.access_token = response.json().get("access_token")
            logging.info("Token refreshed successfully.")
        else:
            logging.error("Failed to refresh token: %s", response.text)

    def refresh_amazon_ads_token(self):
        # Refresh the Amazon Ads API token
        logging.info("Attempting to refresh the Amazon Ads token.")
        url = "https://api.amazon.com/auth/o2/token"
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token_ads,
            'client_id': self.app_id_ads,
            'client_secret': self.client_secret_id_ads
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(url, data=payload, headers=headers)
        if response.ok:
            self.ads_access_token = response.json().get("access_token")
            logging.info("Ads token refreshed successfully.")
        else:
            logging.error("Failed to refresh ads token: %s", response.text)

    def split_date_range(self, total_days, segment_length):
        # Split the specified date range into segments for processing
        logging.info("Splitting the date range into segments.")
        segments = []
        start_date = datetime.utcnow() - timedelta(minutes=5)
        while total_days > 0:
            current_segment_length = min(segment_length, total_days)
            end_date = start_date - timedelta(days=current_segment_length)
            segments.append((start_date, end_date))
            start_date = end_date
            total_days -= current_segment_length
        return segments

    def create_report(self, start_date, end_date, report_type, marketplace_id=None):
        # Request a new report from Amazon SP-API
        logging.info("Creating %s report from %s to %s for marketplace %s",
                    report_type, start_date, end_date, marketplace_id)
        url = "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports"
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': self.access_token
        }
        payload = json.dumps({
            "marketplaceIds": [marketplace_id],
            "reportType": report_type,
            "dataStartTime": end_date.isoformat(timespec='milliseconds') + 'Z',
            "dataEndTime": start_date.isoformat(timespec='milliseconds') + 'Z'
        })

        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 202:
            report_id = response.json().get('reportId')
            logging.info("Report created successfully with ID: %s", report_id)
            return report_id
        else:
            logging.error("Failed to create report: %s", response.text)
            return None

    def create_ledger_report(self, start_date, end_date, report_type, marketplace_id):
            logging.info(f"Creating {report_type} ledger report from {start_date} to {end_date}")
            url = "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports"
            headers = {'Content-Type':'application/json','x-amz-access-token':self.access_token}
            payload = {
                'marketplaceIds':[marketplace_id],
                'reportType':report_type,
                'dataStartTime':start_date.isoformat(timespec='milliseconds')+'Z',
                'dataEndTime':end_date.isoformat(timespec='milliseconds')+'Z'
            }
            if report_type=='GET_LEDGER_SUMMARY_VIEW_DATA':
                payload['reportOptions']={
                    'aggregatedByTimePeriod':'DAILY',
                    'aggregateByLocation':'COUNTRY'
                }
            resp=requests.post(url, headers=headers, data=json.dumps(payload))
            if resp.status_code==202:
                return resp.json()['reportId']
            logging.error(f"Failed to create ledger report: {resp.text}")
            return None

    def poll_report_status_and_download(self, report_id, data_frame, file_name, is_xml, primary_keys):
        # Check report status and download when ready
        logging.info(f"Polling report status for ID {report_id}.")
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/{report_id}"
        headers = {'x-amz-access-token': self.access_token,
                   'Content-Type': 'application/json'}
        while True:
            response = self.controlled_request('get', url, headers=headers)
            if response and response.status_code == 200:
                status = response.json().get('processingStatus')
                logging.info("Report status: %s", status)
                if status == 'DONE':
                    document_id = response.json().get('reportDocumentId')
                    data_frame = self.download_report(
                        document_id, data_frame, file_name, is_xml, primary_keys)
                    if inspect.isgenerator(data_frame):
                        logging.info(f"Data stream from report {report_id} is ready for processing.")
                    else:
                        logging.info(
                            f"Data from report {report_id} loaded, records: {len(data_frame)}")
                    return data_frame # Return the updated dataframe or generator
                elif status in ['CANCELLED', 'FATAL']:
                    logging.error(
                        "Report processing ended with status: %s", status)
                    break
                else:
                    logging.info("Waiting before the next status check...")
                    time.sleep(10)
            else:
                logging.error(
                    "Failed to poll report status: %s", response.text)
                break
        return [] # Return an empty list on failure, which works safely with a 'for' loop

    def download_report(self, document_id, data_frame, file_name, is_xml, primary_keys):
        # Download the report document from Amazon SP-API
        logging.info(f"Downloading report document ID: {document_id}.")
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/documents/{document_id}"
        headers = {'x-amz-access-token': self.access_token,
                   'Content-Type': 'application/json'}
        response = self.controlled_request('get', url, headers=headers)
        if response and response.status_code == 200:
            document_url = response.json().get('url')
            return self.process_document(document_url, response.json().get('compressionAlgorithm', ''), is_xml, file_name)
        else:
            logging.error(f"Failed to download document: {response.text}")
            # Ensure this returns an empty DataFrame on failure.
            return pd.DataFrame()

    def process_document(self, document_url, compression_algorithm, is_xml, file_name):
        # Process the document after downloading, convert from XML/CSV as needed
        response = self.controlled_request('get', document_url)
        if response and response.status_code == 200:
            content = gzip.decompress(
                response.content) if compression_algorithm == 'GZIP' else response.content
            if is_xml:
                if file_name == 'orders.csv':
                    data_frame = self.parse_all_orders_xml_report(content)
                else:
                    data_frame = self.parse_xml_data(content)
            else:
                data_frame = pd.read_csv(io.StringIO(
                    content.decode('utf-8')), delimiter='\t')
            return data_frame
        else:
            logging.error("Failed to download or process document.")
            return pd.DataFrame()

    def parse_xml_data(self, xml_data):
        # Parse XML and extract data into a DataFrame
        logging.info("Starting XML data parsing.")
        """ Parse XML and extract data into a DataFrame. """
        root = ET.fromstring(xml_data)
        all_records = []

        # Iterate over each return_detail element in the XML
        for return_detail in root.findall('.//return_details'):
            item_detail = return_detail.find('.//item_details')

            # Extracting fields from item_details and return_details
            record = {
                'item_name': item_detail.findtext('item_name', ''),
                'asin': item_detail.findtext('asin', ''),
                'return_reason_code': item_detail.findtext('return_reason_code', ''),
                'merchant_sku': item_detail.findtext('merchant_sku', ''),
                'in_policy': item_detail.findtext('in_policy', ''),
                'return_quantity': item_detail.findtext('return_quantity', ''),
                'resolution': item_detail.findtext('resolution', ''),
                'category': item_detail.findtext('category', ''),
                'refund_amount': item_detail.findtext('refund_amount', ''),
                'order_id': return_detail.findtext('order_id', ''),
                'order_date': return_detail.findtext('order_date', ''),
                'amazon_rma_id': return_detail.findtext('amazon_rma_id', ''),
                'return_request_date': return_detail.findtext('return_request_date', ''),
                'return_request_status': return_detail.findtext('return_request_status', ''),
                'a_to_z_claim': return_detail.findtext('a_to_z_claim', ''),
                'is_prime': return_detail.findtext('is_prime', ''),
                'label_cost': return_detail.find('.//label_details/label_cost').text if return_detail.find('.//label_details/label_cost') is not None else '',
                'label_type': return_detail.find('.//label_details/label_type').text if return_detail.find('.//label_details/label_type') is not None else '',
                'label_to_be_paid_by': return_detail.findtext('label_to_be_paid_by', ''),
                'return_type': return_detail.findtext('return_type', ''),
                'order_amount': return_detail.findtext('order_amount', ''),
                'order_quantity': return_detail.findtext('order_quantity', '')
            }
            all_records.append(record)
        logging.info("Completed parsing XML data.")
        return pd.DataFrame(all_records)

    def parse_all_orders_xml_report(self, xml_data):
        """
        Parses XML data from an All Orders report using a memory-efficient
        stream parser (iterparse) and yields the data in manageable chunks.
        """
        CHUNK_SIZE = 2000

        # Helper functions are unchanged
        def get_text_from_node(node, path, default=''):
            if node is None:
                return default
            found_node = node.find(path)
            return found_node.text.strip() if found_node is not None and found_node.text else default

        def get_price_component(item_price_node, component_type, default=0.0):
            if item_price_node is None:
                return default
            for component in item_price_node.findall('Component'):
                if get_text_from_node(component, 'Type') == component_type:
                    amount_node = component.find('Amount')
                    return float(amount_node.text) if amount_node is not None and amount_node.text else default
            return default

        try:
            chunk_items = []
            context = io.BytesIO(xml_data)

            for event, elem in ET.iterparse(context, events=('end',)):
                if elem.tag == 'Message':
                    order = elem.find('Order')
                    if order is None:
                        elem.clear()
                        continue

                    fulfillment_data = order.find('FulfillmentData')
                    address = fulfillment_data.find('Address') if fulfillment_data is not None else None
                    
                    order_details = {
                        'amazon_order_id': get_text_from_node(order, 'AmazonOrderID'),
                        'merchant_order_id': get_text_from_node(order, 'MerchantOrderID'),
                        'purchase_date': get_text_from_node(order, 'PurchaseDate'),
                        'last_updated_date': get_text_from_node(order, 'LastUpdatedDate'),
                        'order_status': get_text_from_node(order, 'OrderStatus'),
                        'sales_channel': get_text_from_node(order, 'SalesChannel'),
                        'fulfillment_channel': get_text_from_node(fulfillment_data, 'FulfillmentChannel'),
                        'ship_service_level': get_text_from_node(fulfillment_data, 'ShipServiceLevel'),
                        'address_type': get_text_from_node(address, 'AddressType', default=get_text_from_node(order, 'AddressType')),
                        'ship_city': get_text_from_node(address, 'City'),
                        'ship_state': get_text_from_node(address, 'State'),
                        'ship_postal_code': get_text_from_node(address, 'PostalCode'),
                        'ship_country': get_text_from_node(address, 'Country'),
                        'is_business_order': get_text_from_node(order, 'IsBusinessOrder'),
                        'payment_method_details': get_text_from_node(order, 'PaymentMethodDetails'),
                        'buyer_tax_registration_country': get_text_from_node(order, 'BuyerTaxRegistrationCountry'),
                        'buyer_tax_registration_type': get_text_from_node(order, 'BuyerTaxRegistrationType'),
                        'purchase_order_number': get_text_from_node(order, 'PurchaseOrderNumber'),
                        'is_replacement_order': get_text_from_node(order, 'IsReplacementOrder'),
                        'is_exchange_order': get_text_from_node(order, 'IsExchangeOrder'),
                        'original_order_id': get_text_from_node(order, 'OriginalOrderID'),
                        'is_iba': get_text_from_node(order, 'IsIba'),
                        'ioss_number': get_text_from_node(order, 'IossNumber'),
                    }

                    for item in order.findall('OrderItem'):
                        item_price_node = item.find('ItemPrice')
                        promotion_node = item.find('Promotion')
                        item_details = {
                            'amazon_order_item_code': get_text_from_node(item, 'AmazonOrderItemCode'),
                            'product_name': get_text_from_node(item, 'ProductName'),
                            'sku': get_text_from_node(item, 'SKU'),
                            'asin': get_text_from_node(item, 'ASIN'),
                            'item_status': get_text_from_node(item, 'ItemStatus'),
                            'quantity': int(get_text_from_node(item, 'Quantity', '0')),
                            'number_of_items': int(get_text_from_node(item, 'NumberOfItems', '0')),
                            'currency': item_price_node.find('.//Amount').get('currency') if item_price_node and item_price_node.find('.//Amount') is not None else '',
                            'item_price': get_price_component(item_price_node, 'Principal'),
                            'item_tax': get_price_component(item_price_node, 'Tax'),
                            'shipping_price': get_price_component(item_price_node, 'Shipping'),
                            'shipping_tax': get_price_component(item_price_node, 'ShippingTax'),
                            'gift_wrap_price': get_price_component(item_price_node, 'GiftWrap'),
                            'gift_wrap_tax': get_price_component(item_price_node, 'GiftWrapTax'),
                            'vat_exclusive_item_price': get_price_component(item_price_node, 'VatExclusiveItemPrice'),
                            'vat_exclusive_shipping_price': get_price_component(item_price_node, 'VatExclusiveShippingPrice'),
                            'vat_exclusive_giftwrap_price': get_price_component(item_price_node, 'VatExclusiveGiftWrapPrice'),
                            'promotion_ids': get_text_from_node(promotion_node, 'PromotionIDs'),
                            'item_promotion_discount': float(get_text_from_node(promotion_node, 'ItemPromotionDiscount', '0.0')),
                            'ship_promotion_discount': float(get_text_from_node(promotion_node, 'ShipPromotionDiscount', '0.0')),
                            'tax_collection_model': get_text_from_node(item, 'TaxCollectionModel'),
                            'tax_collection_responsible_party': get_text_from_node(item, 'TaxCollectionResponsibleParty'),
                            'is_heavy_or_bulky': get_text_from_node(item, 'IsHeavyOrBulky'),
                            'is_amazon_invoiced': get_text_from_node(item, 'IsAmazonInvoiced'),
                            'is_transparency': get_text_from_node(item, 'IsTransparency'),
                            'is_buyer_requested_cancellation': get_text_from_node(item, 'IsBuyerRequestedCancellation'),
                            'buyer_requested_cancel_reason': get_text_from_node(item, 'BuyerRequestedCancel/Reason'),
                            'amazon_programs': get_text_from_node(item, './/AmazonProgramName'),
                            'buyer_company_name': get_text_from_node(item, 'BuyerInfo/BuyerCompanyName'),
                        }
                        flat_record = {**order_details, **item_details}
                        chunk_items.append(flat_record)

                    # When the chunk is full, yield it as a DataFrame and reset the list
                    if len(chunk_items) >= CHUNK_SIZE:
                        yield pd.DataFrame(chunk_items)
                        chunk_items = []

                    elem.clear()

            # After the loop, yield any remaining items in the last partial chunk
            if chunk_items:
                yield pd.DataFrame(chunk_items)
                
        except ET.ParseError as e:
            logging.error(f"Failed to parse XML data: {e}")
            return
    
    def fetch_financial_events(self, next_token=None):
        # Fetch financial events from Amazon SP-API
        logging.info("Fetching financial events.")
        url = "https://sellingpartnerapi-eu.amazon.com/finances/v0/financialEvents"
        headers = {'x-amz-access-token': self.access_token,
                   'Content-Type': 'application/json'}
        params = {'NextToken': next_token} if next_token else {
            'PostedAfter': self.get_date_days_ago(self.date_range)}

        response = self.controlled_request(
            'get', url, headers=headers, params=params)
        if response and response.status_code == 200:
            logging.info("Financial events fetched successfully.")
            return response.json()
        else:
            logging.error(f"Failed to fetch financial events: {response.text}")
            return None

    def controlled_request(self, method, url, headers=None, params=None, data=None, retry_count=0):
        # Send requests and handle rate limits with exponential backoff
        try:
            response = requests.request(method, url, headers=headers, params=params, data=data)
            if response.status_code == 429:  # Check if rate limit was hit
                if retry_count < 5:  # Limit the number of retries to prevent infinite loop
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)  # Exponential backoff with jitter
                    logging.warning(f"Rate limit hit, retrying after {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                    return self.controlled_request(method, url, headers, params, data, retry_count + 1)
                else:
                    logging.error("Rate limit hit repeatedly, stopping retries.")
                    return None
            return response
        except requests.exceptions.RequestException as e:
            logging.error("HTTP Request failed: %s", e)
            return None

    @staticmethod
    def camel_to_snake(name):
        # Converts CamelCase to snake_case
        """Converts CamelCase to snake_case"""
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

    def process_financial_data(self, data):
        # Process raw financial data into structured DataFrame
        logging.info("Starting to process financial data.")
        if not data or 'payload' not in data or 'FinancialEvents' not in data['payload']:
            logging.error("No data or incorrect data structure received.")
            # Return an empty DataFrame to handle this scenario gracefully.
            return pd.DataFrame()

        # Base columns for financial data DataFrame
        columns = [
            'amazon_order_id', 'marketplace_name', 'posted_date', 'seller_sku', 'order_item_id',
            'quantity_shipped'
        ]
        charge_types = set()
        fee_types = set()
        promotion_ids = set()

        # Collect all types of charges, fees, and promotions for shipments
        for event in data['payload']['FinancialEvents']['ShipmentEventList']:
            for item in event.get('ShipmentItemList', []):
                for charge in item.get('ItemChargeList', []):
                    charge_types.add(self.camel_to_snake(charge.get('ChargeType', '')))
                for fee in item['ItemFeeList']:
                    fee_types.add(self.camel_to_snake(fee.get('FeeType', '')))
                # if 'PromotionList' in item:
                #     for promo in item['PromotionList']:
                #         promotion_ids.add((self.camel_to_snake(promo['PromotionType']), promo['PromotionId']))

        # Collect all types of charges, fees, and promotions for refunds
        for event in data['payload']['FinancialEvents']['RefundEventList']:
            for item in event['ShipmentItemAdjustmentList']:
                for charge in item.get('ItemChargeAdjustmentList', []):
                    charge_types.add(self.camel_to_snake(charge.get('ChargeType', '')))
                for fee in item.get('ItemFeeAdjustmentList', []):
                    fee_types.add(self.camel_to_snake(fee.get('FeeType', '')))

        # Define all possible charge and fee types that Amazon API can return
        # This ensures consistent schema even when some types are missing from current data
        # Based on the complete list from Keboola error message
        all_possible_charge_types = {
            'gift_wrap', 'shipping_charge', 'shipping_tax', 'principal', 'tax',
            'gift_wrap_tax', 'giftwrap_commission', 'renewed_program_fee',
            'shipping_hb', 'variable_closing_fee', 'fixed_closing_fee',
            'commission', 'refund_commission', 'return_shipping', 'goodwill',
            'digital_services_fee', 'giftwrap_chargeback', 'shipping_chargeback',
            'fba_per_unit_fulfillment_fee', 'generic_deduction', 'digital_services_fee_fba'
        }
        
        # Add all possible charge and fee types to ensure consistent schema
        all_charge_fee_types = charge_types.union(fee_types).union(all_possible_charge_types)
        
        # Adding columns for each type of charge and fee
        for charge_type in all_charge_fee_types:
            columns.append(f"{charge_type}_amount")
            columns.append(f"{charge_type}_currency")
        for promo_type, promo_id in promotion_ids:
            columns.append(f"{promo_type}_amount")
            columns.append(f"{promo_type}_currency")
            columns.append(f"{promo_type}_id")

        # Initialize DataFrame with new columns
        financial_data_df = pd.DataFrame(columns=columns)
        all_rows = []

        # Populate DataFrame with shipment data
        for event in data['payload']['FinancialEvents']['ShipmentEventList']:
            for item in event['ShipmentItemList']:
                # Initialize row with all possible columns set to empty values
                row = {col: '' for col in columns}
                row.update({
                    'amazon_order_id': event.get('AmazonOrderId', ''),
                    'marketplace_name': event.get('MarketplaceName', ''),
                    'posted_date': event.get('PostedDate', ''),
                    'seller_sku': item.get('SellerSKU', ''),
                    'order_item_id': item.get('OrderItemId', ''),
                    'quantity_shipped': item.get('QuantityShipped', 0)
                })
                for charge in item.get('ItemChargeList', []):
                    charge_type_snake = self.camel_to_snake(charge.get('ChargeType', ''))
                    charge_amount_dict = charge.get('ChargeAmount', {})
                    if charge_type_snake and charge_amount_dict:
                        row[f"{charge_type_snake}_amount"] = charge_amount_dict.get('CurrencyAmount')
                        row[f"{charge_type_snake}_currency"] = charge_amount_dict.get('CurrencyCode')

                for fee in item.get('ItemFeeList', []):
                    fee_type_snake = self.camel_to_snake(fee.get('FeeType', ''))
                    fee_amount_dict = fee.get('FeeAmount', {})
                    if fee_type_snake and fee_amount_dict:
                        row[f"{fee_type_snake}_amount"] = fee_amount_dict.get('CurrencyAmount')
                        row[f"{fee_type_snake}_currency"] = fee_amount_dict.get('CurrencyCode')

                # if 'PromotionList' in item:
                #     for promo in item['PromotionList']:
                #         promo_type_snake = self.camel_to_snake(promo['PromotionType'])
                #         promo_type_id = f"{promo_type_snake}_id"
                #         promo_type_amount = f"{promo_type_snake}_amount"
                #         promo_type_currency = f"{promo_type_snake}_currency"
                #         row[promo_type_id] = promo['PromotionId']
                #         row[promo_type_amount] = promo['PromotionAmount']['CurrencyAmount']
                #         row[promo_type_currency] = promo['PromotionAmount']['CurrencyCode']

                all_rows.append(row)  # Append each item as a row to the list

        # Populate DataFrame with refund data
        for event in data['payload']['FinancialEvents']['RefundEventList']:
            for item in event['ShipmentItemAdjustmentList']:
                # Initialize row with all possible columns set to empty values
                row = {col: '' for col in columns}
                row.update({
                    'amazon_order_id': event.get('AmazonOrderId', ''),
                    'marketplace_name': event.get('MarketplaceName', ''),
                    'posted_date': event.get('PostedDate', ''),
                    'seller_sku': item.get('SellerSKU', ''),
                    'order_item_id': item.get('OrderAdjustmentItemId', ''),
                    'quantity_shipped': item.get('QuantityShipped', 0)
                })
                for charge in item.get('ItemChargeAdjustmentList', []):
                    charge_type_snake = self.camel_to_snake(charge.get('ChargeType', ''))
                    charge_amount_dict = charge.get('ChargeAmount', {})
                    if charge_type_snake and charge_amount_dict:
                        row[f"{charge_type_snake}_amount"] = charge_amount_dict.get('CurrencyAmount')
                        row[f"{charge_type_snake}_currency"] = charge_amount_dict.get('CurrencyCode')

                for fee in item.get('ItemFeeAdjustmentList', []):
                    fee_type_snake = self.camel_to_snake(fee.get('FeeType', ''))
                    fee_amount_dict = fee.get('FeeAmount', {})
                    if fee_type_snake and fee_amount_dict:
                        row[f"{fee_type_snake}_amount"] = fee_amount_dict.get('CurrencyAmount')
                        row[f"{fee_type_snake}_currency"] = fee_amount_dict.get('CurrencyCode')

                all_rows.append(row)  # Append each item as a row to the list

        return pd.DataFrame(all_rows, columns=columns)

    def process_data(self, df, file_name, primary_keys, process_empty = False):
        # Process and save data to a file
        logging.info(f"Processing {len(df)} records to write to {file_name}.")
        if not df.empty or process_empty == True:
            table_path = self.create_out_table_definition(
                file_name, incremental=True, primary_key=primary_keys).full_path
            df.to_csv(table_path, index=False)
            logging.info(
                f"File {file_name} created and data written successfully.")
        else:
            logging.warning(
                f"No data available to write to {file_name}. DataFrame is empty.")

    def create_and_download_ads_report(self, scope, country, ad_product):
        logging.info(f"Starting report creation for ad product: {ad_product} in country: {country}")
        report_id = self.create_ads_report(scope, ad_product)
        if report_id:
            logging.info(f"Report created successfully for {ad_product} in {country} with ID: {report_id}")
            report_url = self.poll_ads_report_status(report_id, scope)
            if report_url:
                logging.info(f"Downloading report for {ad_product} in {country}")
                report_data = self.download_ads_report(report_url)
                self.process_ads_data(report_data, country, ad_product)
            else:
                logging.error(f"Failed to download report for {ad_product} in {country}")
        else:
            logging.error(f"Failed to create report for {ad_product} in {country}")

    def create_ads_report(self, scope, ad_product):
        # Create an Amazon Ads report
        url = 'https://advertising-api-eu.amazon.com/reporting/reports'
        headers = {
            'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
            'Amazon-Advertising-API-ClientId': self.app_id_ads,
            'Amazon-Advertising-API-Scope': scope,
            'Authorization': f'Bearer {self.ads_access_token}'
        }

        start_date = (datetime.utcnow() - timedelta(days=10)).strftime('%Y-%m-%d')
        end_date = datetime.utcnow().strftime('%Y-%m-%d')
        payload = self.generate_payload(ad_product, start_date, end_date)

        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            report_id = response.json().get('reportId')
            logging.info(f"Report created successfully with ID: {report_id}")
            return report_id
        else:
            logging.error(f"Failed to create Amazon Ads report: {response.text}")
            return None

    def generate_payload(self, ad_product, start_date, end_date):
        base_payload = {
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "groupBy": ["campaign"],
                "columns": ["campaignId", "campaignName", "date", "impressions", "clicks", "cost"],
                "timeUnit": "DAILY",
                "format": "GZIP_JSON"
            }
        }

        if ad_product == "SPONSORED_PRODUCTS":
            base_payload["name"] = "SP advertised product report"
            base_payload["configuration"]["adProduct"] = "SPONSORED_PRODUCTS"
            base_payload["configuration"]["reportTypeId"] = "spCampaigns"
        elif ad_product == "SPONSORED_BRANDS":
            base_payload["name"] = "SB advertised product report"
            base_payload["configuration"]["adProduct"] = "SPONSORED_BRANDS"
            base_payload["configuration"]["reportTypeId"] = "sbCampaigns"
        elif ad_product == "SPONSORED_DISPLAY":
            base_payload["name"] = "SD advertised product report"
            base_payload["configuration"]["adProduct"] = "SPONSORED_DISPLAY"
            base_payload["configuration"]["reportTypeId"] = "sdCampaigns"

        return base_payload

    def poll_ads_report_status(self, report_id, scope):
        # Poll the status of the Amazon Ads report
        url = f'https://advertising-api-eu.amazon.com/reporting/reports/{report_id}'
        headers = {
            'Amazon-Advertising-API-ClientId': self.app_id_ads,
            'Amazon-Advertising-API-Scope': scope,
            'Authorization': f'Bearer {self.ads_access_token}'
        }
        while True:
            response = self.controlled_request('get', url, headers=headers)
            if response and response.status_code == 200:
                status = response.json().get('status')
                logging.info(f"Report status: {status}")
                if status == 'COMPLETED':
                    return response.json().get('url')
                elif status in ['FAILURE', 'CANCELLED']:
                    logging.error(f"Report processing ended with status: {status}")
                    break
                else:
                    logging.info("Waiting before the next status check...")
                    time.sleep(30)  # Wait for 30 seconds before checking again
            else:
                logging.error(f"Failed to poll report status: {response.text}")
                break
        return None

    def download_ads_report(self, report_url):
        # Download the Amazon Ads report
        response = self.controlled_request('get', report_url)
        if response and response.status_code == 200:
            content = gzip.decompress(response.content)
            return json.loads(content.decode('utf-8'))
        else:
            logging.error(f"Failed to download Amazon Ads report: {response.text}")
            return None

    def process_ads_data(self, report_data, country, ad_product):
        logging.info(f"Processing Amazon Ads report data for ad product: {ad_product} in country: {country}.")
        if report_data:
            df = pd.json_normalize(report_data)
            df.rename(columns={
                "campaignId": "campaign_id",
                "campaignName": "campaign_name",
                "date": "date",
                "impressions": "impressions",
                "clicks": "clicks",
                "cost": "cost"
            }, inplace=True)
            df['market'] = country
            df['adProduct'] = ad_product  # Add adProduct type to the data
            self.all_ads_data = pd.concat([self.all_ads_data, df], ignore_index=True)
            logging.info(f"Processed {len(df)} records for {ad_product} in {country}")
        else:
            logging.warning(f"No data to process for Amazon Ads report: {ad_product} in {country}.")

    def save_ads_data_to_csv(self):
        # Save combined ads data to a single CSV file
        logging.info("Saving all combined Amazon Ads report data to CSV.")
        if not self.all_ads_data.empty:
            file_name = 'advertising.csv'
            primary_keys = ['ad_id', 'campaign_id', 'date', 'advertised_sku', 'advertised_asin']
            self.process_data(self.all_ads_data, file_name, primary_keys)
        else:
            logging.warning("No data available to save. DataFrame is empty.")


if __name__ == "__main__":
    try:
        component = Component()
        component.execute_action()
    except Exception as e:
        logging.exception("An unexpected error occurred.")
        exit(1)