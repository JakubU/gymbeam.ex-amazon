import logging
import requests
from datetime import datetime, timedelta
import pandas as pd
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
import time
import gzip
import io
import json
import xml.etree.ElementTree as ET
import warnings
import re

# Suppress FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Configuration variables for API access and other settings
KEY_REFRESH_TOKEN = '#refresh_token'
KEY_APP_ID = '#app_id'
KEY_CLIENT_SECRET_ID = '#client_secret_id'
KEY_MARKETPLACE_ID = 'marketplace_id'
KEY_DATE_RANGE = 'date_range'
KEY_REFRESH_TOKEN_ADS = '#refresh_token_ads'
KEY_APP_ID_ADS = '#app_id_ads'
KEY_CLIENT_SECRET_ID_ADS = '#client_secret_id_ads'


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.setup_logging()
        self.all_orders_data = pd.DataFrame()
        self.all_returns_data = pd.DataFrame()
        self.all_financial_data = pd.DataFrame()
        self.all_ads_data = pd.DataFrame() 


    def setup_logging(self):
        # Configure logging format and level
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def get_date_days_ago(days, date_format='%Y-%m-%dT%H:%M:%S.%fZ'):
        # Return a formatted string of the datetime days ago from now
        date = datetime.utcnow() - timedelta(days=days)
        return date.strftime(date_format)

    def run(self):
        # Main execution method called to process data
        params = self.configuration.parameters
        self.refresh_token = params.get(KEY_REFRESH_TOKEN)
        self.app_id = params.get(KEY_APP_ID)
        self.client_secret_id = params.get(KEY_CLIENT_SECRET_ID)
        self.marketplace_id = params.get(KEY_MARKETPLACE_ID)
        self.date_range = int(params.get(KEY_DATE_RANGE, 7))
        self.refresh_token_ads = params.get(KEY_REFRESH_TOKEN_ADS)
        self.app_id_ads = params.get(KEY_APP_ID_ADS)
        self.client_secret_id_ads = params.get(KEY_CLIENT_SECRET_ID_ADS)

        self.refresh_amazon_token()
        self.refresh_amazon_ads_token()
        if self.access_token:
            self.handle_orders()
            self.handle_returns()
            self.handle_finances()
        if self.ads_access_token:
            self.create_and_download_ads_report("665807000098197", "Amazon.it")  # Italy
            self.create_and_download_ads_report("2780716582721957", "Amazon.de")  # Germany
            self.save_ads_data_to_csv()  
        else:
            logging.error("Failed to refresh token and could not proceed.")

    def handle_orders(self):
        # Fetch and process order data
        order_segments = self.split_date_range(self.date_range, 28)
        # Initialize a DataFrame to hold all orders data.
        all_orders_data = pd.DataFrame()
        for start_date, end_date in order_segments:
            report_id = self.create_report(
                start_date, end_date, "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL")
            if report_id:
                temp_data = self.poll_report_status_and_download(report_id, pd.DataFrame(
                ), 'orders.csv', is_xml=False, primary_keys=['amazon-order-id', 'sku', 'asin'])
                if not temp_data.empty:
                    all_orders_data = pd.concat(
                        [all_orders_data, temp_data], ignore_index=True)
        # Log before processing data
        logging.info(
            f"Number of records to process for orders: {len(all_orders_data)}")
        # Write to CSV after collecting data from all reports
        if not all_orders_data.empty:
            self.process_data(all_orders_data, 'orders.csv', [
                              'amazon-order-id', 'sku', 'asin'])
        else:
            logging.warning("No order data to process.")

    def handle_returns(self):
        # Fetch and process return data
        return_segments = self.split_date_range(self.date_range, 50)
        all_returns_data = pd.DataFrame()
        for start_date, end_date in return_segments:
            report_id = self.create_report(
                start_date, end_date, "GET_XML_RETURNS_DATA_BY_RETURN_DATE")
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
            self.process_data(all_returns_data, 'returns.csv', [
                              'return-id', 'order-id'])
        else:
            logging.warning("No return data to process.")

    def handle_finances(self):
        # Fetch and process financial data
        financial_data = self.fetch_financial_events()
        # Initialize the dataframe to hold all data.
        all_financial_data = pd.DataFrame()

        while financial_data:
            processed_data = self.process_financial_data(financial_data)
            print(
                f"Number of records to process for processed_data: {len(processed_data)}")
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
                              'amazon_order_id', 'seller_sku', 'order_item_id'])
        else:
            logging.info("No financial data to process.")

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

    def create_report(self, start_date, end_date, report_type):
        # Request a new report from Amazon SP-API
        logging.info("Creating %s report from %s to %s",
                     report_type, start_date, end_date)
        url = "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports"
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': self.access_token
        }
        payload = json.dumps({
            "marketplaceIds": [self.marketplace_id],
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

    def poll_report_status_and_download(self, report_id, data_frame, file_name, is_xml, primary_keys):
        # Check report status and download when ready
        logging.info(f"Polling report status for ID {report_id}.")
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/{report_id}"
        headers = {'x-amz-access-token': self.access_token,
                   'Content-Type': 'application/json'}
        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                status = response.json().get('processingStatus')
                logging.info("Report status: %s", status)
                if status == 'DONE':
                    document_id = response.json().get('reportDocumentId')
                    data_frame = self.download_report(
                        document_id, data_frame, file_name, is_xml, primary_keys)
                    logging.info(
                        f"Data from report {report_id} loaded, records: {len(data_frame)}")
                    return data_frame  # Return the updated dataframe
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
        return pd.DataFrame()  # Return an empty DataFrame if failed

    def download_report(self, document_id, data_frame, file_name, is_xml, primary_keys):
        # Download the report document from Amazon SP-API
        logging.info(f"Downloading report document ID: {document_id}.")
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/documents/{document_id}"
        headers = {'x-amz-access-token': self.access_token,
                   'Content-Type': 'application/json'}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            document_url = response.json().get('url')
            return self.process_document(document_url, response.json().get('compressionAlgorithm', ''), is_xml)
        else:
            logging.error(f"Failed to download document: {response.text}")
            # Ensure this returns an empty DataFrame on failure.
            return pd.DataFrame()

    def process_document(self, document_url, compression_algorithm, is_xml):
        # Process the document after downloading, convert from XML/CSV as needed
        response = requests.get(document_url)
        if response.status_code == 200:
            content = gzip.decompress(
                response.content) if compression_algorithm == 'GZIP' else response.content
            if is_xml:
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
        if response.status_code == 200:
            logging.info("Financial events fetched successfully.")
            return response.json()
        else:
            logging.error(f"Failed to fetch financial events: {response.text}")
            return None

    def controlled_request(self, method, url, headers=None, params=None, data=None):
        # Send requests and handle rate limits
        try:
            response = requests.request(
                method, url, headers=headers, params=params, data=data)
            if response.status_code == 429:  # Check if rate limit was hit
                logging.warning("Rate limit hit, sleeping for 2 seconds...")
                time.sleep(2)
                return self.controlled_request(method, url, headers, params, data)
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

        # Collect all types of charges, fees, and promotions
        for event in data['payload']['FinancialEvents']['ShipmentEventList']:
            for item in event['ShipmentItemList']:
                for charge in item['ItemChargeList']:
                    charge_types.add(self.camel_to_snake(charge['ChargeType']))
                for fee in item['ItemFeeList']:
                    fee_types.add(self.camel_to_snake(fee['FeeType']))
                # if 'PromotionList' in item:
                #     for promo in item['PromotionList']:
                #         promotion_ids.add((self.camel_to_snake(promo['PromotionType']), promo['PromotionId']))

        # Adding columns for each type of charge and fee
        for charge_type in charge_types:
            columns.append(f"{charge_type}_amount")
            columns.append(f"{charge_type}_currency")
        for fee_type in fee_types:
            columns.append(f"{fee_type}_amount")
            columns.append(f"{fee_type}_currency")
        for promo_type, promo_id in promotion_ids:
            columns.append(f"{promo_type}_amount")
            columns.append(f"{promo_type}_currency")
            columns.append(f"{promo_type}_id")

        # Initialize DataFrame with new columns
        financial_data_df = pd.DataFrame(columns=columns)
        all_rows = []

        # Populate DataFrame with data
        for event in data['payload']['FinancialEvents']['ShipmentEventList']:
            for item in event['ShipmentItemList']:
                row = {
                    'amazon_order_id': event['AmazonOrderId'],
                    'marketplace_name': event['MarketplaceName'],
                    'posted_date': event['PostedDate'],
                    'seller_sku': item['SellerSKU'],
                    'order_item_id': item['OrderItemId'],
                    'quantity_shipped': item['QuantityShipped']
                }
                for charge in item['ItemChargeList']:
                    charge_type_snake = self.camel_to_snake(
                        charge['ChargeType'])
                    row[f"{charge_type_snake}_amount"] = charge['ChargeAmount']['CurrencyAmount']
                    row[f"{charge_type_snake}_currency"] = charge['ChargeAmount']['CurrencyCode']

                for fee in item['ItemFeeList']:
                    fee_type_snake = self.camel_to_snake(fee['FeeType'])
                    row[f"{fee_type_snake}_amount"] = fee['FeeAmount']['CurrencyAmount']
                    row[f"{fee_type_snake}_currency"] = fee['FeeAmount']['CurrencyCode']

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

        return pd.DataFrame(all_rows, columns=columns)

    def process_data(self, df, file_name, primary_keys):
        # Process and save data to a file
        logging.info(f"Processing {len(df)} records to write to {file_name}.")
        if not df.empty:
            table_path = self.create_out_table_definition(
                file_name, incremental=True, primary_key=primary_keys).full_path
            df.to_csv(table_path, index=False)
            logging.info(
                f"File {file_name} created and data written successfully.")
        else:
            logging.warning(
                f"No data available to write to {file_name}. DataFrame is empty.")

    def create_and_download_ads_report(self, scope,country):
        report_id = self.create_ads_report(scope)
        if report_id:
            report_url = self.poll_ads_report_status(report_id, scope)
            if report_url:
                report_data = self.download_ads_report(report_url)
                self.process_ads_data(report_data, country)

    def create_ads_report(self, scope):
        # Create an Amazon Ads report
        url = 'https://advertising-api-eu.amazon.com/reporting/reports'
        headers = {
            'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
            'Amazon-Advertising-API-ClientId': self.app_id_ads,
            'Amazon-Advertising-API-Scope': scope,
            'Authorization': f'Bearer {self.ads_access_token}'
        }
        payload = {
            "name": "SP advertised product report",
            "startDate": (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d'),
            "endDate": datetime.utcnow().strftime('%Y-%m-%d'),
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["advertiser"],
                "columns": ["adGroupId", "campaignId", "campaignName", "date", "adId", "impressions", "advertisedAsin", "advertisedSku", "clicks", "cost", "costPerClick", "spend"],
                "reportTypeId": "spAdvertisedProduct",
                "timeUnit": "DAILY",
                "format": "GZIP_JSON"
            }
        }
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            report_id = response.json().get('reportId')
            logging.info(f"Report created successfully with ID: {report_id}")
            return report_id
        else:
            logging.error(f"Failed to create Amazon Ads report: {response.text}")
            return None

    def poll_ads_report_status(self, report_id, scope):
        # Poll the status of the Amazon Ads report
        url = f'https://advertising-api-eu.amazon.com/reporting/reports/{report_id}'
        headers = {
            'Amazon-Advertising-API-ClientId': self.app_id_ads,
            'Amazon-Advertising-API-Scope': scope,
            'Authorization': f'Bearer {self.ads_access_token}'
        }
        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
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
        response = requests.get(report_url)
        if response.status_code == 200:
            content = gzip.decompress(response.content)
            return json.loads(content.decode('utf-8'))
        else:
            logging.error(f"Failed to download Amazon Ads report: {response.text}")
            return None

    def process_ads_data(self, report_data, country):
        # Process and combine the Amazon Ads report data into a single DataFrame
        logging.info(f"Processing Amazon Ads report data for country: {country}.")
        if report_data:
            df = pd.json_normalize(report_data)
            df.rename(columns={
                "adGroupId": "ad_group_id",
                "campaignId": "campaign_id",
                "campaignName": "campaign_name",
                "date": "date",
                "adId": "ad_id",
                "impressions": "impressions",
                "advertisedAsin": "advertised_asin",
                "advertisedSku": "advertised_sku",
                "clicks": "clicks",
                "cost": "cost",
                "costPerClick": "cost_per_click",
                "spend": "spend"
            }, inplace=True)
            df['market'] = country
            self.all_ads_data = pd.concat([self.all_ads_data, df], ignore_index=True)
        else:
            logging.warning("No data to process for Amazon Ads report.")

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