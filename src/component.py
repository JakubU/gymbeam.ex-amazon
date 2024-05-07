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
import os
import xml.etree.ElementTree as ET


# Configuration variables
KEY_REFRESH_TOKEN = '#refresh_token'
KEY_APP_ID = '#app_id'
KEY_CLIENT_SECRET_ID = '#client_secret_id'
KEY_MARKETPLACE_ID = 'marketplace_id'
KEY_DATE_RANGE = 'date_range'

class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.setup_logging()
        self.all_orders_data = pd.DataFrame()
        self.all_returns_data = pd.DataFrame()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def get_date_days_ago(days, date_format='%Y-%m-%dT%H:%M:%S.%fZ'):
        date = datetime.utcnow() - timedelta(days=days)
        return date.strftime(date_format)

    def run(self):
        params = self.configuration.parameters
        self.refresh_token = params.get(KEY_REFRESH_TOKEN)
        self.app_id = params.get(KEY_APP_ID)
        self.client_secret_id = params.get(KEY_CLIENT_SECRET_ID)
        self.marketplace_id = params.get(KEY_MARKETPLACE_ID)
        self.date_range = int(params.get(KEY_DATE_RANGE, 7))

        self.refresh_amazon_token()
        if self.access_token:
            order_segments = self.split_date_range(self.date_range, 28)
            for start_date, end_date in order_segments:
                report_id = self.create_report(start_date, end_date, "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL")
                if report_id:
                    self.poll_report_status_and_download(report_id, self.all_orders_data, 'orders.csv', is_xml=False, primary_keys=['amazon-order-id', 'sku', 'asin'])
                    if not self.all_orders_data.empty:
                        self.process_data(self.all_orders_data, 'orders.csv', ['amazon-order-id', 'sku', 'asin'])
                        self.all_orders_data = pd.DataFrame()  # Reset after writing

            return_segments = self.split_date_range(self.date_range, 50)
            for start_date, end_date in return_segments:
                report_id = self.create_report(start_date, end_date, "GET_XML_RETURNS_DATA_BY_RETURN_DATE")
                if report_id:
                    self.poll_report_status_and_download(report_id, self.all_returns_data, 'returns.csv', is_xml=True, primary_keys=['return-id', 'order-id'])
                    if not self.all_returns_data.empty:
                        self.process_data(self.all_returns_data, 'returns.csv', ['return-id', 'order-id'])
                        self.all_returns_data = pd.DataFrame()  # Reset after writing
        else:
            logging.error("Failed to refresh token and could not proceed.")


    def refresh_amazon_token(self):
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

    def split_date_range(self, total_days, segment_length):
        logging.info("Splitting the date range into segments.")
        segments = []
        start_date = datetime.utcnow()
        while total_days > 0:
            current_segment_length = min(segment_length, total_days)
            end_date = start_date - timedelta(days=current_segment_length)
            segments.append((start_date, end_date))
            start_date = end_date
            total_days -= current_segment_length
        return segments

    def create_report(self, start_date, end_date, report_type):
        logging.info("Creating %s report from %s to %s", report_type, start_date, end_date)
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
        logging.info(f"Polling report status for ID {report_id}.")
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/{report_id}"
        headers = {'x-amz-access-token': self.access_token, 'Content-Type': 'application/json'}
        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                status = response.json().get('processingStatus')
                logging.info("Report status: %s", status)
                if status == 'DONE':
                    document_id = response.json().get('reportDocumentId')
                    self.download_report(document_id, data_frame, file_name, is_xml, primary_keys)  # Pass primary_keys to download_report
                    break
                elif status in ['CANCELLED', 'FATAL']:
                    logging.error("Report processing ended with status: %s", status)
                    break
                else:
                    logging.info("Waiting before the next status check...")
                    time.sleep(10)
            else:
                logging.error("Failed to poll report status: %s", response.text)
                break

    def download_report(self, document_id, data_frame, file_name, is_xml, primary_keys):
        logging.info(f"Downloading report document ID: {document_id}.")
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/documents/{document_id}"
        headers = {'x-amz-access-token': self.access_token, 'Content-Type': 'application/json'}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            document_url = response.json().get('url')
            updated_df = self.process_document(document_url, response.json().get('compressionAlgorithm', ''), data_frame, is_xml)
            if not updated_df.empty:
                self.process_data(updated_df, file_name, primary_keys)  # Process and save each segment individually
        else:
            logging.error(f"Failed to download document: {response.text}")


    def process_document(self, document_url, compression_algorithm, data_frame, is_xml):
        logging.info(f"Processing document with compression: {compression_algorithm}")
        response = requests.get(document_url)
        if response.status_code == 200:
            content = gzip.decompress(response.content) if compression_algorithm == 'GZIP' else response.content
            try:
                if is_xml:
                    # Here you'll handle XML processing.
                    # Let's assume you want to parse XML directly here
                    df = self.parse_xml_data(content)
                    if df is not None and not df.empty:
                        return pd.concat([data_frame, df], ignore_index=True)
                    else:
                        logging.warning("No data found in the document.")
                        return data_frame
                else:
                    # For CSV or other formats
                    df = pd.read_csv(io.StringIO(content.decode('utf-8')), delimiter='\t')
                    if not df.empty:
                        return pd.concat([data_frame, df], ignore_index=True)
                    else:
                        logging.warning("No data found in the document.")
                        return data_frame
            except Exception as e:
                logging.error(f"Failed to process document due to: {e}")
                return data_frame
        else:
            logging.error("Failed to download or process document.")
            return data_frame  # Return the original or updated DataFrame


    def parse_xml_data(self, xml_data):
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

        return pd.DataFrame(all_records)

    def process_data(self, df, file_name, primary_keys):
        if not df.empty:
            # Get the full path for the CSV file
            table_path = self.create_out_table_definition(file_name, incremental=True, primary_key=primary_keys).full_path

            # Since the file is always expected to be created new, always include headers.
            df.to_csv(table_path, index=False)
            logging.info(f"File {file_name} created and data written successfully.")
        else:
            logging.warning(f"No data available to write to {file_name}. DataFrame is empty.")




if __name__ == "__main__":
    try:
        component = Component()
        component.execute_action()
    except Exception as e:
        logging.exception("An unexpected error occurred.")
        exit(1)
