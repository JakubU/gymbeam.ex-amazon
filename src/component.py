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

# Configuration variables
KEY_REFRESH_TOKEN = '#refresh_token'
KEY_APP_ID = '#app_id'
KEY_CLIENT_SECRET_ID = '#client_secret_id'
KEY_MARKETPLACE_ID = 'marketplace_id'
KEY_DATE_RANGE = 'date_range'

# List of mandatory parameters
REQUIRED_PARAMETERS = []
REQUIRED_IMAGE_PARS = []

class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.setup_logging()

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
            date_segments = self.split_date_range(self.date_range, 28)
            for start_date, end_date in date_segments:
                report_id = self.create_report(start_date, end_date)
                if report_id:
                    self.poll_report_status_and_download(report_id)
        else:
            logging.error("Failed to refresh token, cannot proceed with report creation.")
            
    def refresh_amazon_token(self):
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
            logging.error("Failed to refresh token.")

    def split_date_range(self, total_days, segment_length):
        segments = []
        start_date = datetime.utcnow()
        while total_days > 0:
            current_segment_length = min(segment_length, total_days)
            end_date = start_date - timedelta(days=current_segment_length)
            segments.append((start_date, end_date))
            start_date = end_date
            total_days -= current_segment_length
        return segments

    def create_report(self, start_date, end_date):
        url = "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports"
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': self.access_token
        }
        payload = json.dumps({
            "marketplaceIds": [self.marketplace_id],
            "reportType": "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL",
            "reportOptions": {},
            "dataStartTime": end_date.isoformat(timespec='milliseconds') + 'Z',
            "dataEndTime": start_date.isoformat(timespec='milliseconds') + 'Z'
        })
        
        logging.info(f'Creating report from {start_date} to {end_date}')
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 202:
            report_id = response.json().get('reportId')
            logging.info(f'Report created successfully with ID: {report_id}')
            return report_id
        else:
            logging.error(f"Failed to create report: {response.text}")
            return None

    def poll_report_status_and_download(self, report_id):
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/{report_id}"
        headers = {'x-amz-access-token': self.access_token, 'Content-Type': 'application/json'}
        logging.info(f"Polling the status of report ID: {report_id}")
        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                status = response.json().get('processingStatus')
                logging.info(f"Current processing status of the report: {status}")
                if status == 'DONE':
                    report_document_id = response.json().get('reportDocumentId')
                    logging.info(f'Report processing completed. Document ID: {report_document_id}')
                    self.download_report(report_document_id)
                    break
                elif status in ['CANCELLED', 'FATAL']:
                    logging.error(f'Report processing ended with status: {status}')
                    break
                else:
                    logging.info("Waiting before the next status check...")
                    time.sleep(10)  # Poll every 10 seconds
            else:
                logging.error(f"Failed to get report status: {response.text}")
                break

    def download_report(self, document_id):
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/documents/{document_id}"
        headers = {'x-amz-access-token': self.access_token, 'Content-Type': 'application/json'}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            document_url = response.json().get('url')
            self.process_document(document_url, response.json().get('compressionAlgorithm', ''))
        else:
            logging.error(f"Failed to retrieve document: {response.text}")

    def process_document(self, document_url, compression_algorithm):
        response = requests.get(document_url)
        if response.status_code == 200:
            content = gzip.decompress(response.content) if compression_algorithm == 'GZIP' else response.content
            df = pd.read_csv(io.StringIO(content.decode('utf-8')), delimiter='\t')
            self.process_orders_data(df)
        else:
            logging.error("Failed to download document.")

    def process_orders_data(self, df):
        table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['amazon-order-id', 'sku', 'asin'])
        df.to_csv(table.full_path, index=False)
        logging.info('Data processed and written successfully.')

if __name__ == "__main__":
    try:
        component = Component()
        component.execute_action()
    except Exception as e:
        logging.exception("An unexpected error occurred.")
        exit(1)
