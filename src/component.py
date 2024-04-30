import logging
import requests
import urllib.parse
from datetime import datetime, timedelta
import pandas as pd
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
import time
import gzip
import io
import json
import xml.etree.ElementTree as ET


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
    """
    Extends the base class for general Python components, initializes the CommonInterface,
    and performs configuration validation.
    """
    def __init__(self):
        super().__init__()
        self.setup_logging()

    def setup_logging(self):
        """ Configures the logging. """
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def get_date_days_ago(days, date_format='%Y-%m-%dT%H:%M:%S.%fZ'):
        """ Returns a date that is `days` ago from today formatted according to `date_format`. """
        date = datetime.utcnow() - timedelta(days=days)
        return date.strftime(date_format)

    def run(self):
        """
        Main execution method for the component.
        """
        params = self.configuration.parameters
        self.refresh_token = params.get(KEY_REFRESH_TOKEN)
        self.app_id = params.get(KEY_APP_ID)
        self.client_secret_id = params.get(KEY_CLIENT_SECRET_ID)
        self.marketplace_id = params.get(KEY_MARKETPLACE_ID)
        self.date_range = int(params.get(KEY_DATE_RANGE, 7))

        previous_state = self.get_state_file()
        logging.info(f'Previous state: {previous_state.get("some_state_parameter")}')
        
        self.refresh_amazon_token()
        if self.access_token:
            report_id = self.create_report()
            if report_id:
                self.poll_report_status_and_download(report_id)
        else:
            logging.error("Failed to refresh token, cannot proceed with report creation.")

    def refresh_amazon_token(self):
        """
        Refreshes the Amazon token at the beginning of the run.
        """
        url = "https://api.amazon.com/auth/o2/token"
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.app_id,
            'client_secret': self.client_secret_id
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        try:
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()
            json_data = response.json()
            self.access_token = json_data.get("access_token")
            logging.info("Token refreshed successfully.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to refresh token: {e}")

    def create_report(self):
        """ Creates a report based on defined parameters. """
        url = "https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports"
        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': self.access_token
        }
        now = datetime.utcnow()
        data_end_time = now.isoformat(timespec='milliseconds') + 'Z'
        data_start_time = (now - timedelta(days=self.date_range)).isoformat(timespec='milliseconds') + 'Z'
        
        payload = json.dumps({
            "marketplaceIds": [self.marketplace_id],
            "reportType": "GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
            "reportOptions": {},
            "dataStartTime": data_start_time,
            "dataEndTime": data_end_time
        })
        
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 202:
            report_id = response.json().get('reportId')
            logging.info(f'Report created successfully with ID: {report_id}')
            return report_id
        else:
            logging.error(f"Failed to create report: {response.text}")
            return None

    def poll_report_status_and_download(self, report_id):
        """ Polls the report status and downloads it when ready. """
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/reports/{report_id}"
        headers = {
            'x-amz-access-token': self.access_token,
            'Content-Type': 'application/json'
        }

        logging.info(f"Starting to poll the status of the report with ID: {report_id}")

        while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                status = response.json().get('processingStatus')
                logging.info(f"Current processing status of the report: {status}")

                if status in ['DONE', 'CANCELLED', 'FATAL']:
                    if status == 'DONE':
                        report_document_id = response.json().get('reportDocumentId')
                        logging.info(f'Report processing completed. Document ID: {report_document_id}')
                        self.download_report(report_document_id)
                    else:
                        logging.info(f'Report processing ended with status: {status}')
                    break
                else:
                    logging.info("Report is still processing. Waiting before the next status check...")
                    time.sleep(10)  # Poll every 10 seconds
            else:
                logging.error(f"Failed to get report status: {response.text}")
                break
            
    def process_orders_data(self, orders):
        """
        Processes and writes order data to a CSV file.
        """
        df = pd.DataFrame(orders)
        table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['AmazonOrderId'])
        df.to_csv(table.full_path, index=False)
        logging.info('Data processed and written successfully.')

    def download_report(self, document_id):
        """ Downloads the report using the document ID, decompresses if GZIP, parses XML, and saves as CSV using process_orders_data. """
        url = f"https://sellingpartnerapi-eu.amazon.com/reports/2021-06-30/documents/{document_id}"
        headers = {
            'x-amz-access-token': self.access_token,
            'Content-Type': 'application/json'
        }

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            document_url = response.json().get('url')
            compression_algorithm = response.json().get('compressionAlgorithm', '')
            logging.info(f'Document URL retrieved: {document_url}')
            
            # Download the actual report
            report_response = requests.get(document_url)
            if report_response.status_code == 200:
                if compression_algorithm == 'GZIP':
                    with gzip.GzipFile(fileobj=io.BytesIO(report_response.content)) as decompressed:
                        xml_content = decompressed.read()
                else:
                    xml_content = report_response.content

                # Parse XML and extract data
                root = ET.fromstring(xml_content)
                orders = []

                for order in root.findall('.//Order'):
                    order_data = {
                        'AmazonOrderID': order.findtext('AmazonOrderID'),
                        'PurchaseDate': order.findtext('PurchaseDate'),
                        'LastUpdatedDate': order.findtext('LastUpdatedDate'),
                        'OrderStatus': order.findtext('OrderStatus'),
                        'SalesChannel': order.findtext('SalesChannel'),
                        'City': order.find('.//Address/City').text if order.find('.//Address/City') is not None else None,
                        'State': order.find('.//Address/State').text if order.find('.//Address/State') is not None else None,
                        'PostalCode': order.find('.//Address/PostalCode').text if order.find('.//Address/PostalCode') is not None else None,
                        'Country': order.find('.//Address/Country').text if order.find('.//Address/Country') is not None else None,
                        'FulfillmentChannel': order.find('.//FulfillmentData/FulfillmentChannel').text if order.find('.//FulfillmentData/FulfillmentChannel') is not None else None,
                        'ShipServiceLevel': order.find('.//FulfillmentData/ShipServiceLevel').text if order.find('.//FulfillmentData/ShipServiceLevel') is not None else None
                    }
                    for item in order.findall('.//OrderItem'):
                        order_data.update({
                            'AmazonOrderItemCode': item.findtext('AmazonOrderItemCode'),
                            'ASIN': item.findtext('ASIN'),
                            'SKU': item.findtext('SKU'),
                            'ItemStatus': item.findtext('ItemStatus'),
                            'ProductName': item.findtext('ProductName'),
                            'Quantity': item.findtext('Quantity'),
                            'ItemPriceAmount': item.find('.//ItemPrice/Component[Type="Principal"]/Amount').text if item.find('.//ItemPrice/Component[Type="Principal"]/Amount') is not None else None
                        })
                        orders.append(order_data.copy())  # Make a copy to avoid overwriting

                # Now pass the data to process_orders_data
                if orders:
                    self.process_orders_data(orders)
                else:
                    logging.info('No data found in XML to process.')

            else:
                logging.error(f"Failed to download document: {report_response.text}")
        else:
            logging.error(f"Failed to retrieve document: {response.text}")
        
if __name__ == "__main__":
    try:
        component = Component()
        component.execute_action()
    except Exception as e:
        logging.exception("An unexpected error occurred.")
        exit(1)
