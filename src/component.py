import logging
import requests
import urllib.parse
from datetime import datetime, timedelta
import pandas as pd
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

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
    def get_date_x_days_ago(num_days):
        """
        Returns a date that is `num_days` ago from today in ISO 8601 format.
        """
        date_x_days_ago = datetime.utcnow() - timedelta(days=num_days)
        return date_x_days_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

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

        last_update_after = self.get_date_x_days_ago(self.date_range)
        orders = self.fetch_orders(last_update_after)
        if orders:
            self.process_orders_data(orders)
        else:
            logging.info("No new orders to process.")

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

    def fetch_orders(self, last_update_after):
        """
        Fetches orders from the Amazon Selling Partner API using the LastUpdateDate as a filter.
        Handles pagination using NextToken.
        """
        base_url = "https://sellingpartnerapi-eu.amazon.com/orders/v0/orders"
        all_orders = []
        next_token = None

        while True:
            if next_token:
                # If NextToken is available, use it to fetch the next page of results
                url = f"{base_url}?NextToken={urllib.parse.quote(next_token)}"
            else:
                # Initial URL for the first API call
                url = f"{base_url}?LastUpdatedAfter={last_update_after}&MarketplaceIds={self.marketplace_id}"

            if not hasattr(self, 'access_token') or not self.marketplace_id:
                logging.error("Access token or marketplace ID is missing.")
                return None

            headers = {'Accept': 'application/json', 'x-amz-access-token': self.access_token}
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Ensures HTTPError is raised for bad responses
                orders_data = response.json().get('payload', {})
                all_orders.extend(orders_data.get('Orders', []))
                next_token = orders_data.get('NextToken')  # Extract NextToken for pagination
                if not next_token:
                    logging.info("No NextToken found, ending pagination.")
                    break
            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching orders: {str(e)}")
                break

        return all_orders


    def process_orders_data(self, orders):
        """
        Processes and writes order data to a CSV file.
        """
        df = pd.DataFrame(orders)
        table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['AmazonOrderId'])
        df.to_csv(table.full_path, index=False)
        logging.info('Data processed and written successfully.')

if __name__ == "__main__":
    try:
        component = Component()
        component.execute_action()
    except UserException as e:
        logging.exception("User configuration error occurred.")
        exit(1)
    except Exception as e:
        logging.exception("An unexpected error occurred.")
        exit(2)
