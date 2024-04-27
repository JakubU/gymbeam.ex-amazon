import csv
import logging
import requests
from datetime import datetime
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from datetime import datetime, timedelta


# configuration variables
KEY_REFRESH_TOKEN = '#refresh_token'
KEY_APP_ID = '#app_id'
KEY_CLIENT_SECRET_ID = '#client_secret_id'
KEY_MARKETPLACE_ID = 'marketplace_id'
KEY_DATE_RANGE = 'date_range'


# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = []
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
    
    @staticmethod
    def get_date_x_days_ago(num_days):
        """
        Returns a date that is `num_days` ago from today in ISO 8601 format.
        """
        date_x_days_ago = datetime.utcnow() - timedelta(days=num_days)
        return date_x_days_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

    def run(self):
        """
        Main execution code
        """

        # ####### EXAMPLE TO REMOVE
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        self.refresh_token = params.get(KEY_REFRESH_TOKEN)
        self.app_id = params.get(KEY_APP_ID)
        self.client_secret_id = params.get(KEY_CLIENT_SECRET_ID)
        self.marketplace_id = params.get(KEY_MARKETPLACE_ID)
        self.date_range = int(params.get(KEY_DATE_RANGE, 7))  # Default to last 7 days if not specified


        # get last state data/in/state.json from previous run
        previous_state = self.get_state_file()
        logging.info(previous_state.get('some_state_parameter'))

        # Refresh the Amazon token at the beginning of the run
        self.refresh_amazon_token()
        
        # Fetch orders updated after a specific date computed from date_range
        last_update_after = self.get_date_x_days_ago(self.date_range)
        print(last_update_after)
        orders = self.fetch_orders(last_update_after)
        if orders:
            print("Fetched orders:", orders)
        else:
            print("Failed to fetch orders.")
        
        # Create output table (Tabledefinition - just metadata)
        table = self.create_out_table_definition(
            'output.csv', incremental=True, primary_key=['timestamp'])

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        # Save table manifest (output.csv.manifest) from the tabledefinition
        self.write_manifest(table)

        # Write new state - will be available next run
        self.write_state_file({"some_state_parameter": "value"})

        # ####### EXAMPLE TO REMOVE END

    def refresh_amazon_token(self):
        # Amazon token endpoint
        url = "https://api.amazon.com/auth/o2/token"

        # Request payload
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.app_id,
            'client_secret': self.client_secret_id
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        try:
            # POST request with data encoded in URL format
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()  # Raises an HTTPError for bad responses

            # Handle response
            if response.status_code == 200:
                logging.info("Token refreshed successfully")
                json_data = response.json()
                self.access_token = json_data.get("access_token")
                # Further processing with response data here, if needed
            else:
                logging.error(
                    f"Failed to refresh token: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            # Log any error that occurred during the request
            logging.error(f"Error during token refresh: {str(e)}")
            

            
    def fetch_orders(self, last_update_after):
        """
        Fetches orders from the Amazon Selling Partner API using the LastUpdateDate as a filter.

        :param last_update_after: ISO 8601 format date string to filter orders updated after this date.
        """
        # API endpoint for fetching orders
        base_url = "https://sellingpartnerapi-eu.amazon.com/orders/v0/orders"
        url = f"{base_url}?LastUpdatedAfter={last_update_after}&MarketplaceIds={self.marketplace_id}"

        # Check if access_token and marketplace_id are available
        if not hasattr(self, 'access_token') or not self.marketplace_id:
            logging.error("Access token or marketplace ID is not available.")
            return

        # Prepare headers with the access token
        headers = {
            'Accept': 'application/json',
            'x-amz-access-token': self.access_token
        }

        # Make the GET request
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            # Process the response
            orders_data = response.json()
            logging.info("Orders fetched successfully.")
            return orders_data
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching orders: {str(e)}")
            return None



if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
