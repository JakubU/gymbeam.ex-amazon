# Configuration Description for Amazon Extractor

## Description

This component is designed to connect to Amazon's Selling Partner API and extract orders, returns, and financial data. It's optimized for use within the Keboola Connection (KBC) ecosystem.

## Configuration Sections

### Storage

#### Input
- **Tables**: Configurations for input tables from which data can be fetched, specifying source, destination, and optional filters.

#### Output
- **Tables**: Specifies the tables where the output data will be stored after processing. In the current setup, there are no predefined output tables.

### Parameters

- **marketplace_id**: Specifies the Amazon marketplace from which data is to be extracted. It is crucial for directing API calls to the correct regional endpoint.
- **#refresh_token**, **#app_id**, **#client_secret_id**: These are secured parameters used for OAuth authentication with Amazon's API. They must be stored securely and are essential for accessing Amazon's resources.
- **#refresh_token_ads**, **#app_id_ads**, **#client_secret_id_ads**: These are secured parameters used for OAuth authentication with Amazon Ads API.
- **date_range**: Determines the time frame for the data extraction. The component calculates the start date by subtracting the given number of days from the current date.

### Obtaining API Credentials and Marketplace ID

1. **Marketplace ID**: Identify the Amazon marketplace ID relevant to your geographic sales region. This ID is crucial as it determines which Amazon data you can access. [See Marketplace IDs](https://developer-docs.amazon.com/sp-api/docs/marketplace-ids).

2. **Register for Amazon API Access**:
    - Sign in to your Amazon Seller Central account.
    - Navigate to the 'Apps & Services' section and select 'Develop Apps'.
    - Follow the process to create a new application, which will provide you with the necessary `app_id` and other details.
    - [Connecting to the Selling Partner API Guide](https://developer-docs.amazon.com/sp-api/docs/connecting-to-the-selling-partner-api?ld=NSGoogle) provides a step-by-step approach.

3. **Authentication Credentials**:
    - Once your application is created, you will receive a `refresh_token`, `app_id`, and `client_secret_id`.
    - These credentials will be used to authenticate API requests made by this component.

### Authorization

Describes OAuth credentials setup for secure access:
- **Credentials**: Contains the OAuth credentials authorized by the user, including refresh tokens and client secrets. 
- **oauthVersion**: Indicates the OAuth version supported by the component, which is "2.0" for Amazon API.

## Functionality

### Data Handling
- **Incremental Loading**: The component supports incremental data loading, which means only new or updated records are fetched in subsequent runs based on the specified date range.
- **Error Handling**: Implements robust error handling to manage API rate limits and possible disconnections or API errors.

### Development and Customization
- Developers can clone and set up the component for customization. They can build upon existing functionalities or add support for additional endpoints as per user or business requirements.

### API Endpoints
- The component handles data from multiple Amazon API endpoints related to orders, returns, and financial transactions. Each endpoint is selected based on the configuration parameters set by the user in KBC.

## Integration

The component is fully integrated into the KBC platform, allowing users to configure and schedule data extraction jobs directly from their KBC projects. It supports both manual and automated triggers for data synchronization.

## Sample Configuration

```json
{
  "parameters": {
    "marketplace_id": "APJ6JRA9NG5V4",
    "#refresh_token": "EncryptedToken",
    "#app_id": "EncryptedAppID",
    "#client_secret_id": "EncryptedClientSecret",
    "#refresh_token_ads": "EncryptedTokenAds",
    "#app_id_ads": "EncryptedAppIDAds",
    "#client_secret_id_ads": "EncryptedClientSecretAds",
    "date_range": "30"
  }
}