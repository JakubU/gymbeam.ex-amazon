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
- **date_range**: Determines the time frame for the data extraction. The component calculates the start date by subtracting the given number of days from the current date.

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
    "date_range": "30"
  }
}
