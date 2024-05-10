# Amazon Data Integration

## Description

This component facilitates the extraction of financial, orders, and returns data from the Amazon Selling Partner API. It is designed to be deployed within the Keboola Connection (KBC) ecosystem, leveraging its processing capabilities to handle API data efficiently.

## Table of Contents

- [Functionality Notes](#functionality-notes)
- [Prerequisites](#prerequisites)
- [Features](#features)
- [Supported Endpoints](#supported-endpoints)
- [Configuration](#configuration)
- [Output](#output)
- [Development](#development)
- [Integration](#integration)

## Functionality Notes

This component is specifically developed to manage data flows from Amazon's API into KBC, allowing for detailed analysis and storage.

## Prerequisites

- **Amazon Seller Account**: Must have access to Amazon Seller Central.
- **API Credentials**: Register your application with Amazon to obtain API credentials such as the `client_id`, `client_secret`, and `refresh_token`.

## Features

| **Feature**              | **Description**                                   |
|--------------------------|---------------------------------------------------|
| Authentication           | OAuth authentication with refresh token mechanism |
| Incremental Loading      | Supports incremental data fetch to reduce load    |
| Date Range Filter        | Customizable date range for data extraction       |
| Dynamic Configuration    | Configuration can be dynamically adjusted         |
| Supported API Endpoints  | Handles multiple API endpoints                    |
| Error Handling           | Robust error handling and logging                 |

## Supported Endpoints

- **Orders**: Fetches order details within a specified date range.
- **Returns**: Retrieves return information associated with orders.
- **Financial Events**: Gathers financial transactions and adjustments.

For additional endpoints, submit a request at [ideas.keboola.com](https://ideas.keboola.com/).

## Configuration

- **refresh_token**: Your Amazon API refresh token.
- **app_id**: The application ID received from Amazon upon app registration.
- **client_secret_id**: The client secret tied to your Amazon application.
- **marketplace_id**: The Amazon marketplace ID relevant to your data.
- **date_range**: Number of days from the current date for which to pull historical data.

## Output

The output will be a set of tables uploaded to KBC, structured according to the data schema of the fetched API data. This includes details on orders, returns, and financial events.

## Development

Set up your development environment by cloning the repository and running the component:

```bash
git clone https://github.com/JakubU/gymbeam.ex-amazon gymbeam.ex-amazon
cd gymbeam.ex-amazon
docker-compose build
docker-compose run --rm dev