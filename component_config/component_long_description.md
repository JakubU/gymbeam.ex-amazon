# Amazon Data Extractor - Comprehensive E-commerce Analytics

## Overview

This advanced data extraction component provides comprehensive access to Amazon's ecosystem through both the Selling Partner API (SP-API) and Amazon Advertising API. Designed for enterprise-level e-commerce analytics, it enables merchants to extract, process, and analyze critical business data across multiple Amazon marketplaces.

## Key Capabilities

### FBA (Fulfillment by Amazon) Data Extraction
- **Inventory Snapshots**: Daily full snapshots of FBA inventory with detailed quantity breakdowns, condition status, and warehouse locations
- **Inventory Planning**: Strategic planning data for inventory optimization and demand forecasting
- **Ledger Reports**: Comprehensive financial tracking with both detail and summary views of inventory movements, costs, and adjustments

### FBM (Fulfilled by Merchant) Data Extraction
- **Order Management**: Complete order data extraction with incremental loading capabilities
- **Returns Processing**: Detailed return information including reasons, quantities, and financial impact
- **Financial Events**: Comprehensive financial transaction tracking including charges, fees, and promotions

### Strategic Product Analysis
- **Sales Rankings**: Real-time sales rank data for strategic products across multiple marketplaces
- **Product Performance**: Classification and display group rankings for competitive analysis
- **Market Intelligence**: ASIN-based product performance tracking

### Advertising Analytics
- **Campaign Reports**: Multi-channel advertising data from Sponsored Products, Sponsored Brands, and Sponsored Display
- **Performance Metrics**: Impressions, clicks, costs, and conversion tracking
- **Cross-Marketplace Analysis**: Advertising performance across different Amazon stores

## Technical Architecture

### Multi-API Integration
- **Selling Partner API**: Primary data source for seller-related information
- **Amazon Advertising API**: Specialized advertising and campaign data
- **OAuth 2.0 Authentication**: Secure token-based authentication for both APIs

### Data Processing Features
- **Incremental Loading**: Efficient data extraction with change detection
- **Pagination Handling**: Automatic management of large datasets
- **Rate Limit Management**: Intelligent backoff and retry mechanisms
- **Error Recovery**: Robust error handling with detailed logging

### Multi-Marketplace Support
- **Configurable Marketplaces**: Support for multiple Amazon marketplaces simultaneously
- **Regional Optimization**: Automatic endpoint selection based on marketplace configuration
- **Data Consolidation**: Unified data structure across different marketplaces

## Configuration Flexibility

### Execution Control
- **Selective Extraction**: Toggle individual data extraction modules on/off
- **Custom Date Ranges**: Configurable lookback periods for historical data
- **Marketplace Selection**: Choose specific marketplaces for targeted data extraction

### Data Output
- **Structured CSV Files**: Clean, normalized data output for easy analysis
- **Primary Key Management**: Automatic deduplication and data integrity
- **Metadata Tracking**: Extraction timestamps and source tracking

## Use Cases

### Business Intelligence
- Inventory optimization and demand forecasting
- Financial performance analysis and cost tracking
- Competitive analysis and market positioning

### Operational Excellence
- Order fulfillment monitoring and optimization
- Return analysis and process improvement
- Advertising ROI analysis and budget optimization

### Strategic Planning
- Market expansion analysis across multiple marketplaces
- Product performance benchmarking
- Revenue optimization through data-driven insights

## Integration Benefits

- **Keboola Connection Native**: Seamless integration with KBC ecosystem
- **Scalable Architecture**: Handles high-volume data extraction efficiently
- **Real-time Processing**: Near real-time data availability for decision making
- **Enterprise Security**: Secure credential management and data handling