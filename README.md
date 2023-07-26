# Tiny ERP to BigQuery ETL

This script is a data extraction, transformation, and loading (ETL) tool for the Tiny ERP system. It pulls data from the Tiny ERP API, processes this data, and then loads it into Google's BigQuery service. The script is designed to run as a cron job every 5 minutes, ensuring that your BigQuery dataset is always up-to-date with the latest sales data.

## Description

The ETL script is specifically designed to extract data from the Tiny ERP system's API, transform the data into a suitable format for querying, and then load it into Google's BigQuery service. This includes extracting data on orders and products, transforming data for optimal querying, and loading the data into a BigQuery dataset.

On the first run, if no last processed order number is defined, the script will populate the BigQuery database with all past sales. The script keeps track of the last processed order number, allowing it to only request new sales on subsequent runs. This ensures efficient use of resources by avoiding re-processing of data.

## Features

- **Data Extraction:** The script pulls data from several Tiny ERP API endpoints, collecting detailed information on orders and products.

- **Data Transformation:** Collected data is processed and transformed into a suitable format for storage in BigQuery, with necessary data type conversions and handling of missing values.

- **Data Loading:** The transformed data is then loaded into a BigQuery dataset for further analysis.

- **Incremental Updates:** The script keeps track of the last processed order number, allowing it to only request new sales on subsequent runs. This ensures efficient use of resources by avoiding re-processing of data.

- **Error Handling:** The script includes some error handling.

## Installation

1. Clone this repository to your local machine.
2. Install the required Python packages using pip:

```
pip install -r requirements.txt
```

3. Set up the required environment variables for Google Cloud and Tiny ERP API authentication.     Specifically, you need to set the `TINY_ERP_TOKEN` environment variable to your Tiny ERP token,     and the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your Google Service Account key file.

This script requires Python 3.7 or later.

## Usage

The script is designed to be run as a cron job every 5 minutes. Here is a suggested cron job command:

```
*/5 * * * * run-one /usr/bin/python3 /opt/scripts/tiny/request.py >> /opt/scripts/tiny/activity.log 2>&1
```

This command will execute the script every 5 minutes and write any output to an activity log.

## Contributing

Contributions are always welcome! Please read the contributing guidelines before making any changes. We use the "Fork-and-Pull" Git workflow for contributions.

1. **Fork** the repo on GitHub.
2. **Clone** the project to your machine.
3. **Commit** changes to your branch.
4. **Push** your work back up to your fork.
5. Submit a **Pull request** so your changes can be reviewed.

## License

This project is licensed under the terms of the MIT license.
