# 📈 Tiny ERP to BigQuery ETL

This script is a data extraction, transformation, and loading (ETL) tool for the Tiny ERP system. It pulls data from the Tiny ERP API, processes this data, and then loads it into Google's BigQuery service. The script is designed to run as a cron job every 5 minutes, ensuring that your BigQuery dataset is always up-to-date with the latest sales data.


## 📄 Description

The ETL script is specifically designed to extract data from the Tiny ERP system's API, transform the data into a suitable format for querying, and then load it into Google's BigQuery service. This includes extracting data on orders and products, transforming data for optimal querying, and loading the data into a BigQuery dataset.

On the first run, if no last processed order number is defined, the script will populate the BigQuery database with all past sales. The script keeps track of the last processed order number, allowing it to only request new sales on subsequent runs. This ensures efficient use of resources by avoiding re-processing of data.


## ✨ Features

- **Data Extraction:** The script pulls data from several Tiny ERP API endpoints, collecting detailed information on orders and products.

- **Data Transformation:** Collected data is processed and transformed into a suitable format for storage in BigQuery, with necessary data type conversions and handling of missing values.

- **Data Loading:** The transformed data is then loaded into a BigQuery dataset for further analysis.

- **Incremental Updates:** The script keeps track of the last processed order number, allowing it to only request new sales on subsequent runs. This ensures efficient use of resources by avoiding re-processing of data.

- **Error Handling:** The script includes some error handling.


## 🚀 Installation

1. Clone this repository to your local machine.
2. Install the required Python packages using pip:

```
pip install -r requirements.txt
```

3. Set up the required environment variables for Google Cloud and Tiny ERP API authentication.     Specifically, you need to set the `TINY_ERP_TOKEN` environment variable to your Tiny ERP token,     and the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your Google Service Account key file.     In addition, you should set the `table_prefix`, `dataset_name`, and `last_processed_pedido_number_file` variables at the beginning of the script to your desired values.     The `table_prefix` and `dataset_name` will be used as the prefix for the table names and the name of the dataset in BigQuery, respectively.     The `last_processed_pedido_number_file` should be the full path to the file where the last processed pedido number is stored.

This script requires Python 3.7 or later.


## 🛠️ Usage

The script is designed to be run as a cron job every 5 minutes. Here is a suggested cron job command:

```
*/5 * * * * run-one /usr/bin/python3 /opt/scripts/tinyerp-to-bigquery/request.py >> /opt/scripts/tinyerp-to-bigquery/activity.log 2>&1
```

This command will execute the script every 5 minutes and write any output to an activity log.


## 🕓 Running as a Cron Job

When running this script as a cron job, be aware that cron jobs do not have access to the same environment variables as your interactive shell. This means that if you've set the `TINY_ERP_TOKEN` and `GOOGLE_APPLICATION_CREDENTIALS` environment variables in your shell (e.g., in your `.bashrc` or `.bash_profile` file), the cron job won't see them.

Here are a few ways to give your cron job access to these environment variables:

- **Define the environment variables in the crontab file:** You can add lines to your crontab file setting the `TINY_ERP_TOKEN` and `GOOGLE_APPLICATION_CREDENTIALS` variables. Open your crontab with `crontab -e`, and add the following lines at the top:

    ```bash
    TINY_ERP_TOKEN=your_tiny_erp_token
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service/account/key.json
    ```

    Replace `your_tiny_erp_token` and `/path/to/your/service/account/key.json` with your actual values.

- **Source your .bashrc or .bash_profile file in the cron job command:** You can tell the cron job to source your `.bashrc` or `.bash_profile` file before running the script. This will load the environment variables you've defined there. Here's how your cron job command might look:

    ```bash
    */5 * * * * . $HOME/.bashrc; run-one /usr/bin/python3 /opt/scripts/tinyerp-to-bigquery/request.py >> /opt/scripts/tinyerp-to-bigquery/activity.log 2>&1
    ```

- **Use a wrapper script:** You can create a wrapper bash script that sources your `.bashrc` or `.bash_profile` file (thus loading the environment variables) and then runs your Python script. Your cron job would then call this wrapper script.

Remember, security is paramount. Ensure that your crontab file, .bashrc or .bash_profile file, and wrapper script (if used) are secured with appropriate file permissions, and be careful not to expose your secret tokens.

## 💹 Handling the "Desconto" field in Google Data Studio

The "desconto" field at the pedidos table in the Tiny ERP data represents discounts applied to sales, and it can be in two formats: a raw BRL value, or a percentage of the total sale value.

For better analysis in Google Looker Studio, we create two new variables from the "desconto" field:

- Desconto (%)": This field extracts the percentage discount when available, and calculates the percentage discount from the total value when only the discount in BRL is available.

```
CASE
  WHEN REGEXP_MATCH(desconto, "(\\d+(\\.\\d+)?%)")
    THEN CAST(REGEXP_REPLACE(Desconto, "%", "") AS NUMBER) / 100
  ELSE CAST(desconto AS NUMBER) / CAST(Total_Value AS NUMBER)
END
```

- "Desconto (BRL)": This field extracts the discount in BRL when available, and calculates the discount in BRL from the total value and percentage discount when only the percentage discount is available.

```
CASE
  WHEN REGEXP_MATCH(desconto, "(\\d+(\\.\\d+)?%)")
    THEN CAST(totalProdutos AS NUMBER) * (CAST(REGEXP_REPLACE(desconto, "%", "") AS NUMBER) / 100)
  ELSE CAST(desconto AS NUMBER)
END
```

Remember to replace Desconto and Total_Value with the actual names of your discount and total value columns, respectively.


## 🤝 Contributing

Contributions are always welcome! Please read the contributing guidelines before making any changes. We use the "Fork-and-Pull" Git workflow for contributions.

1. **Fork** the repo on GitHub.
2. **Clone** the project to your machine.
3. **Commit** changes to your branch.
4. **Push** your work back up to your fork.
5. Submit a **Pull request** so your changes can be reviewed.


## 📝 License

This project is licensed under the terms of the MIT license.
