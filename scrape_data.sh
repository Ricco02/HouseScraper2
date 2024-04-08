#!/bin/bash
eval "$(conda shell.bash hook)"
conda activate scraper

echo "Currently active Conda environment:"
conda env list | grep '*' | awk '{print $1}'

# Run first Python script
echo "Running links scraper Python script..."
python src/scraper.py

# Check the exit status of the first script
if [ $? -eq 0 ]; then
    echo "Scraper executed successfully."
else
    echo "Error: Scraper failed."
    exit 1
fi

# Run second Python script
echo "Running advert scraper script..."
python src/ad_scraper_aiohttp.py

# Check the exit status of the second script
if [ $? -eq 0 ]; then
    echo "Advert scraper executed successfully."
else
    echo "Error: Advert scraper failed."
    exit 1
fi

conda activate pyspark

echo "Currently active Conda environment:"
conda env list | grep '*' | awk '{print $1}'

echo "Running data preprocessing script..."
python src/data_cleansing_spark.py

# Check the exit status of the third script
if [ $? -eq 0 ]; then
    echo "Preprocessing script executed successfully."
else
    echo "Error: Preprocessing script failed."
    exit 1
fi

echo "Both Python scripts executed successfully."
