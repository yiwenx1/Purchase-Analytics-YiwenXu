#!/bin/bash
#
# Use this shell script to compile (if necessary) your code and then execute it. Below is an example of what might be found in this file if your program was written in Python
#
SOURCE="./src/purchase_analytics.py"  # source code
ORDERS="./input/order_products.csv"  # order_products.csv file
PRODUCTS="./input/products.csv"  # products.csv file
REPORT="./output/report.csv"  # report.csv file

if [ -f $REPORT ]; then
    rm $REPORT
fi

python $SOURCE $ORDERS $PRODUCTS $REPORT
