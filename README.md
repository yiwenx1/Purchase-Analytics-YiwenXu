# Problem
The purpose of this project is to calculate, for each department, the number of times a product was requested, number of times a product was requested for the first time and a ratio of those two numbers.

# Run Instructions  
Running is tested on Python 2.7  
In based directory, simply `./run.sh`.  
(If seeing permission errors, please try `chmod 777 run.sh` first)  

In the run script, default specifications are:  
```
SOURCE="./src/purchase_analytics.py"  # source code
ORDERS="./input/order_products.csv"  # order_products.csv file
PRODUCTS="./input/products.csv"  # products.csv file
REPORT="./output/report.csv"  # report.csv file
```  
If in need of change any directories, please modify the `run.sh` file and change these parameters.  

Unit tests are also provided. It can be run under `src` with `python purchase_analytics_test.py` or with Python `unittest` commandline interface.  

# Solution Walkthrough
This solution uses the idea of Map Reduce. It's a framework suitable for scalable big data engineering projects.  

## Sample Input
File `order_products.csv`:
```
order_id,product_id,add_to_cart_order,reordered
2,33120,1,1
2,28985,2,1
2,9327,3,0
2,45918,4,1
3,17668,1,1
3,46667,2,1
3,17461,4,1
3,32665,3,1
4,46842,1,0
```
File `products.csv`:
```
product_id,product_name,aisle_id,department_id
9327,Garlic Powder,104,13
17461,Air Chilled Organic Boneless Skinless Chicken Breasts,35,12
17668,Unsweetened Chocolate Almond Breeze Almond Milk,91,16
28985,Michigan Organic Kale,83,4
32665,Organic Ezekiel 49 Bread Cinnamon Raisin,112,3
33120,Organic Egg Whites,86,16
45918,Coconut Butter,19,13
46667,Organic Ginger Root,83,4
46842,Plain Pre-Sliced Bagels,93,3
```
## Prepare Product-Department Lookup file
First, based on `products.csv`, we create a lookup dictionary (hashmap) that has the format {product_id: department_id}. Compared with the size of product orders (transactions), the size of this lookup file is much smaller. So this lookup file can be spreaded across all mappers without using much additional space.  
Lookup file:  
```
{9327: 13,
 17461: 12,
 17668: 16,
 28985: 4,
 32665: 3,
 33120: 16,
 45918: 13,
 46667: 4,
 46842: 3}
```  
## Map
Based on `order_products.csv`, we map each line (except csv header) into `[dept_id, 1, first_ordered]` format.  
Result for map:
```
[(16, 1, 0),
 (4, 1, 0),
 (13, 1, 1),
 (13, 1, 0),
 (16, 1, 0),
 (4, 1, 0),
 (12, 1, 0),
 (3, 1, 0),
 (3, 1, 1)]
```  
## Shuffle
Based on output of mapper, we group by department ID, and sort by department ID in ascending order.  
Result for shuffle:  
```
[[3, [(1, 0), (1, 1)]],
 [4, [(1, 0), (1, 0)]],
 [12, [(1, 0)]],
 [13, [(1, 1), (1, 0)]],
 [16, [(1, 0), (1, 0)]]]
```  
## Reduce
For each key, we sum up `ordered` (first item in value) and `first_ordered` (second item in value).  
Result for reduce:  
```
[[3, (2, 1)], 
[4, (2, 0)], 
[12, (1, 0)], 
[13, (2, 1)], 
[16, (2, 0)]]
```  
## Final Map
Finally we calculate the percentage, as well as transforming all fields into string.  
Result for final map:  
```
[('3', '2', '1', '0.50'),
 ('4', '2', '0', '0.00'),
 ('12', '1', '0', '0.00'),
 ('13', '2', '1', '0.50'),
 ('16', '2', '0', '0.00')]
```   
## Output
When written to csv file, the output is:  
```
department_id,number_of_orders,number_of_first_orders,percentage
3,2,1,0.50
4,2,0,0.00
12,1,0,0.00
13,2,1,0.50
16,2,0,0.00
```












