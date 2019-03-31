from functools import reduce
import sys

order_products_file = ""
order_products_file2 = ""
products_file = ""
output_file = ""

output_header = "department_id,number_of_orders,number_of_first_orders,percentage"

# prod_dept_lookup: {prod_id: dept_id}, time: O(num_products), space: O(num_products)
prod_dept_lookup = {}
with open(products_file) as f:
    header = f.next() # get rid of first header line
    for line in f:
        record = line.strip().split(",")
        prod_id, dept_id = record[0], record[-1] # only need prod_id and dept_id, no need to worry comma in prod name
        prod_dept_lookup[int(prod_id)] = int(dept_id)

def map_func1(record):
    record = record.strip().split(",")
    prod_id, first_ordered = int(record[1]), 1 - int(record[-1])
    return prod_dept_lookup[prod_id], 1, first_ordered

def reduce_func(record1, record2):
    return record1[0] + record2[0], record1[1] + record2[1]

def map_func2(record):
    key, val = record[0], record[1]
    return str(key), str(val[0]), str(val[1]), "%.2f" % (float(val[1]) / val[0])

# Read in files. In distributed file systems, we can read multiple chunks of files
with open(order_products_file) as f:
    input_str = f.read().split("\n")[1:] # skip csv header
    if input_str[-1] == '': # when file ends with a new line, skip that line
        input_str = input_str[:-1]

# map phase 1
mapper_output = map(map_func1, input_str)

# group by prod_id (shuffling)
group = {}
for dept_id, ordered, first_ordered in mapper_output:
    if dept_id not in group:
        group[dept_id] = [(ordered, first_ordered)]
    else:
        group[dept_id].append((ordered, first_ordered))

group = sorted([[key, group[key]] for key in group], key=lambda x: x[0])

# reduce
output = []
for idx in range(len(group)):
    key, grouped_value = group[idx][0], group[idx][1]
    output.append([key, reduce(reduce_func, grouped_value)])

# map phase 2
mapper_output2 = map(map_func2, output)

# process output
with open(output_file, "w") as f:
    f.write(output_header)
    f.write("\n")
    for record in mapper_output2:
        f.write(",".join(record))
        f.write("\n")