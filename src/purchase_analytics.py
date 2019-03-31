order_products_file = ""
products_file = ""
output_file = ""

output_header = "department_id,number_of_orders,number_of_first_orders,percentage"

# prod_dept: {prod_id: dept_id}, time: O(num_products), space: O(num_products)
prod_dept = {}
with open(products_file) as f:
    header = f.next() # get rid of first header line
    for line in f:
        record = line.strip().split(",")
        prod_id, dept_id = record[0], record[-1] # only need prod_id and dept_id, no need to worry comma in prod name
        prod_dept[int(prod_id)] = int(dept_id)

# prod_order: {product_id: [num_ordered, num_reordered, dept_id]}, time: O(num_requests), space: O(num_products)
prod_order = {}
with open(order_products_file) as f:
    header = f.next()
    for line in f:
        record = line.strip().split(",")
        prod_id, reordered = int(record[1]), int(record[-1])
        if prod_id not in prod_order:
            prod_order[prod_id] = [1, 1 - reordered, prod_dept[prod_id]]
        else:
            prod_order[prod_id][0] += 1
            prod_order[prod_id][0] += 1 - reordered

# dept_result: {dept_id: [num_orders, num_first_orders]}, time: O(num_products), space: O(num_depts)
dept_result = {}
for prod_id in prod_order:
    num_ordered, num_firstordered, dept_id = prod_order[prod_id]
    if dept_id not in dept_result:
        dept_result[dept_id] = [num_ordered, num_firstordered]
    else:
        dept_result[dept_id][0] += num_ordered
        dept_result[dept_id][1] += num_firstordered

# produce results
results = sorted(dept_result.items(), key=lambda x: x[0]) # sort takes O(num_deptxlog(num_dept)) time
with open(output_file, "w") as f:
    f.write(output_header)
    f.write("\n")
    for record in results:
        dept_id = record[0]
        num_orders = record[1][0]
        num_first_orders = record[1][1]
        precentage = "%.2f" % (float(num_first_orders) / num_orders)
        line = ",".join([str(dept_id), str(num_orders), str(num_first_orders), precentage])
        f.write(line)
        f.write("\n")

