from functools import reduce
import sys

if __name__ == "__main__":

    output_header = "department_id,number_of_orders,number_of_first_orders,percentage"

    def map_func1(record):
        """
        Map func for phase1. Map a line of raw record into [dept_id, 1, first_ordered] format.
        :param record: string
        :return: [dept_id (int), 1 (int value 1), first_ordered (int value 0 or 1)]
        """
        record = record.strip().split(",")
        prod_id, first_ordered = int(record[1]), 1 - int(record[-1])
        return prod_dept_lookup[prod_id], 1, first_ordered

    def reduce_func(record1, record2):
        """
        Reduce func for phase1. Reduce k-v pairs associated with a key into sums of ordered_sum and first_ordered_sum
        :param record1: [1, first_ordered (0/1)]
        :param record2: [1, first_ordered (0/1)]
        :return: [ordered_sum, first_ordered_sum]
        """
        return record1[0] + record2[0], record1[1] + record2[1]

    def map_func2(record):
        """
        Map func for phase2. Calculates first_ordered_sum / ordered_sum percentage and rounds into 2 decimals.
        :param record: [ordered_sum, first_ordered_sum]
        :return: [dept_id, ordered_sum, first_ordered_sum, percentage], all in string
        """
        key, val = record[0], record[1]
        return str(key), str(val[0]), str(val[1]), "%.2f" % (float(val[1]) / val[0])


    if len(sys.argv) != 4:
        print("Please offer 3 input directories for order_products.csv, products.csv, and report.csv.")
        exit(1)

    order_products_file = sys.argv[1]
    products_file = sys.argv[2]
    output_file = sys.argv[3]

    # Build prod_dept_lookup: {prod_id: dept_id}.
    prod_dept_lookup = {}
    with open(products_file) as f:
        input_records = f.read().split("\n")[1:]  # skip csv header
        if input_records[-1] == '':  # when file ends with a new line, skip that line
            input_records = input_records[:-1]

        for line in input_records:
            record = line.strip().split(",")
            prod_id, dept_id = record[0], record[-1]  # only need prod_id and dept_id, no worry for comma in prod name
            prod_dept_lookup[int(prod_id)] = int(dept_id)

    # Read in files. In distributed file systems, we can read multiple chunks of files.
    with open(order_products_file) as f:
        input_records = f.read().split("\n")[1:]  # skip csv header
        if input_records[-1] == '':  # when file ends with a new line, skip that line
            input_records = input_records[:-1]

    # Map phase 1
    mapper_output = map(map_func1, input_records)

    # Group by prod_id (shuffling).
    group = {}
    for dept_id, ordered, first_ordered in mapper_output:
        if dept_id not in group:
            group[dept_id] = [(ordered, first_ordered)]
        else:
            group[dept_id].append((ordered, first_ordered))

    group = sorted([[key, group[key]] for key in group], key=lambda x: x[0])

    # Reduce.
    output = []
    for idx in range(len(group)):
        key, grouped_value = group[idx][0], group[idx][1]
        output.append([key, reduce(reduce_func, grouped_value)])

    # Map phase 2.
    mapper_output2 = map(map_func2, output)

    # Process output.
    with open(output_file, "w") as f:
        f.write(output_header)
        f.write("\n")
        for record in mapper_output2:
            f.write(",".join(record))
            f.write("\n")
