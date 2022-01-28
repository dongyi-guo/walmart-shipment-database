import csv
import sqlite3

# Retrieve Datasets
s0 = [row for row in csv.reader(open('data/shipping_data_0.csv'))][1:]
s1 = [row for row in csv.reader(open('data/shipping_data_1.csv'))][1:]
s2 = [row for row in csv.reader(open('data/shipping_data_2.csv'))][1:]

# Get item appearance times
s1_buf = {}
for s in s1:
    if str(s) not in s1_buf:
        s1_buf[str(s)] = 1
    else:
        s1_buf[str(s)] += 1

# Stuck back into the list
for s in s1:
    s.append(str(s1_buf[str(s)]))

# Remove duplicate entries
s1 = [list(s) for s in set(tuple(e) for e in s1)]

# Combine Spreadsheet 1 into 2
for s in s1:
    for e in s2:
        if s[0] == e[0]:
            if len(e) != 7:
                e.append(s[-3])
                e.append(s[-2])
                e.append(s[-1])
            else:
                s2.append(
                    [e[0], e[1], e[2], e[3], s[-3], s[-2], s[-1]]
                )
            break

# Create product list
# and Replace item name with ID
product_list = {}
counter: int = 1
for row in s0:
    if row[2] not in product_list:
        product_list[row[2]] = counter
        row[2] = str(counter)
        counter += 1
    else:
        row[2] = str(product_list[row[2]])
for row in s2:
    if row[-3] not in product_list:
        product_list[row[-3]] = counter
        row[-3] = str(counter)
        counter += 1
    else:
        row[-3] = str(product_list[row[-3]])

for s in s2:
    print(s)
# Establish Connection
conn = sqlite3.connect("shipment_database.db")
cur = conn.cursor()

# Cleanse what has been left before
cur.execute(
    'DELETE FROM product'
)
cur.execute(
    'DELETE FROM shipment'
)

# Stuck in product list
for key, value in product_list.items():
    cur.execute(
        'INSERT INTO product VALUES (?, ?)',
        (value, key)
    )

counter = 1
# Deal with Spreadsheet 0
for row in s0:
    cur.execute(
        "INSERT INTO shipment VALUES (?,?,?,?,?)",
        (counter, int(row[2]), int(row[-2]), row[0], row[1])
    )
    counter += 1

# Deal with Spreadsheet 2
for row in s2:
    cur.execute(
        "INSERT INTO shipment VALUES (?,?,?,?,?)",
        (counter, int(row[-3]), int(row[-1]), row[1], row[2])
    )
    counter += 1

conn.commit()
conn.close()
