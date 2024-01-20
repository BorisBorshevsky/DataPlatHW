# Cassandra-driver
# Data
import csv

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement

# ('0969419414', 'The Joy of Not Working', 'Ernie J. Zelinski', 1994, 'Not Avail', 'http://images.amazon.com/images/P/0969419414.01.THUMBZZZ.jpg', 'http://images.amazon.com/images/P/0969419414.01.MZZZZZZZ.jpg', 'http://images.amazon.com/images/P/0969419414.01.LZZZZZZZ.jpg')
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("USE books");


# Helper function to construct the batch operation and then run concurrently
def load_data_concurrent(csv_file, insert_query, data_types, batch_size=100, concurrency=20, limit=None):
    global c
    c = 0
    '''Load data from a CSV file into a Cassandra table using batch inserts.
        Inputs:
        1. csv_file: str
            Path to the CSV file.
        2. insert_query: str
            CQL query for inserting data into the Cassandra table.
        3. data_types: list
            List of data types corresponding to the columns in the CSV file.
        4. batch_size: int, optional (default=100)
            Number of rows in each batch.
        5. concurrency: int, optional (default=20)
            Number of threads for parallel execution.

        Returns: Tuple with converted data.
    '''

    # Helper functions
    def convert_data(row):
        print(row[0])
        # Convert a single row's data to the correct format according to the data_types
        converted_data = [data_type(value) if value else data_type() for value, data_type in zip(row, data_types)]
        # for value, data_type in zip(row, data_types):
        #     if data_type == int:
        #         converted_data.append(int(value))
        #     elif data_type == float:
        #         converted_data.append(float(value))
        #     else:
        #         converted_data.append(value)
        return tuple(converted_data)

    def build_batch(rows):
        global c
        # Build a batch statement for the current set of rows (single batch)
        batch = BatchStatement()
        for row in rows:
            c = c + 1
            if row:
                try:
                    converted_row = convert_data(row)
                    batch.add(SimpleStatement(insert_query), converted_row)
                except Exception as e:
                    print(e)
            else:
                print("empty")
                print(row, rows)
        try:
            session.execute(batch, trace=True)
        except Exception as e:
            print(e)

    # Function body
    # open CSV
    with open(csv_file, "r") as f:
        next(f)  # Skip the header row.
        reader = list(csv.reader(f))

    # split to batches
    rows = [reader[i: i + batch_size] for i in range(0, len(reader), batch_size)][:limit]

    for r in rows:
        build_batch(r)


    print(c)

    # concurrently run each batch
    # with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
    #     executor.map(build_batch, rows)


def count_lines(file_name):
    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file)
        count = sum(1 for line in csv_reader if line)
    return count

def count_ids(file_name):
    ids = []
    with open(file_name, 'r') as file:
        csv_reader = csv.reader(file)
        for line in csv_reader:
            ids.append(line.split(",")[0])
    return ids

def find_duplicates(csv_file):
    seen_values = set()

    with open(csv_file, 'r') as file:
        header = file.readline()  # Read the header line if there is one
        for line in file:
            values = line.strip().split(',')[0]  # Assuming values are comma-separated
            if values in seen_values:
                print(f'Duplicate found: {values}')
            else:
                seen_values.add(values)


if __name__ == '__main__':
    ## users table
    # Insertion Query
    books_query = """
                INSERT INTO books (ISBN, Book_Title, Book_Author, Year_Of_Publication, Publisher, Image_URL_S, Image_URL_M, Image_URL_L)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

    inserted = 271360
    # A list of data types for this table
    books_data_types = [str, str, str, int, str, str, str, str]  # ISBN,Book-Title,Book-Author,Year-Of-Publication,Publisher,Image-URL-S,Image-URL-M,Image-URL-L

    # load data
    # load_data_concurrent('Data/Books.csv', books_query, books_data_types)
    # line_count = count_lines('Data/Books.csv')
    # print(line_count)
    row_count = session.execute("SELECT COUNT(1) FROM books").one()
    print(row_count)
    find_duplicates('Data/Books.csv')
