#!/usr/bin/env python
# coding: utf-8

# # HW 1 - Data Platform IDC
# ID1: 308564293
# 
# ID2: 311898746

# ## Step 1: Setup Cassandra Cluster locally -

# > Code is in markdown as we don't want to run this from the notebook
# > The setup is oriented for mac env

# Make sure Docker Desktop is installed. If not, download and install Docker from the official website [Get Docker | Docker Docs](https://docs.docker.com/get-docker/).

# Download docker image for cassandra
# ```bash
# docker pull cassandra:latest
# ```

# Run container locally open for conncetion from the notebook
# ```bash
# docker run --volume=/var/lib/cassandra --restart=no -p 127.0.0.1:9042:9042 -p 127.0.0.1:9160:9160 --name hw-cass -d cassandra:latest
# ```

# Connect to run `cqlsh` locally
# ```bash
# docker exec -it hw-cass cqlsh
# ```

# ## Step 2: setup python environment

# install python
# ```bash
# brew install python
# ```

# install required libraries
# 
# Install libraries
# ```bash
# pip3 install cassandra-driver
# pip3 install pandas
# ```

# ## Step 3: Chossing the dataset

# ##### Data set content - [Dataset link.](https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset/data)
# The Book-Crossing dataset has 3 files and is given in .csv form.
# 
# 
# `Users` Has 3 colums:
# * User-ID (int): Anonymized user identifier mapped to integers.
# * Location (text): Demographic information about the user's location.
# * Age (float): Age of the user. May contain NULL-values if not available.
# 
# Rows:  279K, each identifing a user.
# 
# `Books` Has 8 columns:
# * SBN (text): Unique identifier for each book.
# * Book-Title (text): Title of the book.
# * Book-Author (text): Author of the book. In case of multiple authors, only the first one is provided.
# * Year-Of-Publication (int): The year when the book was published.
# * Publisher (text): The publisher of the book.
# * Image-URL-S, Image-URL-M, Image-URL-L (text): URLs linking to cover images in small, medium, and large sizes.
# 
# Rows: 271K, that identify books.
# 
# `Ratings` Has 3 columns:
# * User-ID (int): Anonymized user identifier corresponding to Users table.
# * ISBN (text): Unique identifier corresponding to Books table.
# * Book-Rating (int): User's rating for a particular book. Explicit ratings are on a scale from 1-10, while implicit ratings are expressed as 0.
# 
# Rows: 1.14M, each representing an interaction where a user rated a specific (single) book.
# 
# 
# #### Dataset Selection
# * The dataset was selected for it's very large size (14 columsn and over 1.5M rows) that will allow us to explore Cassandra's strengths with large datasets.
# * The dataset is also realistic, reflecting a real-life scenario of different data soruces and systems that are interconnected. 
# * Data is relatively well organized and clean.
# 
# **In this project:** The data set is stored in the `Data` folder - where the names of the files are corresponding to the explnation above.

# ## Step 4: Cassendra Database design
# Our dataset will consist of three Cassandra tables that correspond to the different entities in the DB: `Ratings`, `Books` and `Users`.
# 
# Each table has a different purpose, information, keys and columns. 
# Further, We can assume each entity has originated form a differnet data collection method, so it makes senese to keep them separated in three tables.
# 
# ##### `book_ratings`
# * **Structure**: User-ID: int, ISBN: text, Book-Rating: int
# * **Primary key**: (User-ID, ISBN)
# * **Partition Key**: User-ID
# * **Clustering Column**: ISBN
# 
# Since we need a pair of user ID and ISBN to identify a transaction, they are both included in the primary key.
#     
# The partition key is selected to be User-ID as it has high cardinality (even spread of data).
# 
# ##### `books`
# * **Structure**: ISBN: text, Book-Title: text, Book-Author: text, Year-Of-Publication: int, Publisher: text, Image-URL-S: text, Image-URL-M: text, Image-URL-L: text
# * **Primary key**: (ISBN, Book-Author, Publisher)
# * **Partition Key**: ISBN
# * **Clustering Column**: Book-Author, Publisher
# 
# ISBN is a unique identifier per book so it's a good partition key. Clustering keys are seleted to be publisher and author which are expected to have multiple entries.
# 
# ##### `users`
# * **Structure**: User-ID: int, Location: text, Age: float (for some cases age was not present, we decided to store this as 0 since age is part of the clustering key)
# * **Primary key**: (User-ID, Location, Age)
# * **Partition Key**: User-ID
# * **Clustering Column**: Location, Age
# 
# User-ID is a unique identifier per user so it's a good partition key. Clustering keys are seleted to be Location and Age which are expected aggregations.
# 
# 

# ## Step 5: Setup keyspace and tables

# First - We would like to load all the libraries need for ingestion and working with the cassandra DB

# In[1]:


# Cassandra-driver
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement

# Data
import csv
import pandas as pd
import concurrent.futures
from collections import namedtuple


# Connect to our cassandra instance.

# In[2]:


# Connect Cassandra-Driver to the Cluster running on the Docker:
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()


# Create out key space `books`.
# We will use `SimpleStrategy` and `replication_factor` = `1` as they serve us well for the purpose of this excsrsize - as we are not looking for any HA or significant scale

# In[3]:


session.execute("CREATE KEYSPACE IF NOT EXISTS books WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")


# Make sure keyspace created

# In[4]:


session.execute("DESCRIBE books;").one()[3]


# Use the keyspace

# In[5]:


session.execute("USE books");


# ##### Table creation: 
# Next, we will create the tables according to the DB schema.

# In[6]:


session.execute("""
    CREATE TABLE IF NOT EXISTS books (
        ISBN text,
        Book_Title text,
        Book_Author text,
        Publisher text,
        Year_Of_Publication int,       
        Image_URL_S text,
        Image_URL_M text,
        Image_URL_L text,
        PRIMARY KEY (ISBN, Book_Author, Publisher)
    )
""").one()


# In[7]:


session.execute("""
    CREATE TABLE IF NOT EXISTS book_ratings (
        User_ID int,
        ISBN text,
        Book_Rating int,
        PRIMARY KEY(User_ID, ISBN)
    )
""").one()


# In[8]:


session.execute("""
    CREATE TABLE IF NOT EXISTS users (
        User_ID int,
        Location text,
        Age float,
        PRIMARY KEY (User_ID, Location, Age)
    )
""").one()


# ##### Validation:

# In[9]:


session.execute("""DESCRIBE tables;""")[0:]


# ## Step 6: Data ingestion

# *Data Ingestion function*
# `load_data` function reads data from a CSV file, splits it into batches, and inserts the data into a table using concurrency.
# 
# Batches and concurrency are important for improving the performance and efficiency of inserting large amounts of data into a database table.
# 
# When inserting data into a database table, it is possible to insert row by row, or to insert multiple rows at once as a batch. Inserting data in a batch greatly improves the performance of inserting data into the database, as it reduces the number of round trips to the database and can ensure that the data is consistent. This is more efficient than inserting one row at a time, which can be slow and can lead to unnecessary overhead.
# 
# Concurrency is important because it allows multiple threads to be used when inserting data into a database table. This improves performance by allowing multiple inserts to happen simultaneously, which can greatly increase the speed of inserting large amounts of data. Without concurrency, each insert operation would have to wait for the previous insert to complete, leading to a slower overall process.
# 
# In summary, using batches and concurrency can greatly improve the performance and efficiency of inserting large amounts of data into a database table, resulting in faster insert times and better use of system resources.
# 
# 

# In[26]:


# Helper function to construct the batch operation and then run concurrently
def load_data_concurrent(csv_file, insert_query, data_types, batch_size=100, concurrency=20, max_batches=None):        
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
        # Convert a single row's data to the correct format according to the data_types
        
        converted_data = [data_type(value) if value else data_type() for value, data_type in zip(row, data_types)]
        print(converted_data)
        return tuple(converted_data)

    def build_batch(rows):
        # Build a batch statement for the current set of rows (single batch)
        batch = BatchStatement()
        for row in rows:
            # Convert data types
            converted_row = convert_data(row)
            # Create the query and add it to the batch
            #print(converted_row)
            batch.add(insert_query, converted_row)
        session.execute(batch, trace=True)

    # Function body
    # open CSV
    with open(csv_file, "r") as f:
        next(f)  # Skip the header row.
        reader = list(csv.reader(f))

    # split to batches
    rows = [reader[i: i + batch_size] for i in range(0, len(reader), batch_size)][:max_batches]

    for r in rows:
        build_batch(r)
    # concurrently run each batch
    # with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
    #     executor.map(build_batch, rows)


# ##### Define the files

# In[11]:


Files = namedtuple("Files", "users ratings books")

files = Files(
    users='Data/Users.csv',
    ratings='Data/Ratings.csv',
    books='Data/Books.csv'
)


# ##### Investigate the content

# In[15]:


def count_lines(file_name):
    with open(file_name, "r") as file:
        next(file)
        return len(file.readlines())


# In[17]:


users_file_size = count_lines(files.users)
ratings_file_size = count_lines(files.ratings)
books_file_size = count_lines(files.books)

print(f"file sizes are - users: {users_file_size}, ratings: {ratings_file_size}, books: {books_file_size}")


# In[18]:


def num_of_records(table):
    count_query = f"SELECT COUNT(1) FROM {table}"
    result = session.execute(count_query)
    return result.one().count


# #### Data Ingestion:
# Now we will ingest the data using the `load_data_concurrent` fucntion and validate for each table that the number of rows matches the number in the CSV.
# 
# ##### `book_ratings`:

# In[19]:


## book_ratings table
# Insertion Query
ratings_query = """
            INSERT INTO book_ratings (User_ID, ISBN, Book_Rating)
            VALUES (%s, %s, %s)
            """

# A list of data types for this table
ratings_data_types = [int, str, int]  # User_ID, ISBN, Book_Rating

# load data
load_data_concurrent(files.ratings, ratings_query, ratings_data_types)


# ##### Book ratings - Validate all inserted

# In[20]:


# Row count validation
cass_len = num_of_records("book_ratings")
line_count = count_lines(files.ratings)

assert cass_len == line_count


# ##### `users`:

# In[27]:


# Insertion Query
users_query = """
                INSERT INTO users (User_ID, Location, Age)
                VALUES (%s, %s, %s)
                """

# A list of data types for this table
users_data_types = [int, str, float]  # User_ID, Location, Age

# load data
load_data_concurrent(files.users, users_query, users_data_types)


# Validation:

# In[28]:


# Row count validation
row_count = num_of_records("users")
line_count = count_lines(files.users)

assert row_count == line_count , f"{row_count} != {line_count}"


# ##### `books`:

# In[ ]:


## books table
# Insertion Query
books_query = """
            INSERT INTO books (ISBN, Book_Title, Book_Author, Year_Of_Publication, Publisher, Image_URL_S, Image_URL_M, Image_URL_L)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

# A list of data types for this table
books_data_types = [str, str, str, int, str, str, str, str]  # ISBN,Book-Title,Book-Author,Year-Of-Publication,Publisher,Image-URL-S,Image-URL-M,Image-URL-L

# load data
load_data_concurrent(files.books, books_query, books_data_types, batch_size=100)


# ##### Validation - Books table - all data is inserted

# In[ ]:


# Row count validation
row_count = session.execute("SELECT COUNT(1) FROM books").one().count
line_count = count_lines(files.books)

assert row_count == line_count - 1, f"{row_count} != {line_count - 1}"


# ## Step 3: Creating Tables

# In[ ]:


USE university;


# In[ ]:


CREATE TABLE students (
  student_id UUID PRIMARY KEY,
  name TEXT,
  email TEXT,
  enrollment_year INT
);


# In[ ]:


CREATE TABLE courses (
  course_id UUID PRIMARY KEY,
  course_name TEXT,
  lecturer_id UUID,
  credits INT
);


# In[ ]:


CREATE TABLE lecturers (
  lecturer_id UUID PRIMARY KEY,
  name TEXT,
  department TEXT
);


# In[ ]:


CREATE TABLE course_enrollments (
  course_id UUID,
  student_id UUID,
  enrollment_date DATE,
  PRIMARY KEY (course_id, student_id)
);


# In[ ]:


# To validate you created the tables
cqlsh:university> DESCRIBE tables;


# In[ ]:


# The output should be:
course_enrollments  courses  lecturers  students


# ## Step 5: Querying the data

# In[ ]:


# Get all students
SELECT * FROM STUDENTS

>>
 student_id                           | email                | enrollment_year | name
--------------------------------------+----------------------+-----------------+---------------
 753e8600-e49d-53e6-c628-668655460002 |                 null |            2019 | Alice Johnson
 652e8500-f39c-42d5-b517-557655450001 | jane.smith@email.com |            2020 |    Jane Smith
 550e8400-e29b-41d4-a716-446655440000 |   john.doe@email.com |            2021 |      John Doe
 (3 rows)


# In[ ]:


# Get the list of students who enrolled after 2020 along with their email and the courses they enrolled in
SELECT student_id, name, email
FROM students
WHERE enrollment_year > 2020 ALLOW FILTERING;

>>
 student_id                           | name     | email
--------------------------------------+----------+--------------------
 550e8400-e29b-41d4-a716-446655440000 | John Doe | john.doe@email.com

(1 rows)


# In[ ]:


# Find all courses and count of students enrolled in them
SELECT course_id, COUNT(student_id) as student_count
FROM course_enrollments
GROUP BY course_id;

>>
 course_id                            | student_count
--------------------------------------+---------------
 183fa000-f821-93a0-b062-a96655650006 |             1
 385fc000-d043-b5e2-f284-c98855670008 |             1
 284fb000-a932-a4b1-c173-b97755660007 |             1

(3 rows)

