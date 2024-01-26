-- count rankings per book
SELECT isbn, COUNT(book_rating)
FROM books_rated_by_user
GROUP BY isbn;

-- avg. rating per book
SELECT isbn, AVG(book_rating)
FROM books_rated_by_user
GROUP BY isbn;
-- top rated books: average rating X count of ratings (count times average gives the actual score) - in memory

-- avg book rating per age
SELECT isbn, age, AVG(book_rating), COUNT(book_rating)
FROM books.books_rated_by_user
GROUP BY isbn, age;
-- further aggregate by age in code

-- from here get the top ISBN per age
-- from ISBN get the author using WHERE

-- top author per age - on hold

-- top rated authors @ TODO
-- first we need a dict of isbn-author
SELECT isbn, book_author
FROM books_rated_by_user
GROUP BY 

-- find locations of users with most rankings
SELECT user_id, location, COUNT(book_rating) AS rating_count
FROM users_by_rated_books
GROUP BY user_id;