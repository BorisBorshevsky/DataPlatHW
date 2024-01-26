CREATE TABLE books.author_books(
	ISBN text,
	book_author text,
    book_title text,
	year_of_publication int,
	PRIMARY KEY((book_author), year_of_publication, ISBN)
);

-- table for author queries.

SELECT book_author,year_of_publication, COUNT (*) AS auth_count 
FROM author_books 
GROUP BY book_author;