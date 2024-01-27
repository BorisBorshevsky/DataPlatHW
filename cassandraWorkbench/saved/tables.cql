CREATE TABLE IF NOT EXISTS books.books(
	book_author text,
	book_title text,
	image_url_l text,
	image_url_m text,
	image_url_s text,
	isbn text,
	publisher text,
	yop int,
	PRIMARY KEY(isbn)
);

CREATE TABLE IF NOT EXISTS books.users(
	age float,
	location text,
	user_id int,
	PRIMARY KEY(user_id)
);

create table books_rated_by_user
(
    user_id   int,
    age int,
    location text,
    isbn   text,
    book_author text,
    yop int,
    publisher text,
    book_rating int,
    primary key ((isbn), location, user_id)
);


create table users_by_rated_books
(
    user_id   int,
    age int,
    location text,
    isbn   text,
    book_author text,
    yop int,
    publisher text,
    book_rating int,
    primary key ((user_id), book_author, isbn)
);

    CREATE TABLE IF NOT EXISTS ratings_by_age (
        user_id int,
        isbn text,
        book_rating int,

        age int,
        age_group text,
        book_author text,
        publisher text,
        
        primary key (age_group, book_author, isbn, user_id)
    )