create table author_ratings
(
    user   int,
    isbn   text,
    rating int,
    author text,
    yop    int,
    primary key ( author, yop, isbn )

);

select avg(rating), count(rating), author, yop
from author_ratings
group by (author, yop);




