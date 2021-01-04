1) --rozkłąd wyników komentarze/posty
Select sentiment_label, count(*) as Counter from sample_db.posts_parquet
group by sentiment_label
order by Counter desc;

2)  -- Dzienny rozkład postów - dzienna ilość publikowanych
Select date(creationdatetime) as Date, count(*) as Counter from sample_db.posts_parquet
group by date(creationdatetime)
order by date(creationdatetime);

3) -- 10 najpopularnijeszych tag wszechczasów i ich średni sentymentem
Select tag,avg(avg_score) as Avg_score, count(tag) as Counter
from sample_db.posts_parquet
CROSS JOIN UNNEST(split(tags,'|')) as unnested_table(tag)
where tag != 'N/A'
group by tag
order by Counter desc
limit 10;

4) -- tagi oznaczone najwyższym/najniższym średnim sentymentem
Select tag, avg(avg_score) as Avg_score
from sample_db.posts_parquet
CROSS JOIN UNNEST(split(tags,'|')) as unnested_table(tag)
where tag != 'N/A'
group by tag
order by Avg_score
limit 10;


5) -- Tytuły postów oznaczone najniższym sentymentem zawiarające słowo klucz w poście, bądź tytule
-- np. nazwisko byłego prezydenta Stanów Zjednoczonych tj. Donalda Trumpa
select title, avg_score
from sample_db.posts_parquet
where (lower(title) like '%donald trump%' or lower(body) like '%donald trump%') and title != 'N/A'
order by avg_score
limit 10;


6) -- Średni score komentarzy wybranych postów utworzonych przez użytkowników z Polski
Select p.title, avg(c.avg_score) as Avg_score
from posts_parquet as p
join comments_parquet as c
on p.postid = c.postid
where lower(p.location) like '%poland%' and p.title != 'N/A'
group by p.title;