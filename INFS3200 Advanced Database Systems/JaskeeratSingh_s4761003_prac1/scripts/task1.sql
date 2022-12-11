select count(*) from user_s4761003.athlete2 where ccode = 'AUS';

select sportid as "Sport ID", count(*) as "Count" from USER_S4761003.athlete3
where ccode = 'FRA'
GROUP BY sportid;

create table user_s4761003.ATHLETE_FULL AS
select * from user_s4761003.athlete1
union
select * from user_s4761003.athlete2
union
select * from user_s4761003.athlete3;

select count(*) as "Count" 
from USER_S4761003.athlete_full af
inner join USER_S4761003.country c
on af.ccode = c.ccode 
where c.continent = 'AF';
