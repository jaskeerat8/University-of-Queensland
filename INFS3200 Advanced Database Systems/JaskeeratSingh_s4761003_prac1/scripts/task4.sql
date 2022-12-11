select v1.athleteid from user1_vf_s4761003.athlete_v1 v1;

select v2.athleteid, v2.bdate, v2.ccode, v2.sportid
from USER2_VF_S4761003.athlete_v2 v2
inner join
(select v1.athleteid from user1_vf_s4761003.athlete_v1 v1) v1
on v1.athleteid = v2.athleteid where v2.ccode = 'AUS';

select v1.athleteid, v1.fname, v1.sname, v2.bdate, v2.ccode, v2.sportid
from user1_vf_s4761003.athlete_v1 v1
inner join
(
select v2.athleteid, v2.bdate, v2.ccode, v2.sportid
from USER2_VF_S4761003.athlete_v2 v2
inner join
(select v1.athleteid from user1_vf_s4761003.athlete_v1 v1) v1
on v1.athleteid = v2.athleteid
where v2.ccode = 'AUS'
) v2
on v1.athleteid = v2.athleteid;



select * from USER2_VF_S4761003.athlete_v2;

select v1.athleteid, v1.fname, v1.sname, v2.bdate, v2.ccode, v2.sportid from user1_vf_s4761003.athlete_v1 v1
inner join (select * from USER2_VF_S4761003.athlete_v2) v2
on v1.athleteid = v2.athleteid 
where v2.ccode = 'AUS';