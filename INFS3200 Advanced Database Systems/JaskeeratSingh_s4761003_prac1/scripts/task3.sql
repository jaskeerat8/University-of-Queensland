SELECT replace(v1.fname, '"', '') || ' ' || replace(v1.sname, '"', '') as "full name",
v2.bdate as "DOB"
from USER1_VF_S4761003.athlete_v1 v1
inner join USER2_VF_S4761003.athlete_v2 v2
on v1.athleteid = v2.athleteid
where v1.athleteid <= 450 and v1.athleteid >= 445;
