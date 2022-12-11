BEGIN
    update USER1_HF_FULL_S4761003.athlete1_replica1 set CCODE = 'AUS' where ATHLETEID = 128;
    update USER2_HF_FULL_S4761003.athlete1_replica2 set CCODE = 'AUS' where ATHLETEID = 128;
    update USER3_HF_FULL_S4761003.athlete1_replica3 set CCODE = 'AUS' where ATHLETEID = 128;
END;
/
COMMIT;


BEGIN
    update USER1_HF_PA_S4761003.athlete1_replica1 set CCODE = 'AUS' where ATHLETEID = 128;
    update USER2_HF_PA_S4761003.athlete1_replica2 set CCODE = 'AUS' where ATHLETEID = 128;
END;
/
COMMIT;


BEGIN
    update USER1_HF_NO_S4761003.athlete1_replica1 set CCODE = 'AUS' where ATHLETEID = 128;
END;
/
COMMIT;
