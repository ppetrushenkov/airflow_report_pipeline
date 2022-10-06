/* 
Stickness is the percentage metric, that shows how many Daily Active Users we have in relation to Monthly Active Users.
More is better. If the stickness = 100, this means that all Monthly Active Users are Active Daily too.
If stickness = 50 -> only 50% of Monthly Active Users are Daily Active
*/

-- select unique dates and stickness
SELECT 
    DISTINCT cdate,
    DAU / MAU * 100 AS stickness
FROM
    (SELECT 
        toDate(time) AS cdate, -- transform time to Day date (i.e 16/06/22 00:00:00 -> 16/06/22)
        -- Window func, that counts unique users FROM 30 days ago TO today
        COUNT(DISTINCT user_id) OVER (ORDER BY cdate 
                                      RANGE BETWEEN 30 PRECEDING AND CURRENT ROW) AS MAU,
        -- Window func, that counts unique users BY our date column (for each day)
        COUNT(DISTINCT user_id) OVER (PARTITION BY cdate) AS DAU
        
    FROM simulator_20220920.feed_actions
    WHERE cdate BETWEEN yesterday() - 30 AND yesterday())