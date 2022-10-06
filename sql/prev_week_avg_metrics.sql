/*
Returns the mean metric values for the last week
*/

SELECT
    avg(dau) as DAU,
    avg(views) as Views,
    avg(likes) as Likes,
    avg(ctr) as CTR
FROM (
    SELECT
        toDate(time) as cdate,
        count(distinct user_id) as dau,
        countIf(action='view') as views,
        countIf(action='like') as likes,
        (likes / views) as ctr
        
    FROM
        simulator_20220820.feed_actions
    WHERE
        cdate between yesterday() - 14 and yesterday() - 7
    GROUP BY
        cdate
)