/*
To calculate retention for 20 days we filter out only those users, whose registration date was 20 days ago.
Then we check, how many of them continue to use our app.

Select the date of user appearance and count of users, that still with us.
*/

WITH action_table AS (
    SELECT
        DISTINCT user_id,     -- get unique users
        toDate(time) AS cdate -- get their appearance date
    FROM
        simulator_20220920.feed_actions
    WHERE user_id IN (
        SELECT user_id
        FROM simulator_20220920.feed_actions
        GROUP BY user_id
        HAVING MIN(toDate(time)) = yesterday() - 30)
),
retention_table AS (
    SELECT
        cdate,
        COUNT(user_id) AS retention
    FROM action_table
    WHERE cdate != today()
    GROUP BY cdate
),
(
    SELECT MAX(retention)
    FROM retention_table
) AS max_retention


SELECT
    cdate,
    retention * 100 / max_retention AS retention
FROM
    retention_table