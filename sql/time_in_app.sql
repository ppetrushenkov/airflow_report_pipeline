
/* Return table with all user actions for last month grouped by user and day.
user_id: user ID
cdate: current date (day)
unix_by_day: array with all user actions (as unix timestamp) at the current day
*/
WITH user_actions AS (
    SELECT 
        user_id, 
        toDate(time) as cdate,
        arraySort(groupArray(toUnixTimestamp(time))) as unix_by_day
    FROM 
        simulator_20220820.feed_actions
    FULL OUTER JOIN
        simulator_20220820.message_actions
    USING
        user_id
    WHERE toDate(time) >= today() - 30
    GROUP BY user_id, cdate
    ORDER BY cdate, user_id
),
/*
Finds the difference in 'unix_by_day' array, then filter out 
the values greater 10 minutes (10min * 60 to transform it in seconds),
as we assume if distance between user actions was greater 10 minutes,
then the user didn't use the app.
Return the SUM of filtred difference array as the value of the seconds 
the user spends in the application.
*/
time_table AS (
    SELECT
        user_id,
        cdate,
        arrayDifference(unix_by_day) as diff,
        arrayFilter(x -> x < (10 * 60), diff) as filtred_diff,
        arraySum(filtred_diff) as time_spended
    FROM
        user_actions
)

SELECT 
    cdate,
    avg(time_spended) as avg_seconds_spended,
    avg_seconds_spended / 60 as minutes_in_app,
    minutes_in_app / 60 as hours_in_app
FROM
    time_table
WHERE 
    cdate != today()
GROUP BY
    cdate