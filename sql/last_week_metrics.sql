/*
Returns the data for the last 7 days to yesterday.
The table contains the following metrics:
* DAU
* Views
* Likes
* CTR
*/

SELECT
    toDate(time) as cdate,
    count(distinct user_id) as DAU,
    countIf(action='view') as Views,
    countIf(action='like') as Likes,
    (Likes / Views) as CTR
FROM
    simulator_20220920.feed_actions
WHERE
    cdate BETWEEN yesterday() - 7 AND yesterday()
GROUP BY
    cdate