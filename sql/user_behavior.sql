/*
return user behavior by user
in average (median)
how many:
* posts user views each day
* posts user likes each day
* msgs user send each day
*/
WITH user_behavior AS (
    SELECT
        toDate(time) AS cdate,
        user_id,
        countIf(user_id, action='view') AS viewed_posts,
        countIf(user_id, action='like') AS liked_posts,
        COUNT(reciever_id) AS messages_sent
        
    FROM simulator_20220920.feed_actions fa
    FULL OUTER JOIN simulator_20220920.message_actions ma
    USING user_id
    WHERE toDate(time) BETWEEN yesterday() - 14 AND yesterday()
    GROUP BY cdate, user_id
)

SELECT 
    cdate,
    median(viewed_posts) AS viewed_posts,
    median(liked_posts) AS liked_posts,
    median(messages_sent) AS messages_sent
FROM 
    user_behavior
GROUP BY
    cdate
    