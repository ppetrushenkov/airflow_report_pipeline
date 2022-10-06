/* New, retained and gone users */

/* Return table with users, that didn't return to the app.
* weeks_visited: array, that contains unique dates, rounded to Mondays
:arrayJoin: unzip array, so in each row we have array value
* current_week: return next week for each unzipped date
* previous_week: return previous week for each unzipped date
* status: if the current week is in the weeks_visited (our Mondays activity data),
then the user is retained, else he's gone */
WITH gone_users AS (
    SELECT
        user_id,
        groupUniqArray(toMonday(toDate(time))) as weeks_visited,
        addWeeks(arrayJoin(weeks_visited), +1) as current_week,
        addWeeks(current_week, -1) as previous_week,
        if(has(weeks_visited, current_week) = 1, 'retained', 'gone') as status
    FROM
        simulator_20220920.feed_actions
    GROUP BY
        user_id
    HAVING
        status = 'gone' AND
        current_week != addWeeks(toMonday(today()), 1)
),
/* The same actions here, but now query return users, that are retained or new in app.
* weeks_visited: array, that contains unique dates (mondays)
* current_week: just unzipped values from the 'weeks_visited' array
* previous_week: current_week - 1
* status: if previous week is in weeks_visited, this means the user was active, so he's retained,
otherwise he's a new one. */
retained_and_new_users AS (
    SELECT
        user_id,
        groupUniqArray(toMonday(toDate(time))) as weeks_visited,
        arrayJoin(weeks_visited) as current_week,
        addWeeks(current_week, -1) as previous_week,
        if(has(weeks_visited, previous_week) = 1, 'retained', 'new') as status
    FROM simulator_20220920.feed_actions
    GROUP BY user_id
)

SELECT
    current_week as "Week",
    status as "Status",
    -uniq(user_id) as "Number of users"
FROM
    gone_users gu
GROUP BY
    current_week, status
HAVING
    current_week != toMonday(today()) -- get rid of last week data

UNION ALL

SELECT
    current_week as "Week",
    status as "Status", toInt64(uniq(user_id)) as "Number of users"
FROM
    retained_and_new_users rnu
GROUP BY
    current_week, status
HAVING
    current_week != toMonday(today()) -- get rid of last week data