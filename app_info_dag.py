import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, "/var/lib/airflow/airflow/airflow.git/dags/airflow-alerts/dags/p-petrushenkov-10")

from airflow.decorators import dag, task
from functions import *
from config import *

import io
import telegram
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

plt.style.use('bmh')

# ============== INIT BOT ==============
bot = telegram.Bot(token=TOKEN_NAME)


@dag(default_args=DEFAULT_DAG_ARGS,
     schedule_interval="30 11 * * *",  # At 11:30 (don't want to 11:00)
     catchup=False)
def statixx_app_info():
    """
    This function send information about an app in the telegram chat.
    The information will be lead the following pattern:
    * Turnover table and plot
    * User metrics table and plot
    * User behavior and plot

    Description:
    ------------
    1. Turnover (How many for last 30 days):
    * new users we got yesterday
    * users retained yesterday
    * users that doesn't return in app yesterday

    2. User Metrics (for last 30 days)
    * Retention metric
    * Stickiness metric

    3. User behavior:
    * How many posts users see each day
    * How many posts users like each day
    * How many messages users send each day
    """

    @task()
    def extract_turnover() -> pd.DataFrame:
        """Return turnover data for the last month.
        Turnover (How many for last 30 days):
        * new users we got yesterday
        * users retained yesterday
        * users that doesn't return in app yesterday
        """
        turnover = get_data("daily_turnover.sql")
        turnover.set_index('cdate', inplace=True)
        return turnover

    @task()
    def extract_metrics() -> pd.DataFrame:
        """Return user metrics for the last month.
        User Metrics (for last 30 days)
        * Retention metric
        * Stickiness metric
        """
        retention = get_data("retention.sql")
        stickiness = get_data("stickiness.sql")
        metrics = retention.merge(stickiness, on='cdate')
        metrics.set_index('cdate', inplace=True)
        return metrics

    @task()
    def extract_behavior() -> pd.DataFrame:
        """Return user behavior data for the last month.
        User behavior:
        * How many posts users see each day
        * How many posts users like each day
        * How many messages users send each day
        """
        behavior = get_data("user_behavior.sql")
        behavior.set_index('cdate', inplace=True)
        return behavior

    @task()
    def send_title() -> None:
        prev_day = datetime.today() - timedelta(1)
        str_date = prev_day.date().strftime('%d-%m-%Y')

        bot.sendMessage(
            text=f"[StatixX] Short metric summary for the whole app for {str_date}",
            chat_id=MY_CHAT_ID)

    @task()
    def send_turnover(turnover: pd.DataFrame):
        pivot_turnover = pd.pivot_table(data=turnover, index="cdate", columns='status').sort_values(by="cdate")
        pivot_turnover.columns = ['gone', 'new', 'retained']
        pivot_turnover = pivot_turnover.iloc[:-1, :]  # get rid of last week
        send_info(pivot_turnover, "User turnover", MY_CHAT_ID)
        send_plot(pivot_turnover, "User turnover", MY_CHAT_ID, style="stacked")

    @task()
    def send_metrics(metrics: pd.DataFrame):
        send_info(metrics, "Retention metrics", MY_CHAT_ID)
        send_plot(metrics, "Retention metrics", MY_CHAT_ID)

    @task()
    def send_behavior(behavior: pd.DataFrame):
        send_info(behavior, "Actions by users in average", MY_CHAT_ID)
        send_plot(behavior, "Actions by users in average", MY_CHAT_ID, style="stacked")

    def send_info(info: pd.DataFrame, info_type: str, chat_id: str):
        """Send info table as picture"""
        avg_info = get_avg_info(info)
        avg_info.reset_index(inplace=True)
        photo = get_table_pic(avg_info, info_type)
        bot.sendPhoto(chat_id=chat_id, photo=photo)

    def send_plot(data: pd.DataFrame, info_type: str, chat_id: str, style="bar"):
        """Form subplot of metrics and send it to specified chat_id"""
        fig, ax = plt.subplots(data.shape[1], 1, figsize=(15, 10))
        fig.suptitle(f'{info_type} for 2 weeks', weight='bold', size=16, color='k')

        if style == "stacked":
            fig, ax = plt.subplots(figsize=(12, 6))
            data.plot(kind='bar', stacked=True, ax=ax)
            ax.set_xticklabels([t.get_text().split(" ")[0] for t in ax.get_xticklabels()])
            plt.xticks(rotation=45)
        else:
            for i, col in enumerate(data.columns):
                if style == "bar":
                    sns.barplot(x=data.index, y=data[col], ax=ax[i])
                elif style == "line":
                    sns.lineplot(data=data, x=data.index, y=col, ax=ax[i])
                else:
                    print("[INFO] Choose the style: ['bar', 'line']")
                    return
                ax[i].title.set_text(col)
                ax[i].xaxis.set_major_locator(ticker.LinearLocator(10))

        fig.autofmt_xdate()
        plt.tight_layout()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'last_2weeks_daily_metrics.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    # 1. Extract data
    turnover = extract_turnover()
    metrics = extract_metrics()
    behavior = extract_behavior()

    # 2. Send data
    send_title()
    send_turnover(turnover)
    send_metrics(metrics)
    send_behavior(behavior)


dag = statixx_app_info()
