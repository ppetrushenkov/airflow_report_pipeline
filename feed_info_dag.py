import sys
sys.path.insert(0, "/var/lib/airflow/airflow/airflow.git/dags/airflow-alerts/dags/p-petrushenkov-10")

from datetime import (datetime, timedelta)
from airflow.decorators import dag, task
from config import TOKEN_NAME, CUR_DIR, MY_CHAT_ID, CONNECTION

import io
import telegram
import pandas as pd
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns

plt.style.use("bmh")


DEFAULT_DAG_ARGS = dict(owner='p-petrushenkov-10',
                        depends_on_past=False,
                        retries=2,
                        retry_delay=timedelta(minutes=5),
                        start_date=datetime(2022, 9, 9))

# ============== INIT BOT ==============
bot = telegram.Bot(token=TOKEN_NAME)


# ============== Functions ==============
def get_query(path_to_query: str) -> str:
    """Return the query as string from the certain path"""
    full_path = CUR_DIR + '/sql/' + path_to_query
    with open(full_path, "r") as f:
        query = f.read()
    return query


def get_data(query: str) -> pd.DataFrame:
    """Return DataFrame for specified query"""
    return ph.read_clickhouse(query, connection=CONNECTION)


def form_info(last: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    """
    Form info table of data:

    * Average metrics for previous week
    * Metrics before yesterday
    * Metrics for yesterday
    """
    indices = [
        "Prev. week (avg)",
        "Yesterday - 1",
        "Yesterday"
    ]
    last_two_days = last.iloc[-2:, 1:]
    formed_info = pd.concat([prev, last_two_days], axis=0)
    formed_info.index = indices
    return formed_info.round(2).reset_index()


def get_table_pic(last: pd.DataFrame, prev: pd.DataFrame) -> io.BytesIO:
    """Return table as picture object"""
    info = form_info(last, prev)
    N = info.shape[0]

    fig, ax1 = plt.subplots(figsize=(10, 2 + N / 2.5))
    table = ax1.table(
        cellText=info.values,
        cellLoc='center',
        rowLoc='center',
        colLabels=info.columns,
        colColours=['lavender'] * info.shape[1],
        loc='center'
    )
    table.scale(1, 2)
    table.auto_set_font_size(False)
    table.set_fontsize(13)
    ax1.axis('off')
    title = "Short metric summary"
    ax1.set_title(title, weight='bold', size=16, color='k')

    plot_object = io.BytesIO()
    plt.savefig(plot_object, dpi=300, bbox_inches='tight')
    plot_object.seek(0)
    plot_object.name = 'short_info.png'
    plt.close()
    return plot_object


# ============== DAG ==============
@dag(default_args=DEFAULT_DAG_ARGS,
     schedule_interval="25 11 * * *",  # at 11:25
     catchup=False)
def statixx_feed_info():
    @task()
    def extract_last_week_data():
        """
        Return daily metrics for last week:
        * date
        * DAU (Daily Active Users)
        * Views
        * Likes
        * CTR (Likes / Views)

        (8 rows of data)
        """
        q = get_query("last_week_metrics.sql")
        return get_data(q)

    @task()
    def extract_prev_week_data():
        """
        Return average metrics for previous week:
        * DAU: Average DAU
        * Views: Average Views
        * Likes: Average likes
        * CTR: Average CTR
        (1 row of data)
        """
        q = get_query("prev_week_avg_metrics.sql")
        return get_data(q)

    @task()
    def send_info(last: pd.DataFrame, prev: pd.DataFrame, chat_id: str):
        """Send info table as picture"""
        prev_day = datetime.today() - timedelta(1)
        str_date = prev_day.date().strftime('%d-%m-%Y')

        bot.sendMessage(text=f"[StatixX] A short summary of feed metrics for {str_date}",
                        chat_id=chat_id)

        photo = get_table_pic(last, prev)
        bot.sendPhoto(chat_id=chat_id, photo=photo)

    @task()
    def send_plots(data: pd.DataFrame, chat_id: str):
        """Form subplot of 4 metrics and send it to specified chat_id"""
        fig, ax = plt.subplots(2, 2, figsize=(10, 10))
        fig.suptitle('Metrics for the last 7 days', weight='bold', size=16, color='k')
        sns.lineplot(x=data['cdate'], y=data['DAU'], ax=ax[0, 0], marker="o")
        sns.lineplot(x=data['cdate'], y=data['CTR'], ax=ax[0, 1], marker="o")
        sns.lineplot(x=data['cdate'], y=data['Likes'], ax=ax[1, 0], marker="o")
        sns.lineplot(x=data['cdate'], y=data['Views'], ax=ax[1, 1], marker="o")

        ax[0, 0].title.set_text('DAU')
        ax[0, 1].title.set_text('CTR')
        ax[1, 0].title.set_text('Likes')
        ax[1, 1].title.set_text('Views')
        fig.autofmt_xdate()
        plt.tight_layout()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'last_week_daily_metrics.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    # 1. Extract data
    last_week = extract_last_week_data()
    prev_week = extract_prev_week_data()

    # 2. Send info and plots
    send_info(last_week, prev_week, chat_id=MY_CHAT_ID)
    send_plots(last_week, chat_id=MY_CHAT_ID)


dag = statixx_feed_info()
