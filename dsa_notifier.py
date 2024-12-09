import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import schedule
import time
from datetime import datetime
import os
import logging
from dotenv import load_dotenv
import sys

# --------------------------------
# Load Environment Variables
# --------------------------------
load_dotenv()  # Loads variables from .env into environment

# --------------------------------
# Configuration
# --------------------------------
SLACK_TOKEN = os.getenv('SLACK_BOT_TOKEN')
CHANNEL_ID = os.getenv('SLACK_CHANNEL')
CSV_FILE = os.getenv('CSV_FILE', 'DSA_Practice_Questions.csv')
QUESTIONS_PER_DAY = int(os.getenv('QUESTIONS_PER_DAY', 6))
START_DATE_STR = os.getenv('START_DATE', '2024-12-06')
SEND_TIME = os.getenv('SEND_TIME', '09:30')

# Convert START_DATE to datetime object
try:
    START_DATE = datetime.strptime(START_DATE_STR, '%Y-%m-%d')
except ValueError:
    logging.error(f"Invalid START_DATE format: {START_DATE_STR}. Expected YYYY-MM-DD.")
    START_DATE = datetime.now()

# --------------------------------
# Logging Configuration
# --------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("dsa_notifier.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("Starting DSA Notifier script initialization.")

# --------------------------------
# Initialize Slack client
# --------------------------------
if not SLACK_TOKEN:
    logger.error("Slack Bot Token not found. Please set SLACK_BOT_TOKEN in the .env file.")
    exit(1)
if not CHANNEL_ID:
    logger.error("Slack Channel ID not found. Please set SLACK_CHANNEL in the .env file.")
    exit(1)

client = WebClient(token=SLACK_TOKEN)

def ensure_pushed_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure that the 'Pushed' column exists in the DataFrame.
    If it doesn't, add it and default to False for all rows.
    """
    if 'Pushed' not in df.columns:
        logger.info("'Pushed' column not found. Adding it and defaulting to False.")
        df['Pushed'] = False
    else:
        logger.debug("'Pushed' column already exists.")
    return df

def load_questions(csv_file: str) -> pd.DataFrame:
    """
    Load questions from the CSV file and ensure the Pushed column exists.
    """
    logger.debug(f"Attempting to load questions from {csv_file}.")
    try:
        df = pd.read_csv(csv_file)
        df = ensure_pushed_column(df)
        logger.debug("Questions loaded successfully and 'Pushed' column verified.")
        return df
    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_file}. Ensure the file is in the correct location.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"An error occurred while loading the CSV file: {e}")
        return pd.DataFrame()

def save_questions(df: pd.DataFrame, csv_file: str):
    """
    Save the updated DataFrame back to the CSV file.
    """
    logger.debug(f"Saving updated questions DataFrame to {csv_file}.")
    try:
        df.to_csv(csv_file, index=False)
        logger.debug("DataFrame saved successfully.")
    except Exception as e:
        logger.error(f"An error occurred while saving the CSV file: {e}")

def get_today_questions(df: pd.DataFrame, start_date: datetime, questions_per_day: int) -> pd.DataFrame:
    """
    Determine today's questions based on the start date and number of questions per day.
    """
    today = datetime.now().date()
    delta_days = (today - start_date.date()).days
    logger.debug(f"Today: {today}, Start Date: {start_date.date()}, Delta Days: {delta_days}")

    if delta_days < 0:
        logger.info("Current date is before the start date. No questions to send today.")
        return pd.DataFrame()

    start_idx = delta_days * questions_per_day
    end_idx = start_idx + questions_per_day
    logger.debug(f"Selecting questions from index {start_idx} to {end_idx}.")

    if start_idx >= len(df):
        # No more questions available
        logger.info("No more questions available. All questions have been covered.")
        return pd.DataFrame()

    return df.iloc[start_idx:end_idx]

def format_questions(questions_df: pd.DataFrame) -> str:
    """
    Format the questions into a Slack message.
    If questions_df is empty, return a completion message.
    """
    if questions_df.empty:
        logger.info("No questions to format. Possibly all questions completed.")
        return "🎉 *Congratulations!* You've completed all the practice questions. Keep up the great work! 🎉"

    message = "*Today's DSA Practice Questions:* 📚\n"
    for idx, row in questions_df.iterrows():
        question = row['Question']
        topic = row['Topic']
        category = row['Category']
        day_number = (datetime.now().date() - START_DATE.date()).days + 1
        question_number = (day_number - 1) * QUESTIONS_PER_DAY + (idx % QUESTIONS_PER_DAY) + 1
        message += f"\n• *Question {question_number}:* {question}\n  _Topic:_ {topic} | _Category:_ {category}\n"
    return message

def send_slack_message(message: str, channel: str = CHANNEL_ID) -> bool:
    """
    Send a message to Slack and return True if successful, False otherwise.
    """
    logger.debug("Attempting to send Slack message.")
    try:
        response = client.chat_postMessage(channel=channel, text=message)
        logger.info("Message sent successfully.")
        return True
    except SlackApiError as e:
        logger.error(f"Error sending message to Slack: {e.response['error']}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error when sending message to Slack: {e}")
        return False

def job():
    logger.info("Running scheduled job.")
    df = load_questions(CSV_FILE)

    if df.empty:
        logger.warning("DataFrame is empty. Cannot proceed with question selection.")
        send_slack_message("❗ *Error:* Questions data not found or failed to load.")
        return

    today_questions = get_today_questions(df, START_DATE, QUESTIONS_PER_DAY)

    if today_questions.empty:
        logger.info("No questions to send today.")
        send_slack_message("🔔 *DSA Notifier:* No questions to send today.")
        return

    today_indices = today_questions.index
    pushed_values = df.loc[today_indices, 'Pushed']

    if pushed_values.all():
        logger.info("Today's questions are already marked as pushed. No action needed.")
        return
    else:
        logger.info("Today's questions have not been pushed yet. Attempting to send...")
        message = format_questions(today_questions)
        if send_slack_message(message):
            df.loc[today_indices, 'Pushed'] = True
            save_questions(df, CSV_FILE)
            logger.info("Today's questions have been marked as pushed.")
        else:
            logger.warning("Failed to send today's questions. Will retry on next run.")

# --------------------------------
# Scheduling
# --------------------------------
logger.info(f"Scheduling the job to run every day at {SEND_TIME}.")
schedule.every().day.at(SEND_TIME).do(job)

logger.info("DSA Notifier is running... Press Ctrl+C to stop.")

# -------------------------
# Check arguments and possibly run now
# -------------------------
if __name__ == "__main__":
    if "--run-now" in sys.argv:
        job()
        sys.exit(0)

    logger.info(f"Scheduling the job to run every day at {SEND_TIME}.")
    schedule.every().day.at(SEND_TIME).do(job)
    logger.info("DSA Notifier is running... Press Ctrl+C to stop.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("DSA Notifier stopped by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
