import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import schedule
import time
import os
import logging
from dotenv import load_dotenv
import sys

# --------------------------------
# Load Environment Variables
# --------------------------------
load_dotenv()

# --------------------------------
# Configuration
# --------------------------------
SLACK_TOKEN = os.getenv('SLACK_BOT_TOKEN')
CHANNEL_ID = os.getenv('SLACK_CHANNEL')
CSV_FILE = os.getenv('CSV_FILE', 'DSA_Practice_Questions.csv')
QUESTIONS_PER_DAY = int(os.getenv('QUESTIONS_PER_DAY', 6))
SEND_TIME = os.getenv('SEND_TIME', '10:00')

# --------------------------------
# Logging Configuration
# --------------------------------
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for detailed logs
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("dsa_notifier.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

logger.info("Starting DSA Notifier script initialization.")

if not SLACK_TOKEN:
    logger.error("Slack Bot Token not found. Please set SLACK_BOT_TOKEN in the .env file.")
    sys.exit(1)
if not CHANNEL_ID:
    logger.error("Slack Channel ID not found. Please set SLACK_CHANNEL in the .env file.")
    sys.exit(1)

client = WebClient(token=SLACK_TOKEN)


def load_questions(csv_file: str) -> pd.DataFrame:
    logger.debug(f"Attempting to load questions from {csv_file}.")
    try:
        df = pd.read_csv(csv_file)
        logger.debug(f"Questions loaded successfully. Total questions: {len(df)}.")
        return df
    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_file}. Please ensure the file exists.")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing CSV file: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading the CSV: {e}")
        return pd.DataFrame()


def save_questions(df: pd.DataFrame, csv_file: str):
    logger.debug(f"Saving updated questions to {csv_file}.")
    try:
        df.to_csv(csv_file, index=False)
        logger.debug("CSV file saved successfully.")
    except Exception as e:
        logger.error(f"Failed to save CSV file: {e}")


def get_next_questions(df: pd.DataFrame, questions_per_day: int) -> pd.DataFrame:
    logger.debug("Identifying the last pushed question.")

    start_idx = (df["Pushed"] != "True").idxmax()
    end_idx = start_idx + questions_per_day

    logger.debug(f"Selecting questions from index {start_idx} to {end_idx}.")

    next_questions = df.iloc[start_idx:end_idx]

    if next_questions.empty:
        logger.info("No more unpushed questions available.")
    else:
        logger.debug(f"Selected {len(next_questions)} questions to push.")

    return next_questions


def format_questions(questions_df: pd.DataFrame) -> str:
    if questions_df.empty:
        logger.info("No questions to format. Preparing completion message.")
        return "🎉 *Congratulations!* You've completed all the practice questions. Keep up the great work! 🎉"

    message = "*Today's DSA Practice Questions:* 📚\n"
    for idx, row in questions_df.iterrows():
        question_number = idx + 1  # Assuming 0-based index
        question = row['Question']
        topic = row['Topic']
        category = row['Category']
        message += f"\n• *Question {question_number}:* {question}\n  _Topic:_ {topic} | _Category:_ {category}\n"

    return message


def send_slack_message(message: str, channel: str = CHANNEL_ID) -> bool:
    logger.debug("Attempting to send message to Slack.")
    try:
        response = client.chat_postMessage(channel=channel, text=message)
        if response['ok']:
            logger.info("Message sent successfully to Slack.")
            return True
        else:
            logger.error(f"Failed to send message to Slack: {response['error']}")
            return False
    except SlackApiError as e:
        logger.error(f"Slack API Error: {e.response['error']}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while sending message to Slack: {e}")
        return False


def job():
    logger.info("Running scheduled job.")
    df = load_questions(CSV_FILE)

    if df.empty:
        logger.warning("No data loaded from CSV. Exiting job.")
        send_slack_message("❗ *DSA Notifier Error:* Questions data not found or failed to load.")
        return

    next_questions = get_next_questions(df, QUESTIONS_PER_DAY)

    if next_questions.empty:
        logger.info("No new questions to send.")
        send_slack_message("🔔 *DSA Notifier:* No new questions to send today.")
        return

    # Prepare message
    message = format_questions(next_questions)

    if send_slack_message(message):
        df.loc[next_questions.index, 'Pushed'] = True
        save_questions(df, CSV_FILE)
        logger.info(f"Marked questions {next_questions.index.min() + 1} to {next_questions.index.max() + 1} as pushed.")
    else:
        logger.warning("Failed to send messages to Slack. Will retry on next run.")


# --------------------------------
# Scheduling
# --------------------------------
def schedule_job():
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
        logger.error(f"An unexpected error occurred in the scheduler: {e}")


# -------------------------
# Command-Line Argument Handling
# -------------------------
if __name__ == "__main__":
    if "--run-now" in sys.argv:
        logger.info("Running job immediately as per '--run-now' argument.")
        job()
        sys.exit(0)

    schedule_job()
