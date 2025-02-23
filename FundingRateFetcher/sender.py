import json
import logging
import pandas as pd
import pytz
import html
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
)

from exchange import ExchangeManager
from pipeline import PipelineMerger
from table import TableViewer

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def load_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file {file_path} not found.")
        return {}
    except json.JSONDecodeError:
        logging.error("Error decoding JSON from the configuration file.")
        return {}


config = load_config("config.json")
alphawave_bot = config.get("alphawave_bot_token")
alphawave_group_chat_id = config.get("alphawave_group_chat_id")


def create_viewer(**kwargs) -> TableViewer:
    exch_mgr = ExchangeManager()
    pipeline = PipelineMerger.load_pipeline(
        exch_mgr=exch_mgr, get_fr=True, get_lm=True, get_ba=True
    )
    return TableViewer(
        exch_mgr=exch_mgr,
        pipeline=pipeline,
        data_map=pipeline.data_map,
        base_exch=kwargs.get("exch_name", "hyperliquid"),
        timezone=kwargs.get("tz", "Asia/Seoul")
    )


def chunk_text(text: str, chunk_size: int = 4000):
    for i in range(0, len(text), chunk_size):
        sub = text[i:i+chunk_size]
        if len(sub.strip()) == 0:
            continue
        yield sub


async def do_update(context: ContextTypes.DEFAULT_TYPE):
    logging.info(
        "Updating data (ExchangeManager -> PipelineMerger -> TableViewer) ...")
    viewer = create_viewer()
    context.application.bot_data["viewer"] = viewer
    logging.info("Update done.")


async def cmd_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await do_update(context)
    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text="Data has been updated manually."
    )


async def job_update(context: ContextTypes.DEFAULT_TYPE):
    await do_update(context)
    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text="Data has been updated automatically."
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_text = (
        "Hello! The following commands are:\n"
        "/info - Show info table\n"
        "/fund - Show funding table\n"
        "/update - Manually re-load data\n"
        "/help - Show usage instructions\n"
        "(All messages will be sent to this group.)"
    )
    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text=message_text
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "Command Reference:\n"
        "/start - Basic greeting\n"
        "/info - Retrieves the info table\n"
        "/fund - Retrieves the funding table\n"
        "/update - Forces an immediate data update\n"
        "/help - Displays this help message\n\n"
        "The data is automatically updated every hour, 3 minutes before the hour (HH:57 KST)."
    )
    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text=help_text
    )


async def cmd_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    viewer: TableViewer = context.bot_data["viewer"]
    df = viewer.get_info_table()
    table_str = df.to_markdown(index=True, tablefmt="pipe")
    escaped_str = html.escape(table_str)
    for chunk in chunk_text(escaped_str, 3000):
        msg = f"<pre>{chunk}</pre>"
        await context.bot.send_message(
            chat_id=alphawave_group_chat_id,
            text=msg,
            parse_mode="HTML"
        )


async def cmd_fund(update: Update, context: ContextTypes.DEFAULT_TYPE):
    viewer: TableViewer = context.bot_data["viewer"]
    df = viewer.get_funding_table(hours_ahead=8, tolerance_minutes=30)
    if df.empty:
        await context.bot.send_message(
            chat_id=alphawave_group_chat_id,
            text="=== Funding Table is empty ==="
        )
        return
    table_str = df.to_markdown(index=True, tablefmt="pipe")
    escaped_str = html.escape(table_str)
    for chunk in chunk_text(escaped_str, 3000):
        msg = f"<pre>{chunk}</pre>"
        await context.bot.send_message(
            chat_id=alphawave_group_chat_id,
            text=msg,
            parse_mode="HTML"
        )


def main():
    viewer = create_viewer()
    application = ApplicationBuilder().token(alphawave_bot).build()
    application.bot_data["viewer"] = viewer

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("info", cmd_info))
    application.add_handler(CommandHandler("fund", cmd_fund))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("update", cmd_update))

    tz_seoul = pytz.timezone("Asia/Seoul")
    now = datetime.now(tz_seoul)
    target = now.replace(minute=57, second=0, microsecond=0)
    if now >= target:
        target += timedelta(hours=1)
    first_delay = (target - now).total_seconds()
    job_queue = application.job_queue
    job_queue.run_repeating(job_update, interval=3600, first=first_delay)

    application.run_polling()


if __name__ == "__main__":
    main()
