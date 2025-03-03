import logging
import datetime
import asyncio

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler
)

from tools import Tools
from exchange import ExchangeManager
from pipeline import PipelineMerger
from table import TableViewer

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

config = Tools.load_config("config.json")
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


def escape_md(text: str) -> str:
    text = text.replace("\\", "\\\\")
    text = text.replace("`", "\\`")
    text = text.replace("_", "\\_")
    return text


async def do_update(context: ContextTypes.DEFAULT_TYPE):
    logging.info(
        "Updating data (ExchangeManager -> PipelineMerger -> TableViewer) ...")
    viewer = await asyncio.to_thread(create_viewer)
    context.application.bot_data["viewer"] = viewer
    logging.info("Update done.")


async def job_update(context: ContextTypes.DEFAULT_TYPE):
    await do_update(context)
    next_run = datetime.datetime.now() + datetime.timedelta(minutes=1)
    msg = f"Updating: estimated: {next_run.strftime('%Y-%m-%d %H:%M:%S')}"
    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text=msg
    )
    logging.info(msg)


async def cmd_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await do_update(context)
    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text="Data has been updated manually."
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_text = (
        "Hello! Available commands:\n"
        "/table - Show table (ticker/exch1/exch2/ER) with details on click\n"
        "/update - Manually re-load data\n"
        "/help - Show usage instructions\n\n"
        "Data automatically updates every 1 minute."
    )
    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text=message_text
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "Command Reference:\n"
        "/table - Retrieves simplified table. Click ticker/exch1/exch2 for details.\n"
        "/update - Forces an immediate data update\n"
        "/help - Displays help message\n\n"
        "Data automatically updates every 1 minute."
    )
    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text=help_text
    )


async def cmd_table(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_table(context)


async def detail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    if not data.startswith("DETAIL|"):
        return

    row_idx_str = data.split("|")[1]
    try:
        row_idx = int(row_idx_str)
    except ValueError:
        await context.bot.send_message(
            chat_id=alphawave_group_chat_id,
            text=f"Invalid row index: {row_idx_str}"
        )
        return

    viewer: TableViewer = context.bot_data.get("viewer")
    if not viewer:
        await do_update(context)
        viewer = context.bot_data["viewer"]

    df = viewer.get_table
    df.reset_index(drop=True, inplace=True)

    if row_idx < 0 or row_idx >= len(df):
        await context.bot.send_message(
            chat_id=alphawave_group_chat_id,
            text=f"Row index out of range: {row_idx}"
        )
        return

    row_df = df.iloc[[row_idx]].copy()
    detail_str = row_df.to_markdown(tablefmt="pipe", index=False)
    detail = escape_md(detail_str)
    msg_detail = f"```\n{detail}\n```"

    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text=msg_detail,
        parse_mode="Markdown"
    )


async def send_table(context: ContextTypes.DEFAULT_TYPE):
    viewer: TableViewer = context.bot_data.get("viewer")
    if not viewer:
        await do_update(context)
        viewer = context.bot_data["viewer"]

    df = viewer.get_table
    if df.empty:
        await context.bot.send_message(
            chat_id=alphawave_group_chat_id,
            text="=== Table is empty ==="
        )
        return

    df.reset_index(drop=False, inplace=True)
    df_msg = df.head(10).copy()
    cols = ["ticker", "exch1", "exch2", "ER"]
    df_msg = df_msg[cols]

    if "ER" in df_msg.columns:
        df_msg["ER"] = df_msg["ER"].round(4)

    table_str = df_msg.to_markdown(tablefmt="pipe", index=False)
    table = escape_md(table_str)
    msg = f"```\n{table}\n```"

    buttons = []
    for idx, row in df_msg.iterrows():
        ticker = str(row.get("ticker", ""))
        exch1 = str(row.get("exch1", ""))
        exch2 = str(row.get("exch2", ""))
        button = f"{ticker} / {exch1} / {exch2}"
        callback_data = f"DETAIL|{idx}"
        buttons.append([
            InlineKeyboardButton(
                text=button,
                callback_data=callback_data
            )
        ])
    reply_markup = InlineKeyboardMarkup(buttons)

    await context.bot.send_message(
        chat_id=alphawave_group_chat_id,
        text=msg,
        parse_mode="Markdown",
        reply_markup=reply_markup
    )


async def job_table(context: ContextTypes.DEFAULT_TYPE):
    await do_update(context)
    await send_table(context)
    next_run = datetime.datetime.now() + datetime.timedelta(minutes=1)
    logging.info(
        f"Updating: estimated: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    app = ApplicationBuilder().token(alphawave_bot).build()

    viewer = create_viewer()
    app.bot_data["viewer"] = viewer

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("update", cmd_update))
    app.add_handler(CommandHandler("table", cmd_table))
    app.add_handler(CallbackQueryHandler(detail_callback))

    job_queue = app.job_queue

    now = datetime.datetime.now()
    next_minute = now.replace(second=0, microsecond=0) + \
        datetime.timedelta(minutes=1)
    first_delay = (next_minute - now).total_seconds()

    job_queue.run_repeating(job_update, interval=60, first=first_delay)
    job_queue.run_repeating(job_table, interval=60, first=first_delay)

    app.run_polling()


if __name__ == "__main__":
    main()
