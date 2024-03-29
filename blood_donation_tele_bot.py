import os
import telebot
import threading
from dotenv import load_dotenv
from datetime import date, timedelta
from blood_donation_analysis import daily_trend_facility, daily_trend_facility_viz, \
    trend_days_data, trend_year_data, retrieve_retention_data

load_dotenv('source.env')
BOT_TOKEN = os.getenv('BLOOD_TELE_BOT_TOKEN')
bot = telebot.TeleBot(BOT_TOKEN)  # Initialise the bot

# Get latest updates date - The blood donation updates daily
today = date.today()
latest_update = today - timedelta(days=1)
previous_update = latest_update - timedelta(days=1)


# Handler for /start and /hello commands
@bot.message_handler(commands=['start', 'hello'])
def send_welcome(message):
    markup = telebot.types.ReplyKeyboardMarkup(row_width=2)
    itembtn1 = telebot.types.KeyboardButton('blood donation trend')
    itembtn2 = telebot.types.KeyboardButton('blood donation retention')
    markup.add(itembtn1, itembtn2)
    bot.send_message(message.chat.id,
                     "Hello, how are you doing? What would you like to know about blood donation in Malaysia?",
                     reply_markup=markup)


# Handler for 'blood donation trend' option
@bot.message_handler(func=lambda msg: msg.text == 'blood donation trend')
def ask_trend(message):
    markup = telebot.types.ReplyKeyboardMarkup(row_width=2)
    itembtn1 = telebot.types.KeyboardButton('donation in facility')
    itembtn2 = telebot.types.KeyboardButton('donation in states')
    markup.add(itembtn1, itembtn2)
    bot.send_message(message.chat.id, "Would you like to know the donation trend by donation facility or by states?",
                     reply_markup=markup)


# Handler for 'donation in states' option
@bot.message_handler(func=lambda msg: msg.text == 'donation in states')
def ask_trend_facility(message):
    # markup = telebot.types.ReplyKeyboardMarkup(row_width=2)
    # itembtn1 = telebot.types.KeyboardButton('facility latest trend')
    # itembtn2 = telebot.types.KeyboardButton('facility 7-day trends')
    # itembtn3 = telebot.types.KeyboardButton('facility 30 days trends')
    # itembtn4 = telebot.types.KeyboardButton('facility yearly trend')
    # markup.add(itembtn1, itembtn2, itembtn3, itembtn4)
    bot.send_message(message.chat.id, "Feature not available", parse_mode="Markdown")


# Handler for 'donation in facility' option
@bot.message_handler(func=lambda msg: msg.text == 'donation in facility')
def ask_trend_facility(message):
    markup = telebot.types.ReplyKeyboardMarkup(row_width=2)
    itembtn1 = telebot.types.KeyboardButton('facility latest trend')
    itembtn2 = telebot.types.KeyboardButton('facility 7-day trends')
    itembtn3 = telebot.types.KeyboardButton('facility 30 days trends')
    itembtn4 = telebot.types.KeyboardButton('facility yearly trend')
    markup.add(itembtn1, itembtn2, itembtn3, itembtn4)
    bot.send_message(message.chat.id, "What kind of trend would you like to see?", reply_markup=markup)


# Handler for 'facility latest trend' option
@bot.message_handler(func=lambda msg: msg.text == 'facility latest trend')
def latest_trend_analysis_facility(message):
    # Run facility latest trend function script
    bot.send_message(message.chat.id, "Processing data. Please wait for a moment")
    text, df_daily_trend, buf = daily_trend_facility(latest_update, previous_update)

    # Generate the plot using daily_trend_facility_viz
    #buf = daily_trend_facility_viz(df_daily_trend)

    # Send the image to the user
    bot.send_photo(message.chat.id, buf)

    # Also send the text message
    bot.send_message(message.chat.id, text)


# Handler for 'facility 7-day trends' option
@bot.message_handler(func=lambda msg: msg.text == 'facility 7-day trends')
def trend_7days_analysis_facility(message):
    # Run facility 7-day trends function script
    bot.send_message(message.chat.id, "Processing data. Please wait for a moment")
    latest_7_days = today - timedelta(days=7)

    # Generate the plot in a separate thread
    thread = threading.Thread(target=send_plot, args=(message, latest_7_days))
    thread.start()

def send_plot(message, time_frame):
    fig = trend_days_data(time_frame)
    # Send the image to the user
    bot.send_photo(message.chat.id, fig)


# Handler for 'facility 30 days trends' option
@bot.message_handler(func=lambda msg: msg.text == 'facility 30 days trends')
def trend_30days_analysis_facility(message):
    # Run facility 30-day trends function script
    bot.send_message(message.chat.id, "Processing data. Please wait for a moment")
    latest_30_days = today - timedelta(days=30)

    # Generate the plot
    fig = trend_days_data(latest_30_days)

    # Send the image to the user
    bot.send_photo(message.chat.id, fig)


# Handler for 'facility yearly trend' option
@bot.message_handler(func=lambda msg: msg.text == 'facility yearly trend')
def yearly_trend_analysis_facility(message):
    # Run facility yearly trends function script
    bot.send_message(message.chat.id, "Processing data. Please wait for a moment")

    # generate figure
    fig = trend_year_data()

    # Send the image to the user
    bot.send_photo(message.chat.id, fig)


# Handler for 'blood donation retention' option
@bot.message_handler(func=lambda msg: msg.text == 'blood donation retention')
def ask_retention(message):
    markup = telebot.types.ReplyKeyboardMarkup(row_width=2)
    itembtn1 = telebot.types.KeyboardButton('2023')
    itembtn2 = telebot.types.KeyboardButton('2022')
    itembtn3 = telebot.types.KeyboardButton('2021')
    itembtn4 = telebot.types.KeyboardButton('2020')
    markup.add(itembtn1, itembtn2, itembtn3, itembtn4)
    bot.send_message(message.chat.id, "Which year would you like to discover the blood donation retention?",
                     reply_markup=markup)


# Handler for '2023' option
@bot.message_handler(func=lambda msg: msg.text == '2023')
def donation_retention_facility(message):
    # Run donation retention by facility function script
    bot.send_message(message.chat.id, "Processing data. Please wait for a moment")
    msg = retrieve_retention_data(2023)
    bot.send_message(message.chat.id, msg)


# Handler for '2022' option
@bot.message_handler(func=lambda msg: msg.text == '2022')
def donation_retention_states(message):
    # Run donation retention by states function script
    bot.send_message(message.chat.id, "Processing data. Please wait for a moment")
    msg = retrieve_retention_data(2022)
    bot.send_message(message.chat.id, msg)


# Handler for '2021' option
@bot.message_handler(func=lambda msg: msg.text == '2021')
def donation_retention_malaysia(message):
    # Run donation retention in Malaysia function script
    bot.send_message(message.chat.id, "Processing data. Please wait for a moment")
    msg = retrieve_retention_data(2021)
    bot.send_message(message.chat.id, msg)


# Handler for '2020' option
@bot.message_handler(func=lambda msg: msg.text == '2020')
def donation_retention_malaysia(message):
    # Run donation retention in Malaysia function script
    bot.send_message(message.chat.id, "Processing data. Please wait for a moment")
    msg = retrieve_retention_data(2020)
    bot.send_message(message.chat.id, msg)


# Start the bot
bot.infinity_polling()
