
import os
import random
import mimetypes
from settings import logger
from scrapping.debank_scrapper import parse_scrapped_info
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor

# Initialize bot and dispatcher
load_dotenv(".env")
bot = Bot(token=os.getenv('TELEGRAM_BOT_TOKEN'))
dp = Dispatcher(bot)

# Dictionary to store current position for each user
user_positions = {}
protocols = parse_scrapped_info('scrapped_info.json')

# Main menu keyboard
main_menu = types.ReplyKeyboardMarkup(resize_keyboard=True)
main_menu.add(types.KeyboardButton('Посмотреть рекомендации'))
main_menu.add(types.KeyboardButton('Информация о проекте'))

@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.reply("Добро пожаловать в бот рекомендаций протоколов!", reply_markup=main_menu)

@dp.message_handler(lambda message: message.text == 'Посмотреть рекомендации')
async def show_recommendations(message: types.Message):
    global protocols
    random.shuffle(protocols)
    user_id = message.from_user.id
    user_positions[user_id] = 0  # Start with first protocol
    await display_protocol_card(message.chat.id, user_id)

@dp.message_handler(lambda message: message.text == 'Информация о проекте')
async def show_project_info(message: types.Message):
    info_text = (
        "🤖 <b>Бот рекомендаций протоколов</b>\n\n"
        "Этот бот предоставляет информацию о топовых DeFi протоколах "
        "на основе анализа взаимодействий инвесторов с контрактами.\n\n"
        "Каждая рекомендация содержит:\n"
        "- Название протокола\n"
        "- HEX адрес контракта\n"
        "- Краткое описание\n"
        "- Ссылку на официальный сайт\n\n"
        "Используйте кнопки навигации для просмотра рекомендаций."
    )
    await message.answer(info_text, parse_mode='HTML')

async def display_protocol_card(chat_id, user_id):
    position = user_positions.get(user_id, 0)
    protocol = protocols[position]
    
    # Create navigation buttons
    keyboard = InlineKeyboardMarkup()
    
    # Add left arrow if not first item
    if position > 0:
        keyboard.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"prev_{position}"))
    
    # Add right arrow if not last item
    if position < len(protocols) - 1:
        if position > 0:
            # If both buttons exist, place them in same row
            keyboard.inline_keyboard[0].append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"next_{position}"))
        else:
            keyboard.add(InlineKeyboardButton("Вперёд ➡️", callback_data=f"next_{position}"))
    
    # Prepare message with protocol info
    caption = (
        f"<b>{protocol['name']}</b>\n\n"
        f"<b>HEX адрес:</b> <code>{protocol['hex_address']}</code>\n"
        f"<b>Описание:</b> {protocol['description']}\n"
        f"<b>Ссылка:</b> {protocol['url']}"
    )
     # Check if the image is not SVG
    image_url = protocol['image_url']
    mime_type, _ = mimetypes.guess_type(image_url)
    if mime_type == 'image/svg+xml':
        logger.warning(f"SVG format detected for {image_url}. Skipping image.")
        image_url = None  # Optionally, replace with a placeholder image URL
    
    # Send photo with caption and navigation buttons
    logger.info(protocol['image_url'])
    await bot.send_photo(
        chat_id=chat_id,
        photo=image_url if image_url else 'https://cryptologos.cc/logos/ethereum-eth-logo.png',
        caption=caption,
        parse_mode='HTML',
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith(('prev_', 'next_')))
async def process_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    action, current_pos = callback_query.data.split('_')
    current_pos = int(current_pos)
    
    # Update position based on action
    if action == 'prev':
        new_pos = current_pos - 1
    else:
        new_pos = current_pos + 1
    
    user_positions[user_id] = new_pos
    
    # Delete previous message
    await bot.delete_message(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id
    )
    
    # Display new protocol card
    await display_protocol_card(callback_query.message.chat.id, user_id)
    
    # Answer callback query to remove loading indicator
    await bot.answer_callback_query(callback_query.id)

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
