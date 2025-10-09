import requests
import pandas as pd
import json
import time
import os
import math

# Шаблоны URL API для разных типов
url_templates = {
    "animes": "https://shikimori.one/api/animes?order=popularity&page={}&limit=50",
    "manga": "https://shikimori.one/api/mangas?order=popularity&page={}&limit=50",
    "ranobe": "https://shikimori.one/api/ranobe?order=popularity&page={}&limit=50"
}

# Заголовки (обязательно нужен User-Agent)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
}

# Функция для замены NaN значений на None
def replace_nan(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    return value

# Запрос, что будем качать
choice = input("Введите что собирать (animes / manga / ranobe): ").strip().lower()
if choice not in url_templates:
    print("Неверный выбор. Доступные варианты: animes, manga, ranobe")
    exit()

url_template = url_templates[choice]
file_prefix = choice  # чтобы разные данные сохранялись в разные файлы

# Проверка существующего файла
csv_file = f"{file_prefix}_data.csv"
json_file = f"{file_prefix}_data.json"

if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
    existing_data = pd.read_csv(csv_file)
    all_data = existing_data.to_dict('records')
    last_page = (len(existing_data) // 50) + 1
    print(f"Продолжаем с страницы {last_page}")
else:
    all_data = []
    last_page = 1

# Запрос кол-ва страниц
i = int(input("Введите количество страниц (462 anime, 868 manga, 159 ranobe): "))

for page in range(last_page, i + 1):
    url = url_template.format(page)
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data_list = response.json()
            for item in data_list:
                all_data.append({
                    'id': replace_nan(item.get('id')),
                    'name': replace_nan(item.get('name')),
                    'russian': replace_nan(item.get('russian', '')),
                    'url': replace_nan(item.get('url')),
                    'kind': replace_nan(item.get('kind')),
                    'score': replace_nan(item.get('score')),
                    'status': replace_nan(item.get('status')),
                    'volumes': replace_nan(item.get('volumes', '')),  # у манги и ранобэ
                    'chapters': replace_nan(item.get('chapters', '')), # у манги и ранобэ
                    'episodes': replace_nan(item.get('episodes', '')), # у аниме
                    'episodes_aired': replace_nan(item.get('episodes_aired', '')),
                    'aired_on': replace_nan(item.get('aired_on')),
                    'released_on': replace_nan(item.get('released_on'))
                })
            print(f"Страница {page} успешно получена")
        else:
            print(f"Страница {page} - ошибка {response.status_code}")
    except Exception as e:
        print(f"Ошибка при обработке страницы {page}: {e}")

    # Сохраняем
    if len(all_data) > 0:
        df = pd.DataFrame(all_data)
        df.to_csv(csv_file, index=False)

        with open(json_file, 'w', encoding='utf-8') as jf:
            json.dump(all_data, jf, ensure_ascii=False, indent=4)

    time.sleep(1)

print(f"Сбор данных завершен. Данные сохранены в '{csv_file}' и '{json_file}'")