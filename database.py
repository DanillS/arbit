import json
import os
from typing import List, Set
import logging

class UserDatabase:
    def __init__(self, filename="users.json"):
        self.filename = filename
        self.users = self.load_users()
        logging.info(f"Загружено {len(self.users)} пользователей")

    def load_users(self) -> Set[int]:
        """Загружает пользователей из JSON файла"""
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    data = json.load(f)
                    return set(data.get('users', []))
            except Exception as e:
                logging.error(f"Ошибка загрузки пользователей: {e}")
                return set()
        return set()

    def save_users(self):
        """Сохраняет пользователей в JSON файл"""
        try:
            with open(self.filename, 'w') as f:
                json.dump({'users': list(self.users)}, f)
        except Exception as e:
            logging.error(f"Ошибка сохранения пользователей: {e}")

    def add_user(self, user_id: int, username: str = None):
        """Добавляет нового пользователя"""
        if user_id not in self.users:
            self.users.add(user_id)
            self.save_users()
            logging.info(f"Новый пользователь: {user_id} ({username})")

    def remove_user(self, user_id: int):
        """Удаляет пользователя"""
        if user_id in self.users:
            self.users.remove(user_id)
            self.save_users()
            logging.info(f"Пользователь отписался: {user_id}")

    def get_all_users(self) -> List[int]:
        """Возвращает список всех пользователей"""
        return list(self.users)

    def user_exists(self, user_id: int) -> bool:
        """Проверяет, существует ли пользователь"""
        return user_id in self.users