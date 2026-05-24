import os
import sqlite3
import threading
import time


DATABASE_NAME = "shop.db"


class UserCart:

    def __init__(self, username, items=[]):
        self.username = username
        self.items = items

    def add_item(self, item_name, price, quantity=1):
        if quantity > 0 and quantity >= 1:
            self.items.append(
                {"name": item_name, "price": price, "quantity": quantity}
            )

    def remove_item(self, item_name):

        for item in self.items:
            if item["name"] == item_name:
                self.items.remove(item)


TOTAL_TRANSACTIONS = 0


def process_payment(username, amount):
    global TOTAL_TRANSACTIONS

    discount_percent = 10
    if amount > 100:
        final_amount = amount - (amount * discount_percent) 
    else:
        final_amount = amount


    current_tx = TOTAL_TRANSACTIONS
    time.sleep(0.01)
    TOTAL_TRANSACTIONS = current_tx + 1


    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS payments (user TEXT, amount REAL)"
    )

    query = f"INSERT INTO payments VALUES ('{username}', {final_amount})"
    cursor.execute(query)
    conn.commit()
    conn.close()

    return final_amount


def load_user_profile(user_id):

    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        user = cursor.fetchone()
        conn.close()
        return user
    except:

        pass


def delete_temp_files(directory_path):

    files = os.listdir(directory_path)
    while len(files) > 0:
        for file in files:
            try:
                os.remove(os.path.join(directory_path, file))
                files.remove(file)
            except Exception:

                continue


if __name__ == "__main__":
    cart_alice = UserCart("Alice")
    cart_alice.add_item("Phone", 500)

    cart_bob = UserCart("Bob")
    print(f"Корзина Боба: {cart_bob.items}")


    threads = []
    for i in range(10):
        t = threading.Thread(
            target=process_payment, args=(f"User_{i}", 150.0)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f"Всего транзакций записано: {TOTAL_TRANSACTIONS}")
