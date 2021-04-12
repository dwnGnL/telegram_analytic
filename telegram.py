from telethon.sync import TelegramClient,errors
import time
import pymysql
import re
from pymysql.cursors import DictCursor
import datetime
import asyncio
from telethon import functions, types
from telethon.tl.types import InputMessagesFilterUrl
from telethon.tl.functions.messages import SearchRequest


def updateStats(chan, mess_id):
    with connection.cursor() as cursor:
        now2 = datetime.datetime.utcnow()
        query = f'SELECT * FROM stats WHERE channel_id = {chan["id"]} AND date = "{now2.strftime("%Y-%m-%d")} 00:00:00"'
        print(query)
        cursor.execute(query)
        curs, first, prev = 1, 1, 1
        with connection.cursor() as static_cursor:
            query = f'SELECT count(*) as c FROM subscriptions WHERE channel_id = {chan["id"]} limit 1 '
            static_cursor.execute(query)
            for curs in static_cursor:
                curs = curs["c"]

            query = f'SELECT count(*) as c FROM subscriptions WHERE channel_id = {1} limit 1 '
            static_cursor.execute(query)
            for first in static_cursor:
                first = first["c"]

            query = f'SELECT count(*) as c FROM subscriptions WHERE channel_id = {int(chan["number"]) - 1} limit 1 '
            static_cursor.execute(query)
            for prev in static_cursor:
                prev = prev["c"]

            if prev == 0:
                prev = 1

            if first == 0:
                first = 1
        if cursor.rowcount == 0:

            sql = "INSERT INTO stats (`channel_id`,`channel_number`,`count`,`percent_prev_channel`,`percent_first_channel`,`date`,`last_chat_id`) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            with connection.cursor() as cursor2:

                cursor2.execute(sql,
                                [str(chan["id"]),
                                 str(chan["number"]),
                                 str(curs),
                                 str(int(curs * 100 / int(first))),
                                 str(int(curs * 100 / int(prev))),
                                 str(now2.strftime("%Y-%m-%d")),
                                 str(mess_id)])
                connection.commit()
        else:
            update_query = f'UPDATE stats SET `count` = {curs}, `date` = "{now2.strftime("%Y-%m-%d")}",`percent_prev_channel`={str(int(curs * 100 / int(first)))},`percent_first_channel`={str(int(curs * 100 / int(prev)))}, `last_chat_id`= {mess_id} WHERE `channel_id` = {chan["id"]}; '
            print(update_query)
            cursor.execute(update_query)
            connection.commit()


connection = pymysql.connect(
    host='localhost',
    user='root',
    password='root',
    db='telegram2',
    charset='utf8mb4',
    cursorclass=DictCursor
)
# with connection.cursor() as cursor:
# query = 'INSERT INTO `users`(`user_id`, `username`) VALUES (%s, %s)'
# cursor.execute(query,('1','ngnl'))
# # необходимо, т.к. по-умолчанию commit происходит только после выхода
# # из контекстного менеджера иначе мы бы не увидели твиттов
# connection.commit()


api_id = 3207826
api_hash = '58b73aea838cbce9839fe45c9d4a08e4'
phone_number = '+79090147889'
regex = r"^(https?:\/\/)?([\w-]{1,32}\.[\w-]{1,32})[^\s@]*$"
client = TelegramClient(phone_number, api_id, api_hash)

client.start()
continued = True


def getName(user):
    return client.get_entity(user).first_name


def SearchMessage(client, channel):
    for event in client.iter_messages(channel, search="", limit=10):
        if bool(event.message):
            match = re.search(regex, event.message)
            if match:
                return event



continued = True


# events = await client.get_admin_log(channel,join=True,leave=False,invite=True)
# print(events)
times = 60
last_chat_id = 0
def main():
    while continued:
        for dialog in client.iter_dialogs():
            try:
                print("step1")
                now = datetime.datetime.utcnow()
                for event in client.iter_admin_log(dialog.id, join=True, invite=True, leave=True,
                                                   min_id=int(last_chat_id)):
                    user_id = 0
                    order = 0
                    print("step2")
                    if event.joined_by_invite or event.joined:
                        print("step3")
                        with connection.cursor() as cursor:
                            query = f'SELECT * FROM users WHERE user_id = {event.user.id}'
                            cursor.execute(query)
                            if cursor.rowcount == 0:
                                print(event.user)
                                sql = "INSERT INTO users (`user_id`,`username`,`first_name`,`last_name`,`avatar`,`status`,`created_at`,`updated_at`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                                with connection.cursor() as cursor2:
                                    cursor2.execute(sql,
                                                    (str(event.user.id),
                                                     str(event.user.first_name),
                                                     str(now.strftime("%Y-%m-%d %H:%M:%S")),
                                                     str(now.strftime("%Y-%m-%d %H:%M:%S"))))
                                    connection.commit()
                                    user_id = cursor2.lastrowid
                            for row in cursor:
                                user_id = row['id']

                        print(event.user.first_name, " ", user_id)


                        print(event.id)
                        updateStats(priv, event.id)

                    if event.joined_invite:
                        user = event.action.participant.user_id
                        print(user)
                        # loop = asyncio.get_event_loop()
                        # loop.run_until_complete(getName(user))
                        username = getName(user)
                        print(username)
                        with connection.cursor() as cursor:
                            query = f'SELECT * FROM users WHERE user_id = {user}'
                            cursor.execute(query)
                            if cursor.rowcount == 0:
                                sql = "INSERT INTO users (`user_id`,`username`,`created_at`,`updated_at`) VALUES (%s,%s,%s,%s)"
                                with connection.cursor() as cursor2:
                                    cursor2.execute(sql,
                                                    (str(user),
                                                     str(username),
                                                     str(now.strftime("%Y-%m-%d %H:%M:%S")),
                                                     str(now.strftime("%Y-%m-%d %H:%M:%S"))))
                                    connection.commit()
                                    user_id = cursor2.lastrowid
                            for row in cursor:
                                user_id = row['id']
                                query_update = f'UPDATE `subscriptions` SET `user_id`={user_id},`channel_id`={priv["id"]},`status` = "subscribed" WHERE id={row2["id"]}'

                        print(username, " ", user_id)
                        with connection.cursor() as cursor:
                            query = f'SELECT * FROM subscriptions WHERE user_id = {user_id} ORDER BY `order` DESC'
                            cursor.execute(query)
                            if cursor.rowcount == 0:
                                if priv["number"] == 1:
                                    have_passed = False
                                    with connection.cursor() as cursor3:
                                        query = f'SELECT * FROM subscriptions WHERE status = "passed_over" ORDER BY `created_at` DESC limit 1'
                                        cursor3.execute(query)

                                        for row2 in cursor3:
                                            have_passed = True
                                            print("time1: ", datetime.datetime.today(), " time2: ", row2["created_at"])
                                            duration = datetime.datetime.today() - row2["created_at"]
                                            print("time: ", duration)
                                            if duration.total_seconds() > 60:
                                                query_delete = f'DELETE FROM subscriptions WHERE status = "passed_over"'
                                                cursor3.execute(query_delete)
                                                connection.commit()

                                            else:
                                                query_update = f'UPDATE `subscriptions` SET `user_id`={user_id},`channel_id`={priv["id"]},`status` = "subscribed" WHERE id={row2["id"]}'
                                                cursor3.execute(query_update)
                                            connection.commit()
                                    if not have_passed:
                                        sql = "INSERT INTO subscriptions (`user_id`,`channel_id`,`order`,`created_at`,`status`) VALUES (%s,%s,%s,%s,%s)"
                                        with connection.cursor() as cursor2:
                                            cursor2.execute(sql,
                                                            [str(user_id),
                                                             str(priv["id"]),
                                                             '1',
                                                             str(now.strftime("%Y-%m-%d %H:%M:%S")),
                                                             str("subscribed")])
                                            connection.commit()

                            else:
                                check = False
                                for row in cursor:
                                    if row['channel_id'] == priv["id"]:
                                        check = True

                                if not check:
                                    order = cursor.rowcount + 1
                                    match = None
                                    match2 = None
                                    name = ""
                                    name2 = ""
                                    for can in double:
                                        if can["number"] == 1:
                                            match = SearchMessage(client, can["name"])
                                        if can["number"] == 2:
                                            match2 = SearchMessage(client, can["name"])

                                    if not match or order == 1:
                                        name = ""
                                    elif order == 2:
                                        name = match.date
                                    if not match2 and order < 3:
                                        name2 = ""
                                    else:
                                        name2 = match2.date
                                    sql = "INSERT INTO subscriptions (`user_id`,`channel_id`,`order`,`created_at`,`utm_content`,`utm_term`,`status`) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                                    with connection.cursor() as cursor2:
                                        cursor2.execute(sql,
                                                        [str(user_id),
                                                         str(priv["id"]),
                                                         str(order),
                                                         str(now.strftime("%Y-%m-%d %H:%M:%S")),
                                                         str(name),
                                                         str(name2),
                                                         str("subscribed")])
                                        connection.commit()

                        print(event.id)
                        updateStats(priv, event.id)

                    if event.left:
                        print(event.action)
                        updateStats(priv, event.id)
                    print(event)
                    priv['last_chat_id'] = event.id
            except errors.ChatAdminRequiredError:
                print("permission denied")

        time.sleep(1)



if __name__ == "__main__":
    main()
