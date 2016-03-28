# -*- coding: utf8 -*-
import pynder, requests, time, token_generator, re, MySQLdb, datetime, keys
from keys import db

class Grinded():
    def __init__(self):
        token = token_generator.Token()
        self.FBTOKEN = token.generate_token()
        self.FBID = keys.fb_id
        self.cur = db.cursor()

        # Enforce UTF-8 for the connection.
        self.cur.execute('SET NAMES utf8mb4')
        self.cur.execute("SET CHARACTER SET utf8mb4")
        self.cur.execute("SET character_set_connection=utf8mb4")

        self.session = pynder.Session(self.FBID, self.FBTOKEN)

    def refresh(self):
        self.cur = db.cursor()

    def update_matches(self):
        users = self.session.matches()
        for user in users:
            # Check if user already in DB
            query = "SELECT * FROM matches WHERE `key` = '%s'" % (user.user.id[:24])
            self.cur.execute(query)
            if len(self.cur.fetchall()) == 0:
                user = user.user
                # Gather the users pynder values
                columns = ['key', 'name', 'bio', 'photo', 'thumbnail', 'age', 'birth_date', 'ping_time', 'distance_km', 'photos', 'instagram_username', 'schools', 'jobs']
                values = [user.id[:24], user.name, user.bio, user.photos, user.thumbnails, user.age, user.birth_date, user.ping_time, user.distance_km, user.get_photos("84"), user.instagram_username, user.schools, user.jobs]

                tmp_col = []
                tmp_val = []

                # Parse pynder values to SQL friendly strings
                for i in range(0, len(columns)):
                    tmp_col.append("`" + columns[i] + "`")
                    value = values[i]
                    if type(value) is list:
                        tmp_val.append("'" + ",".join(value) + "'")
                    elif value is None:
                        tmp_val.append("'" + str(value) + "'")
                    elif type(value) is int:
                        tmp_val.append("'" + str(value) + "'")
                    elif type(value) is float:
                        tmp_val.append("'" + str(value) + "'")
                    elif type(value) is datetime.datetime:
                        tmp_val.append("'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'")
                    else:
                        tmp_val.append("'" + MySQLdb.escape_string(value.encode('utf-8')).decode('utf-8') + "'")

                # Insert parsed values into DB
                query = "INSERT INTO `grinded`.`matches` (%s) VALUES (%s);" % (','.join(tmp_col), ','.join(tmp_val))
                self.cur.execute(query.encode('utf-8'))
                # Don't forget to commit
                db.commit()
                print '-------------------------'
                print '[+] ' + str(user) + ':' + user.id[:24] + ' added to DB.'

    def get_ungrinded_matches(self):
        query = "SELECT `name`, `key` FROM `matches` WHERE `match` IS NULL"
        self.cur.execute(query.encode('utf-8'))
        return self.cur.fetchall()

    def grind_matches(self):
        ungrinded_matches = self.get_ungrinded_matches()
        if len(ungrinded_matches) > 1:
            print '[!] ' + str(len(ungrinded_matches)) + " ungrinded matches found"
            query = "UPDATE `grinded`.`matches` SET `match` = '%s' WHERE `matches`.`key` = '%s';" % (ungrinded_matches[0][1], ungrinded_matches[1][1])
            self.cur.execute(query)
            query = "UPDATE `grinded`.`matches` SET `match` = '%s' WHERE `matches`.`key` = '%s';" % (ungrinded_matches[1][1], ungrinded_matches[0][1])
            self.cur.execute(query)
            db.commit()
            print '[+] ' + ungrinded_matches[0][0] + ' and ' + ungrinded_matches[1][0] + ' are now grinding!'
            self.grind_matches()
        elif len(ungrinded_matches) == 1:
            print '[!] One ungrinded match found. Swipe another victim to start grinding them.'
        else:
            print '[+] No new matches'

    def grind_message(self, message, name, key):
        query = u"SELECT `match` FROM `grinded`.`matches` WHERE `key` = '%s'" % (key)
        self.cur.execute(query)
        lover = self.cur.fetchall()
        users = self.session.matches()
        for user in users:
            if user.user.id[:24] == lover[0][0]:
                while True:
                    try:
                        user.message(message)
                        print '[+] Message successfully forwarded'
                        break
                    except:
                        print '[-] Failed to forward message ' + unicode(message) + '! Retrying in 60seconds....'
                        time.sleep(60)

    def parse_messages(self):
        users = self.session.matches()
        latest = True
        for user in users:
            first = True
            ungrinded = []
            ungrinded_matches = self.get_ungrinded_matches()
            for i in range(0, len(ungrinded_matches)):
                ungrinded.append(ungrinded_matches[i][1])

            if user.user.id[:24] in ungrinded:
                print '[-] %s has no match yet, message ignored.' % (user.user.name)
            else:
                messages = user.messages
                name = user.user.name
                key = user.user.id[:24]

                query = "SELECT `datetime` FROM `messages` WHERE `senderkey` = '%s' OR `receiverkey` = '%s';" % (key, key)
                self.cur.execute(query)
                fetched_messages = self.cur.fetchall()
                queue = []

                for message in reversed(messages):
                    # Encode and sanitize tmp message for print and SQL purposes
                    encoded_msg = MySQLdb.escape_string(message.body.encode('utf-8')).decode('utf-8')#.body.encode('utf-8').decode('latin-1')

                    # TODO : Deprecate hardcoded name filters and use external source instead, json or sql or something
                    # The boolean indicates if partial replace of a word is allowed
                    replaces = [["Jennifer", "you", False], ["Jenny", "you", False], ["\\", "", True], ["duck", "", True], ["girl", "boy", True], ["woman", "man", False], ["man", "woman", False], \
                                ["guy", "girl", False], ["girl", "boy", True], ["boy", "girl", True]]
                    aborts = ["meetup", "meet", "date", "where are you", "hang out"]
                    error = False

                    # create tmp splitted list
                    tmp_msg = encoded_msg.split(" ")

                    # Replace banned words with substitutes
                    parsed_list = []
                    for word in tmp_msg:
                        replaced = False
                        for repl in replaces:
                            banned_word = repl[0]
                            substitute = repl[1]
                            partial_replace = repl[2]
                            if partial_replace:
                                if banned_word in word:
                                    regexp = re.compile(re.escape(banned_word), re.IGNORECASE)
                                    parsed_list.append(regexp.sub(substitute, word))
                                    replaced = True
                                    break
                            else:
                                if banned_word == word:
                                    regexp = re.compile(re.escape(banned_word), re.IGNORECASE)
                                    parsed_list.append(regexp.sub(substitute, word))
                                    replaced = True
                                    break

                        if not replaced:
                            parsed_list.append(word)



                    # Restring the tmp list, then add it to the message object
                    encoded_msg = ' '.join(parsed_list)
                    message.body = encoded_msg

                    # Unmatch if these banned words are detected
                    for abort in aborts:
                        regexp = re.compile(re.escape(repl[0]), re.IGNORECASE)
                        if not re.match(regexp, message.body) == None:
                            print "[!!] ABORTING CONVO: DISALLOWED WORD FROM USER: " + user + ", MESSAGE: " + message.body
                            user.delete()
                            error = True

                    # Filter for american mobile numbers
                    filter_nr = ".*?(\(?\d{{0,9}).*?"
                    regexp = re.compile(filter_nr)
                    if not re.match(regexp, message.body) == None:
                        #encoded_msg = regexp.sub("", encoded_msg)
                        #message.body = regexp.sub("", message.body)
                        print "[!!] ABORTING CONVO: DISALLOWED PHONENUMBER FROM USER " + user + ", MESSAGE: " + message.body
                        exit(0)
                        user.delete()
                        error = True

                    if error:
                        break

                    datetime = message.sent

                    # iterate all parsed messages, parse them to strings
                    parsed_messages = []
                    for msg in fetched_messages:
                        parsed_messages.append(str(msg[0]))

                    # if message already in parsed messages, we stop parsing for this user
                    # this combined with the reversed message parsing lets us check for new messages only
                    if str(datetime)[:19] in parsed_messages:
                        break
                    else:
                        # new message detected
                        first = False
                        latest = False
                        columns = ['senderkey', 'receiverkey', 'datetime', 'message']
                        values = ["'" + message.sender.id[:24] + "'", "'" + message.to.id[:24] + "'", "'" + datetime.strftime('%Y-%m-%d %H:%M:%S') + "'", "'" + encoded_msg + "'"]

                        query = "INSERT INTO `grinded`.`messages` (%s) VALUES (%s);" % (','.join(columns), ','.join(values))
                        self.cur.execute(query.encode('utf-8'))
                        print '[+] Parsing - %s>You: %s' % (name, encoded_msg)
                        if message.sender.id[:24] == key:
                            queue.append([message, name, key])
                        else:
                            db.commit()

                if error:
                    pass

                if len(queue) > 0:
                    for msg in reversed(queue):
                        print '[+] Sending - You>%s\'s grind: %s' % (msg[1], msg[0].body)
                        self.grind_message(msg[0], msg[1], msg[2])
                    db.commit()

        if latest:
            print '[+] No new messages'

while True:
    try:
        grinded = Grinded()
        while True:
            grinded.update_matches()
            grinded.grind_matches()
            grinded.parse_messages()
            print '-------------------'
            print '[+] Sleeping for 60s, Zzzzz'
            time.sleep(60)
            grinded.refresh()
    except Exception as e:
        print '[!] Error, %s, sleeping 120s' % (e)
        time.sleep(120)
