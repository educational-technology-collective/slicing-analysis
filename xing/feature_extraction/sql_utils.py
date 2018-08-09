import os
import subprocess


def extract_coursera_sql_data(course, session):
    '''
    Initializes the MySQL database. This assumes that MySQL is correctly setup in the docker container.
    :return:
    '''
    password='root'
    user='root'
    mysql_binary_location='/usr/bin/mysql'
    mysql_admin_binary_location='/usr/bin/mysqladmin'
    hash_mapping_sql_dump = [x for x in os.listdir('/input/{}/{}'.format(course, session)) if 'hash_mapping' in x and session in x][0]
    forum_sql_dump = [x for x in os.listdir('/input/{}/{}'.format(course, session)) if 'anonymized_forum' in x and session in x][0]
    # start mysql server
    subprocess.call('service mysql start',shell=True)
    # command to create a database
    #command = [mysql_admin_binary_location, '-u', user, '-p', password, 'create course']
    res = subprocess.call('''mysql -u root -proot -e "CREATE DATABASE course"''', shell=True)
    print("RES1: {}".format(res))
    #command = [mysql_binary_location, '-u', user, '-p', password, 'course']
    #res = subprocess.call(command, shell=True)
    # command to load forum posts
    #command = [mysql_binary_location, '-u', user, '-p', password, 'course', '<', forum_sql_dump]
    command = '''mysql -u root -proot course < /input/{}/{}/{}'''.format(course, session, forum_sql_dump)
    res = subprocess.call(command,shell=True)
    print("RES2: {}".format(res))
    # command to load hash mapping
    #command = [mysql_binary_location, '-u', user, '-p', password, 'course', '<', hash_mapping_sql_dump]
    command = '''mysql -u root -proot course < /input/{}/{}/{}'''.format(course, session, hash_mapping_sql_dump)
    res = subprocess.call(command,shell=True)
    print("RES3: {}".format(res))
    # execute forum comment query and send to csv
    query = """SELECT * FROM (SELECT 'thread_id', 'post_time', 'session_user_id' UNION ALL (SELECT thread_id , post_time , b.session_user_id FROM forum_comments as a LEFT JOIN hash_mapping as b ON a.user_id = b.user_id WHERE a.is_spam != 1 ORDER BY post_time)) results INTO OUTFILE '/input/{}/{}/forum_comments.csv' FIELDS TERMINATED BY ',' ;""".format(course, session)
    #command = [mysql_binary_location, '-u', user, '-p', password, 'course', '<', query]
    command = '''mysql -u root -proot course -e"{}"'''.format(query)
    res = subprocess.call(command,shell=True)
    print("RES4: {}".format(res))
    # execute forum post query and send to csv
    query = """SELECT * FROM (SELECT 'id', 'thread_id', 'post_time', 'user_id', 'public_user_id', 'session_user_id', 'eventing_user_id' UNION ALL (SELECT id , thread_id , post_time , a.user_id , public_user_id , session_user_id , eventing_user_id FROM forum_posts as a LEFT JOIN hash_mapping as b ON a.user_id = b.user_id WHERE is_spam != 1 ORDER BY post_time)) results INTO OUTFILE '/input/{}/{}/forum_posts.csv' FIELDS TERMINATED BY ',' """.format(course, session)
    #command = [mysql_binary_location, '-u', user, '-p', password, 'course', '<', query]
    command = '''mysql -u root -proot course -e"{}"'''.format(query)
    res = subprocess.call(command,shell=True)
    print("RES5: {}".format(res))
    return None
