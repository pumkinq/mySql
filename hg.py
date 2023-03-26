import os
import sys
import string
import time
import datetime
import MySQLdb
import logging
import logging.config

logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("Python")
path = './include'
sys.path.insert(0, path)
import functions as func
import sendmail
import sendsms_fx
import sendsms_api

send_mail_max_count = func.get_option('send_mail_max_count')
send_mail_sleep_time = func.get_option('send_mail_sleep_time')
mail_to_list_common = func.get_option('send_mail_to_list')
send_sms_max_count = func.get_option('send_sms_max_count')
send_mail_sleep_time = func.get_option('send_mail_sleep_time')
send_sms_sleep_time = func.get_option('send_sms_sleep_time')
sms_to_list_common = func.get_option('send_sms_to_list')


def get_alarm_mysql_status():
    sql = "select a.server_id,a.connect,a.threads_connected,a.threads_running,a.threads_waits,a.create_time,a.host,a.port,b.alarm_threads_connected,b.alarm_threads_running,alarm_threads_waits,b.threshold_warning_threads_connected,b.threshold_critical_threads_connected,b.threshold_warning_threads_running,b.threshold_critical_threads_running,threshold_warning_threads_waits,threshold_critical_threads_waits,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list,b.tags,'mysql' as db_type from mysql_status a, db_servers_mysql b where a.server_id=b.id;"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            server_id = line[0]
            connect = line[1]
            threads_connected = line[2]
            threads_running = line[3]
            threads_waits = line[4]
            create_time = line[5]
            host = line[6]
            port = line[7]
            alarm_threads_connected = line[8]
            alarm_threads_running = line[9]
            alarm_threads_waits = line[10]
            threshold_warning_threads_connected = line[11]
            threshold_critical_threads_connected = line[12]
            threshold_warning_threads_running = line[13]
            threshold_critical_threads_running = line[14]
            threshold_warning_threads_waits = line[15]
            threshold_critical_threads_waits = line[16]
            send_mail = line[17]
            send_mail_to_list = line[18]
            send_sms = line[19]
            send_sms_to_list = line[20]
            tags = line[21]
            db_type = line[22]
            if send_mail_to_list is None or send_mail_to_list.strip() == '':
                send_mail_to_list = mail_to_list_common
            if send_sms_to_list is None or send_sms_to_list.strip() == '':
                send_sms_to_list = sms_to_list_common
            if connect <> 1:
                send_mail = func.update_send_mail_status(server_id, db_type, 'connect', send_mail, send_mail_max_count)
                send_sms = func.update_send_sms_status(server_id, db_type, 'connect', send_sms, send_sms_max_count)
                func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connect', 'down', 'critical',
                               'mysql server down', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '3', host, port, create_time, 'connect', 'down', 'critical')
                func.update_db_status('sessions', '-1', host, port, '', '', '', '')
                func.update_db_status('actives', '-1', host, port, '', '', '', '')
                func.update_db_status('waits', '-1', host, port, '', '', '', '')
                func.update_db_status('repl', '-1', host, port, '', '', '', '')
                func.update_db_status('repl_delay', '-1', host, port, '', '', '', '')
            else:
                func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'connect', 'up', 'mysql server up',
                                 send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '1', host, port, create_time, 'connect', 'up', 'ok')
                if int(alarm_threads_connected) == 1:
                    if int(threads_connected) >= int(threshold_critical_threads_connected):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'threads_connected', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'threads_connected', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'threads_connected',
                                       threads_connected, 'critical', 'too many threads connected', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 3, host, port, create_time, 'threads_connected',
                                              threads_connected, 'critical')
                    elif int(threads_connected) >= int(threshold_warning_threads_connected):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'threads_connected', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'threads_connected', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'threads_connected',
                                       threads_connected, 'warning', 'too many threads connected', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 2, host, port, create_time, 'threads_connected',
                                              threads_connected, 'warning')
                    else:
                        func.update_db_status('sessions', 1, host, port, create_time, 'threads_connected',
                                              threads_connected, 'ok')
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'threads_connected',
                                         threads_connected, 'threads connected ok', send_mail, send_mail_to_list,
                                         send_sms, send_sms_to_list)
                if int(alarm_threads_running) == 1:
                    if int(threads_running) >= int(threshold_critical_threads_running):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'threads_running', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'threads_running', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'threads_running',
                                       threads_running, 'critical', 'too many threads running', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 3, host, port, create_time, 'threads_running', threads_running,
                                              'critical')
                    elif int(threads_running) >= int(threshold_warning_threads_running):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'threads_running', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'threads_running', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'threads_running',
                                       threads_running, 'warning', 'too many threads running', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 2, host, port, create_time, 'threads_running', threads_running,
                                              'warning')
                    else:
                        func.update_db_status('actives', 1, host, port, create_time, 'threads_running', threads_running,
                                              'ok')
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'threads_running',
                                         threads_running, 'threads running ok', send_mail, send_mail_to_list, send_sms,
                                         send_sms_to_list)

                if int(alarm_threads_waits) == 1:
                    if int(threads_waits) >= int(threshold_critical_threads_waits):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'threads_waits', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'threads_waits', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'threads_waits',
                                       threads_waits, 'critical', 'too many threads waits', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('waits', 3, host, port, create_time, 'threads_waits', threads_waits,
                                              'critical')
                    elif int(threads_waits) >= int(threshold_warning_threads_running):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'threads_waits', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'threads_waits', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'threads_waits',
                                       threads_waits, 'warning', 'too many threads waits', send_mail, send_mail_to_list,
                                       send_sms, send_sms_to_list)
                        func.update_db_status('waits', 2, host, port, create_time, 'threads_waits', threads_waits,
                                              'warning')
                    else:
                        func.update_db_status('waits', 1, host, port, create_time, 'threads_waits', threads_waits, 'ok')
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'threads_waits',
                                         threads_waits, 'threads waits ok', send_mail, send_mail_to_list, send_sms,
                                         send_sms_to_list)
    else:
        pass


def get_alarm_mysql_replcation():
    sql = "select a.server_id,a.slave_io_run,a.slave_sql_run,a.delay,a.create_time,b.host,b.port,b.alarm_repl_status,b.alarm_repl_delay,b.threshold_warning_repl_delay,b.threshold_critical_repl_delay,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list,b.tags,'mysql' as db_type from mysql_replication a, db_servers_mysql b  where a.server_id=b.id and a.is_slave='1';"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            server_id = line[0]
            slave_io_run = line[1]
            slave_sql_run = line[2]
            delay = line[3]
            create_time = line[4]
            host = line[5]
            port = line[6]
            alarm_repl_status = line[7]
            alarm_repl_delay = line[8]
            threshold_warning_repl_delay = line[9]
            threshold_critical_repl_delay = line[10]
            send_mail = line[11]
            send_mail_to_list = line[12]
            send_sms = line[13]
            send_sms_to_list = line[14]
            tags = line[15]
            db_type = line[16]

            if send_mail_to_list is None or send_mail_to_list.strip() == '':
                send_mail_to_list = mail_to_list_common
            if send_sms_to_list is None or send_sms_to_list.strip() == '':
                send_sms_to_list = sms_to_list_common

            if int(alarm_repl_status) == 1:
                if (slave_io_run == "Yes") and (slave_sql_run == "Yes"):
                    func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'replication',
                                     'IO:' + slave_io_run + ',SQL:' + slave_sql_run, 'replication ok', send_mail,
                                     send_mail_to_list, send_sms, send_sms_to_list)
                    func.update_db_status('repl', 1, host, port, create_time, 'replication',
                                          'IO:' + slave_io_run + ',SQL:' + slave_sql_run, 'ok')
                    if int(alarm_repl_delay) == 1:
                        if int(delay) >= int(threshold_critical_repl_delay):
                            send_mail = func.update_send_mail_status(server_id, db_type, 'repl_delay', send_mail,
                                                                     send_mail_max_count)
                            send_sms = func.update_send_sms_status(server_id, db_type, 'repl_delay', send_sms,
                                                                   send_sms_max_count)
                            func.add_alarm(server_id, tags, host, port, create_time, db_type, 'repl_delay', delay,
                                           'critical', 'replication has delay', send_mail, send_mail_to_list, send_sms,
                                           send_sms_to_list)
                            func.update_db_status('repl_delay', 3, host, port, create_time, 'repl_delay', delay,
                                                  'critical')
                        elif int(delay) >= int(threshold_warning_repl_delay):
                            send_mail = func.update_send_mail_status(server_id, db_type, 'repl_delay', send_mail,
                                                                     send_mail_max_count)
                            send_sms = func.update_send_sms_status(server_id, db_type, 'repl_delay', send_sms,
                                                                   send_sms_max_count)
                            func.add_alarm(server_id, tags, host, port, create_time, db_type, 'repl_delay', delay,
                                           'warning', 'replication has delay', send_mail, send_mail_to_list, send_sms,
                                           send_sms_to_list)
                            func.update_db_status('repl_delay', 2, host, port, create_time, 'repl_delay', delay,
                                                  'warning')
                        else:
                            func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'repl_delay', delay,
                                             'replication delay ok', send_mail, send_mail_to_list, send_sms,
                                             send_sms_to_list)
                            func.update_db_status('repl_delay', 1, host, port, create_time, 'repl_delay', delay, 'ok')
                else:
                    send_mail = func.update_send_mail_status(server_id, db_type, 'replication', send_mail,
                                                             send_mail_max_count)
                    send_sms = func.update_send_sms_status(server_id, db_type, 'replication', send_sms,
                                                           send_sms_max_count)
                    func.add_alarm(server_id, tags, host, port, create_time, db_type, 'replication',
                                   'IO:' + slave_io_run + ',SQL:' + slave_sql_run, 'critical', 'replication stop',
                                   send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                    func.update_db_status('repl', 3, host, port, create_time, 'replication',
                                          'IO:' + slave_io_run + ',SQL:' + slave_sql_run, 'critical')
                    func.update_db_status('repl_delay', '-1', host, port, '', '', '', '')
    else:
        pass


def get_alarm_oracle_status():
    sql = "select a.server_id,a.connect,a.session_total,a.session_actives,a.session_waits,a.create_time,b.host,b.port,b.alarm_session_total,b.alarm_session_actives,b.alarm_session_waits,b.threshold_warning_session_total,b.threshold_critical_session_total,b.threshold_warning_session_actives,b.threshold_critical_session_actives,b.threshold_warning_session_waits,b.threshold_critical_session_waits,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list,b.tags,'oracle' as db_type from oracle_status a, db_servers_oracle b where a.server_id=b.id;"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            server_id = line[0]
            connect = line[1]
            session_total = line[2]
            session_actives = line[3]
            session_waits = line[4]
            create_time = line[5]
            host = line[6]
            port = line[7]
            alarm_session_total = line[8]
            alarm_session_actives = line[9]
            alarm_session_waits = line[10]
            threshold_warning_session_total = line[11]
            threshold_critical_session_total = line[12]
            threshold_warning_session_actives = line[13]
            threshold_critical_session_actives = line[14]
            threshold_warning_session_waits = line[15]
            threshold_critical_session_waits = line[16]
            send_mail = line[17]
            send_mail_to_list = line[18]
            send_sms = line[19]
            send_sms_to_list = line[20]
            tags = line[21]
            db_type = line[22]

            if send_mail_to_list is None or send_mail_to_list.strip() == '':
                send_mail_to_list = mail_to_list_common
            if send_sms_to_list is None or send_sms_to_list.strip() == '':
                send_sms_to_list = sms_to_list_common

            if connect <> 1:
                send_mail = func.update_send_mail_status(server_id, db_type, 'connect', send_mail, send_mail_max_count)
                send_sms = func.update_send_sms_status(server_id, db_type, 'connect', send_sms, send_sms_max_count)
                func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connect', 'down', 'critical',
                               'oracle server down', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '3', host, port, create_time, 'connect', 'down', 'critical')
                func.update_db_status('sessions', '-1', host, port, '', '', '', '')
                func.update_db_status('actives', '-1', host, port, '', '', '', '')
                func.update_db_status('waits', '-1', host, port, '', '', '', '')
                func.update_db_status('repl', '-1', host, port, '', '', '', '')
                func.update_db_status('repl_delay', '-1', host, port, '', '', '', '')
            else:
                func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'connect', 'up', 'oracle server up',
                                 send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '1', host, port, create_time, 'connect', 'up', 'ok')
                if int(alarm_session_total) == 1:
                    if int(session_total) >= int(threshold_critical_session_total):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'session_total', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'session_total', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'session_total',
                                       session_total, 'critical', 'too many sessions', send_mail, send_mail_to_list,
                                       send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 3, host, port, create_time, 'session_total', session_total,
                                              'critical')
                    elif int(session_total) >= int(threshold_warning_session_total):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'session_total', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'session_total', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'session_total',
                                       session_total, 'warning', 'too many sessions', send_mail, send_mail_to_list,
                                       send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 2, host, port, create_time, 'session_total', session_total,
                                              'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'session_total',
                                         session_total, 'sessions ok', send_mail, send_mail_to_list, send_sms,
                                         send_sms_to_list)
                        func.update_db_status('sessions', 1, host, port, create_time, 'session_total', session_total,
                                              'ok')

                if int(alarm_session_actives) == 1:
                    if int(session_actives) >= int(threshold_critical_session_actives):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'session_actives', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'session_actives', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'session_actives',
                                       session_actives, 'critical', 'too many active sessions', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 3, host, port, create_time, 'session_actives', session_actives,
                                              'critical')
                    elif int(session_actives) >= int(threshold_warning_session_actives):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'session_actives', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'session_actives', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'session_actives',
                                       session_actives, 'warning', 'too many active sessions', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 2, host, port, create_time, 'session_actives', session_actives,
                                              'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'session_actives',
                                         session_actives, 'active sessions ok', send_mail, send_mail_to_list, send_sms,
                                         send_sms_to_list)
                        func.update_db_status('actives', 1, host, port, create_time, 'session_actives', session_actives,
                                              'ok')
                if int(alarm_session_waits) == 1:
                    if int(session_waits) >= int(threshold_critical_session_waits):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'session_waits', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'session_waits', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'session_waits',
                                       session_waits, 'critical', 'too many waits sessions', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('waits', 3, host, port, create_time, 'session_waits', session_waits,
                                              'critical')
                    elif int(session_waits) >= int(threshold_warning_session_waits):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'session_waits', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'session_waits', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'session_waits',
                                       session_waits, 'warning', 'too many waits sessions', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('waits', 2, host, port, create_time, 'session_waits', session_waits,
                                              'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'session_waits',
                                         session_waits, 'waits sessions ok', send_mail, send_mail_to_list, send_sms,
                                         send_sms_to_list)
                        func.update_db_status('waits', 1, host, port, create_time, 'session_waits', session_waits, 'ok')

    else:
        pass


def get_alarm_sqlserver_status():
    sql = "select a.server_id,a.connect,a.processes,a.processes_running,a.processes_waits,a.create_time,a.host,a.port,b.alarm_processes,b.alarm_processes_running,alarm_processes_waits,b.threshold_warning_processes,b.threshold_warning_processes_running,b.threshold_warning_processes_waits,b.threshold_critical_processes,threshold_critical_processes_running,threshold_critical_processes_waits,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list,b.tags,'sqlserver' as db_type from sqlserver_status a, db_servers_sqlserver b where a.server_id=b.id;"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            server_id = line[0]
            connect = line[1]
            processes = line[2]
            processes_running = line[3]
            processes_waits = line[4]
            create_time = line[5]
            host = line[6]
            port = line[7]
            alarm_processes = line[8]
            alarm_processes_running = line[9]
            alarm_processes_waits = line[10]
            threshold_warning_processes = line[11]
            threshold_warning_processes_running = line[12]
            threshold_warning_processes_waits = line[13]
            threshold_critical_processes = line[14]
            threshold_critical_processes_running = line[15]
            threshold_critical_processes_waits = line[16]
            send_mail = line[17]
            send_mail_to_list = line[18]
            send_sms = line[19]
            send_sms_to_list = line[20]
            tags = line[21]
            db_type = line[22]
            if send_mail_to_list is None or send_mail_to_list.strip() == '':
                send_mail_to_list = mail_to_list_common
            if send_sms_to_list is None or send_sms_to_list.strip() == '':
                send_sms_to_list = sms_to_list_common
            if connect <> 1:
                send_mail = func.update_send_mail_status(server_id, db_type, 'connect', send_mail, send_mail_max_count)
                send_sms = func.update_send_sms_status(server_id, db_type, 'connect', send_sms, send_sms_max_count)
                func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connect', 'down', 'critical',
                               'sqlserver server down', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '3', host, port, create_time, 'connect', 'down', 'critical')
                func.update_db_status('sessions', '-1', host, port, '', '', '', '')
                func.update_db_status('actives', '-1', host, port, '', '', '', '')
                func.update_db_status('waits', '-1', host, port, '', '', '', '')
                func.update_db_status('repl', '-1', host, port, '', '', '', '')
                func.update_db_status('repl_delay', '-1', host, port, '', '', '', '')
            else:
                func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'connect', 'up',
                                 'sqlserver server up', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '1', host, port, create_time, 'connect', 'up', 'ok')
                if int(alarm_processes) == 1:
                    if int(processes) >= int(threshold_critical_processes):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'processes', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'processes', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'processes', processes,
                                       'critical', 'too many processes', send_mail, send_mail_to_list, send_sms,
                                       send_sms_to_list)
                        func.update_db_status('sessions', 3, host, port, create_time, 'processes', processes,
                                              'critical')
                    elif int(processes) >= int(threshold_warning_processes):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'processes', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'processes', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'processes', processes,
                                       'warning', 'too many processes', send_mail, send_mail_to_list, send_sms,
                                       send_sms_to_list)
                        func.update_db_status('sessions', 2, host, port, create_time, 'processes', processes, 'warning')
                    else:
                        func.update_db_status('sessions', 1, host, port, create_time, 'processes', processes, 'ok')
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'processes', processes,
                                         'processes ok', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                if int(alarm_processes_running) == 1:
                    if int(processes_running) >= int(threshold_critical_processes_running):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'processes_running', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'processes_running', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'processes_running',
                                       processes_runnging, 'critical', 'too many processes running', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 3, host, port, create_time, 'processes_running',
                                              processes_running, 'critical')
                    elif int(processes_running) >= int(threshold_warning_processes_running):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'processes_running', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'processes_running', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'processes_running',
                                       processes_running, 'critical', 'too many processes running', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 2, host, port, create_time, 'processes_running',
                                              processes_running, 'warning')
                    else:
                        func.update_db_status('actives', 1, host, port, create_time, 'processes_running',
                                              processes_running, 'ok')
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'processes_running',
                                         processes_running, 'processes running ok', send_mail, send_mail_to_list,
                                         send_sms, send_sms_to_list)
                if int(alarm_processes_waits) == 1:
                    if int(processes_waits) >= int(threshold_critical_processes_waits):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'processes_waits', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'processes_waits', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'processes_waits',
                                       processes_waits, 'critical', 'too many processes waits', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('waits', 3, host, port, create_time, 'processes_waits', processes_waits,
                                              'critical')
                    elif int(processes_waits) >= int(threshold_warning_processes_waits):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'processes_waits', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'processes_waits', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'processes_waits',
                                       processes_waits, 'warning', 'too many processes waits', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('waits', 2, host, port, create_time, 'processes_waits', processes_waits,
                                              'warning')
                    else:
                        func.update_db_status('waits', 1, host, port, create_time, 'processes_waits', processes_waits,
                                              'ok')
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'processes_waits',
                                         processes_waits, 'processes waits ok', send_mail, send_mail_to_list, send_sms,
                                         send_sms_to_list)
    else:
        pass


def get_alarm_oracle_tablespace():
    sql = "select a.server_id,a.tablespace_name,a.total_size,a.used_size,a.avail_size,a.used_rate,a.create_time,b.host,b.port,b.alarm_tablespace,b.threshold_warning_tablespace,b.threshold_critical_tablespace,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list,b.tags,'oracle' as db_type from oracle_tablespace a, db_servers_oracle b where a.server_id=b.id  order by SUBSTRING_INDEX(used_rate,'%',1)+0 asc;"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            server_id = line[0]
            tablespace_name = line[1]
            total_size = line[2]
            used_size = line[3]
            avail_size = line[4]
            used_rate = line[5]
            create_time = line[6]
            host = line[7]
            port = line[8]
            alarm_tablespace = line[9]
            threshold_warning_tablespace = line[10]
            threshold_critical_tablespace = line[11]
            send_mail = line[12]
            send_mail_to_list = line[13]
            send_sms = line[14]
            send_sms_to_list = line[15]
            tags = line[16]
            db_type = line[17]
            used_rate_arr = used_rate.split("%")
            used_rate_int = int(used_rate_arr[0])

            if send_mail_to_list is None or send_mail_to_list.strip() == '':
                send_mail_to_list = mail_to_list_common
            if send_sms_to_list is None or send_sms_to_list.strip() == '':
                send_sms_to_list = sms_to_list_common

            if int(alarm_tablespace) == 1:
                if int(used_rate_int) >= int(threshold_critical_tablespace):
                    send_mail = func.update_send_mail_status(server_id, db_type, 'tablespace(%s)' % (tablespace_name),
                                                             send_mail, send_mail_max_count)
                    send_sms = func.update_send_sms_status(server_id, db_type, 'tablespace(%s)' % (tablespace_name),
                                                           send_sms, send_sms_max_count)
                    func.add_alarm(server_id, tags, host, port, create_time, db_type,
                                   'tablespace(%s)' % (tablespace_name), used_rate, 'critical',
                                   'tablespace %s usage reach %s' % (tablespace_name, used_rate), send_mail,
                                   send_mail_to_list, send_sms, send_sms_to_list)
                    func.update_db_status('tablespace', 3, host, port, create_time,
                                          'tablespace(%s)' % (tablespace_name), used_rate, 'critical')
                elif int(used_rate_int) >= int(threshold_warning_tablespace):
                    send_mail = func.update_send_mail_status(server_id, db_type, 'tablespace(%s)' % (tablespace_name),
                                                             send_mail, send_mail_max_count)
                    send_sms = func.update_send_sms_status(server_id, db_type, 'tablespace(%s)' % (tablespace_name),
                                                           send_sms, send_sms_max_count)
                    func.add_alarm(server_id, tags, host, port, create_time, db_type,
                                   'tablespace(%s)' % (tablespace_name), used_rate, 'warning',
                                   'tablespace %s usage reach %s' % (tablespace_name, used_rate), send_mail,
                                   send_mail_to_list, send_sms, send_sms_to_list)
                    func.update_db_status('tablespace', 2, host, port, create_time,
                                          'tablespace(%s)' % (tablespace_name), used_rate, 'warning')
                else:
                    func.check_if_ok(server_id, tags, host, port, create_time, db_type,
                                     'tablespace(%s)' % (tablespace_name), used_rate,
                                     'tablespace %s usage ok' % (tablespace_name), send_mail, send_mail_to_list,
                                     send_sms, send_sms_to_list)
                    func.update_db_status('tablespace', 1, host, port, create_time, 'tablespace',
                                          'max(%s:%s)' % (tablespace_name, used_rate), 'ok')
    else:
        pass


def get_alarm_mongodb_status():
    sql = "select a.server_id,a.connect,a.connections_current,a.globalLock_activeClients,a.globalLock_currentQueue,a.create_time,b.host,b.port,b.alarm_connections_current,b.alarm_active_clients,b.alarm_current_queue,b.threshold_warning_connections_current,b.threshold_critical_connections_current,b.threshold_warning_active_clients,b.threshold_critical_active_clients,b.threshold_warning_current_queue,b.threshold_critical_current_queue,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list,b.tags,'mongodb' as db_type from mongodb_status a, db_servers_mongodb b where a.server_id=b.id;"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            server_id = line[0]
            connect = line[1]
            connections_current = line[2]
            globalLock_activeClients = line[3]
            globalLock_currentQueue = line[4]
            create_time = line[5]
            host = line[6]
            port = line[7]
            alarm_connections_current = line[8]
            alarm_active_clients = line[9]
            alarm_current_queue = line[10]
            threshold_warning_connections_current = line[11]
            threshold_critical_connections_current = line[12]
            threshold_warning_active_clients = line[13]
            threshold_critical_active_clients = line[14]
            threshold_warning_current_queue = line[15]
            threshold_critical_current_queue = line[16]
            send_mail = line[17]
            send_mail_to_list = line[18]
            send_sms = line[19]
            send_sms_to_list = line[20]
            tags = line[21]
            db_type = line[22]

            if send_mail_to_list is None or send_mail_to_list.strip() == '':
                send_mail_to_list = mail_to_list_common
            if send_sms_to_list is None or send_sms_to_list.strip() == '':
                send_sms_to_list = sms_to_list_common

            if connect <> 1:
                send_mail = func.update_send_mail_status(server_id, db_type, 'connect', send_mail, send_mail_max_count)
                send_sms = func.update_send_sms_status(server_id, db_type, 'connect', send_sms, send_sms_max_count)
                func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connect', 'down', 'critical',
                               'mongodb server down', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '3', host, port, create_time, 'connect', 'down', 'critical')
                func.update_db_status('sessions', '-1', host, port, '', '', '', '')
                func.update_db_status('actives', '-1', host, port, '', '', '', '')
                func.update_db_status('waits', '-1', host, port, '', '', '', '')
                func.update_db_status('repl', '-1', host, port, '', '', '', '')
                func.update_db_status('repl_delay', '-1', host, port, '', '', '', '')
            else:
                func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'connect', 'up',
                                 'mongodb server up', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '1', host, port, create_time, 'connect', 'up', 'ok')
                if int(alarm_connections_current) == 1:
                    if int(connections_current) >= int(threshold_critical_connections_current):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'connections_current', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'connections_current', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connections_current',
                                       connections_current, 'critical', 'too many connections current', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 3, host, port, create_time, 'connections_current',
                                              connections_current, 'critical')
                    elif int(connections_current) >= int(threshold_warning_connections_current):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'connections_current', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'connections_current', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connections_current',
                                       connections_current, 'critical', 'too many connections current', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connections_current',
                                       connections_current, 'warning', 'too many connections current', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 2, host, port, create_time, 'connections_current',
                                              connections_current, 'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'connections_current',
                                         connections_current, 'connections current ok', send_mail, send_mail_to_list,
                                         send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 1, host, port, create_time, 'connections_current',
                                              connections_current, 'ok')

                if int(alarm_active_clients) == 1:
                    if int(globalLock_activeClients) >= int(threshold_critical_active_clients):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'active_clients', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'active_clients', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connections_current',
                                       connections_current, 'critical', 'too many connections current', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'active_clients',
                                       globalLock_activeClients, 'critical', 'too many active clients', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 3, host, port, create_time, 'active_clients',
                                              globalLock_activeClients, 'critical')
                    elif int(globalLock_activeClients) >= int(threshold_warning_active_clients):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'active_clients', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'active_clients', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'active_clients',
                                       globalLock_activeClients, 'warning', 'too many active clients', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 2, host, port, create_time, 'active_clients',
                                              globalLock_activeClients, 'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'active_clients',
                                         globalLock_activeClients, 'active clients ok', send_mail, send_mail_to_list,
                                         send_sms, send_sms_to_list)
                        func.update_db_status('actives', 1, host, port, create_time, 'active_clients',
                                              globalLock_activeClients, 'ok')
                if int(alarm_current_queue) == 1:
                    if int(globalLock_currentQueue) >= int(threshold_critical_current_queue):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'current_queue', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'current_queue', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'current_queue',
                                       globalLock_currentQueue, 'critical', 'too many current queue', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('waits', 3, host, port, create_time, 'current_queue',
                                              globalLock_currentQueue, 'critical')
                    elif int(globalLock_currentQueue) >= int(threshold_warning_current_queue):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'current_queue', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'current_queue', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'current_queue',
                                       globalLock_currentQueue, 'warning', 'too many current queue', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('waits', 2, host, port, create_time, 'current_queue',
                                              globalLock_currentQueue, 'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'current_queue',
                                         globalLock_currentQueue, 'current queue ok', send_mail, send_mail_to_list,
                                         send_sms, send_sms_to_list)
                        func.update_db_status('waits', 1, host, port, create_time, 'current_queue',
                                              globalLock_currentQueue, 'ok')
    else:
        pass


def get_alarm_redis_status():
    sql = "select a.server_id,a.connect,a.connected_clients,a.current_commands_processed,a.blocked_clients,a.create_time,b.host,b.port,b.alarm_connected_clients,b.alarm_command_processed,b.alarm_blocked_clients,b.threshold_warning_connected_clients,b.threshold_critical_connected_clients,b.threshold_warning_command_processed,b.threshold_critical_command_processed,b.threshold_warning_blocked_clients,b.threshold_critical_blocked_clients,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list,b.tags,'redis' as db_type from redis_status a, db_servers_redis b where a.server_id=b.id ;"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            server_id = line[0]
            connect = line[1]
            connected_clients = line[2]
            current_commands_processed = line[3]
            blocked_clients = line[4]
            create_time = line[5]
            host = line[6]
            port = line[7]
            alarm_connected_clients = line[8]
            alarm_command_processed = line[9]
            alarm_blocked_clients = line[10]
            threshold_warning_connected_clients = line[11]
            threshold_critical_connected_clients = line[12]
            threshold_warning_command_processed = line[13]
            threshold_critical_command_processed = line[14]
            threshold_warning_blocked_clients = line[15]
            threshold_critical_blocked_clients = line[16]
            send_mail = line[17]
            send_mail_to_list = line[18]
            send_sms = line[19]
            send_sms_to_list = line[20]
            tags = line[21]
            db_type = line[22]
            if send_mail_to_list is None or send_mail_to_list.strip() == '':
                send_mail_to_list = mail_to_list_common
            if send_sms_to_list is None or send_sms_to_list.strip() == '':
                send_sms_to_list = sms_to_list_common
            if connect <> 1:
                send_mail = func.update_send_mail_status(server_id, db_type, 'connect', send_mail, send_mail_max_count)
                send_sms = func.update_send_sms_status(server_id, db_type, 'connect', send_sms, send_sms_max_count)
                func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connect', 'down', 'critical',
                               'redis server down', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '3', host, port, create_time, 'connect', 'down', 'critical')
                func.update_db_status('sessions', '-1', host, port, '', '', '', '')
                func.update_db_status('actives', '-1', host, port, '', '', '', '')
                func.update_db_status('waits', '-1', host, port, '', '', '', '')
                func.update_db_status('repl', '-1', host, port, '', '', '', '')
                func.update_db_status('repl_delay', '-1', host, port, '', '', '', '')
            else:
                func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'connect', 'up', 'redis server up',
                                 send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('connect', '1', host, port, create_time, 'connect', 'up', 'ok')
                if int(alarm_connected_clients) == 1:
                    if int(connected_clients) >= int(threshold_critical_connected_clients):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'connected_clients', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'connected_clients', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connected_clients',
                                       connected_clients, 'critical', 'too many connected clients', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 3, host, port, create_time, 'connected_clients',
                                              connected_clients, 'critical')
                    elif int(connected_clients) >= int(threshold_warning_connected_clients):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'connected_clients', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'connected_clients', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'connected_clients',
                                       connected_clients, 'warning', 'too many connected clients', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 2, host, port, create_time, 'connected_clients',
                                              connected_clients, 'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'connected_clients',
                                         connected_clients, 'connected clients ok', send_mail, send_mail_to_list,
                                         send_sms, send_sms_to_list)
                        func.update_db_status('sessions', 1, host, port, create_time, 'connected_clients',
                                              connected_clients, 'ok')
                if int(alarm_command_processed) == 1:
                    if int(current_commands_processed) >= int(threshold_critical_command_processed):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'command_processed', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'command_processed', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'command_processed',
                                       current_commands_processed, 'critical', 'too many command processed', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 3, host, port, create_time, 'command_processed',
                                              current_commands_processed, 'critical')
                    elif int(current_commands_processed) >= int(threshold_warning_command_processed):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'command_processed', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'command_processed', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'command_processed',
                                       current_commands_processed, 'warning', 'too many command processed', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 2, host, port, create_time, 'command_processed',
                                              current_commands_processed, 'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'command_processed',
                                         current_commands_processed, 'command processed ok', send_mail,
                                         send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('actives', 1, host, port, create_time, 'command_processed',
                                              current_commands_processed, 'ok')
                if int(alarm_blocked_clients) == 1:
                    if int(blocked_clients) >= int(threshold_critical_blocked_clients):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'blocked_clients', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'blocked_clients', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'blocked_clients',
                                       blocked_clients, 'critical', 'too many blocked clients', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('waits', 3, host, port, create_time, 'blocked_clients', blocked_clients,
                                              'critical')
                    elif int(blocked_clients) >= int(threshold_warning_blocked_clients):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'blocked_clients', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(server_id, db_type, 'blocked_clients', send_sms,
                                                               send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'blocked_clients',
                                       blocked_clients, 'warning', 'too many blocked clients', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('waits', 2, host, port, create_time, 'blocked_clients', blocked_clients,
                                              'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'blocked_clients',
                                         blocked_clients, 'blocked clients ok', send_mail, send_mail_to_list, send_sms,
                                         send_sms_to_list)
                        func.update_db_status('waits', 1, host, port, create_time, 'blocked_clients', blocked_clients,
                                              'ok')
    else:
        pass


def get_alarm_os_status():
    sql = "select a.ip,a.hostname,a.snmp,a.process,a.load_1,a.cpu_idle_time,a.mem_usage_rate,a.create_time,b.tags,b.alarm_os_process,b.alarm_os_load,b.alarm_os_cpu,b.alarm_os_memory,b.threshold_warning_os_process,b.threshold_critical_os_process,b.threshold_warning_os_load,b.threshold_critical_os_load,b.threshold_warning_os_cpu,b.threshold_critical_os_cpu,b.threshold_warning_os_memory,b.threshold_critical_os_memory,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list from os_status a,db_servers_os b where a.ip=b.host"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            host = line[0]
            hostname = line[1]
            snmp = line[2]
            process = line[3]
            load_1 = line[4]
            cpu_idle = line[5]
            memory_usage = line[6]
            create_time = line[7]
            tags = line[8]
            alarm_os_process = line[9]
            alarm_os_load = line[10]
            alarm_os_cpu = line[11]
            alarm_os_memory = line[12]
            threshold_warning_os_process = line[13]
            threshold_critical_os_process = line[14]
            threshold_warning_os_load = line[15]
            threshold_critical_os_load = line[16]
            threshold_warning_os_cpu = line[17]
            threshold_critical_os_cpu = line[18]
            threshold_warning_os_memory = line[19]
            threshold_critical_os_memory = line[20]
            send_mail = line[21]
            send_mail_to_list = line[22]
            send_sms = line[23]
            send_sms_to_list = line[24]
            server_id = 0
            tags = tags
            db_type = "os"
            port = ''
            if send_mail_to_list is None or send_mail_to_list.strip() == '':
                send_mail_to_list = mail_to_list_common
            if send_sms_to_list is None or send_sms_to_list.strip() == '':
                send_sms_to_list = sms_to_list_common

            if snmp <> 1:
                send_mail = func.update_send_mail_status(host, db_type, 'snmp_server', send_mail, send_mail_max_count)
                send_sms = func.update_send_sms_status(host, db_type, 'snmp_server', send_sms, send_sms_max_count)
                func.add_alarm(server_id, tags, host, port, create_time, db_type, 'snmp_server', 'down', 'critical',
                               'snmp server down', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('snmp', '3', host, '', create_time, 'snmp_server', 'down', 'critical')
                func.update_db_status('process', '-1', host, '', '', '', '', '')
                func.update_db_status('load_1', '-1', host, '', '', '', '', '')
                func.update_db_status('cpu', '-1', host, '', '', '', '', '')
                func.update_db_status('memory', '-1', host, '', '', '', '', '')
                func.update_db_status('network', '-1', host, '', '', '', '', '')
                func.update_db_status('disk', '-1', host, '', '', '', '', '')
            else:
                func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'snmp_server', 'up',
                                 'snmp server up', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('snmp', 1, host, '', create_time, 'snmp_server', 'up', 'ok')
                if int(alarm_os_process) == 1:
                    if int(process) >= int(threshold_critical_os_process):
                        send_mail = func.update_send_mail_status(host, db_type, 'process', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(host, db_type, 'process', send_sms, send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'process', process,
                                       'critical', 'too more process running', send_mail, send_mail_to_list, send_sms,
                                       send_sms_to_list)
                        func.update_db_status('process', 3, host, '', create_time, 'process', process, 'critical')
                    elif int(process) >= int(threshold_warning_os_process):
                        send_mail = func.update_send_mail_status(host, db_type, 'process', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(host, db_type, 'process', send_sms, send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'process', process, 'warning',
                                       'too more process running', send_mail, send_mail_to_list, send_sms,
                                       send_sms_to_list)
                        func.update_db_status('process', 2, host, '', create_time, 'process', process, 'warning')
                    else:
                        func.update_db_status('process', 1, host, '', create_time, 'process', process, 'ok')
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'process', process,
                                         'process running ok', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                if int(alarm_os_load) == 1:
                    if int(load_1) >= int(threshold_critical_os_load):
                        send_mail = func.update_send_mail_status(host, db_type, 'load', send_mail, send_mail_max_count)
                        send_sms = func.update_send_sms_status(host, db_type, 'load', send_sms, send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'load', load_1, 'critical',
                                       'too high load', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('load_1', 3, host, '', create_time, 'load', load_1, 'critical')
                    elif int(load_1) >= int(threshold_warning_os_load):
                        send_mail = func.update_send_mail_status(server_id, db_type, 'load', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(host, db_type, 'load', send_sms, send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'load', load_1, 'warning',
                                       'too high load', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('load_1', 2, host, '', create_time, 'load', load_1, 'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'load', load_1, 'load ok',
                                         send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('load_1', 1, host, '', create_time, 'load', load_1, 'ok')
                if int(alarm_os_cpu) == 1:
                    threshold_critical_os_cpu = int(100 - threshold_critical_os_cpu)
                    threshold_warning_os_cpu = int(100 - threshold_warning_os_cpu)
                    if int(cpu_idle) <= int(threshold_critical_os_cpu):
                        send_mail = func.update_send_mail_status(host, db_type, 'cpu_idle', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(host, db_type, 'cpu_idle', send_sms, send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'cpu_idle',
                                       str(cpu_idle) + '%', 'critical', 'too little cpu idle', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('cpu', 3, host, '', create_time, 'cpu_idle', str(cpu_idle) + '%',
                                              'critical')
                    elif int(cpu_idle) <= int(threshold_warning_os_cpu):
                        send_mail = func.update_send_mail_status(host, db_type, 'cpu_idle', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(host, db_type, 'cpu_idle', send_sms, send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'cpu_idle',
                                       str(cpu_idle) + '%', 'warning', 'too little cpu idle', send_mail,
                                       send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('cpu', 2, host, '', create_time, 'cpu_idle', str(cpu_idle) + '%',
                                              'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'cpu_idle',
                                         str(cpu_idle) + '%', 'cpu idle ok', send_mail, send_mail_to_list, send_sms,
                                         send_sms_to_list)
                        func.update_db_status('cpu', 1, host, '', create_time, 'cpu_idle', str(cpu_idle) + '%', 'ok')
                if int(alarm_os_memory) == 1:
                    if memory_usage:
                        memory_usage_int = int(memory_usage.split('%')[0])
                    else:
                        memory_usage_int = 0
                    if int(memory_usage_int) >= int(threshold_critical_os_memory):
                        send_mail = func.update_send_mail_status(host, db_type, 'memory', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(host, db_type, 'memory', send_sms, send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'memory', memory_usage,
                                       'critical', 'too more memory usage', send_mail, send_mail_to_list, send_sms,
                                       send_sms_to_list)
                        func.update_db_status('memory', 3, host, '', create_time, 'memory', memory_usage, 'critical')
                    elif int(memory_usage_int) >= int(threshold_warning_os_memory):
                        send_mail = func.update_send_mail_status(host, db_type, 'memory', send_mail,
                                                                 send_mail_max_count)
                        send_sms = func.update_send_sms_status(host, db_type, 'memory', send_sms, send_sms_max_count)
                        func.add_alarm(server_id, tags, host, port, create_time, db_type, 'memory', memory_usage,
                                       'warning', 'too more memory usage', send_mail, send_mail_to_list, send_sms,
                                       send_sms_to_list)
                        func.update_db_status('memory', 2, host, '', create_time, 'memory', memory_usage, 'warning')
                    else:
                        func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'memory', memory_usage,
                                         'memory usage ok', send_mail, send_mail_to_list, send_sms, send_sms_to_list)
                        func.update_db_status('memory', 1, host, '', create_time, 'memory', memory_usage, 'ok')
    else:
        pass


def get_alarm_os_disk():
    sql = "select a.ip,a.mounted,a.used_rate,a.create_time,b.tags,b.alarm_os_disk,b.threshold_warning_os_disk,b.threshold_critical_os_disk,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list  from os_disk a,db_servers_os b where a.ip=b.host group by ip,mounted order by SUBSTRING_INDEX(used_rate,'%',1)+0 asc;"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            host = line[0]
            mounted = line[1]
            used_rate = line[2]
            create_time = line[3]
            tags = line[4]
            alarm_os_disk = line[5]
            threshold_warning_os_disk = line[6]
            threshold_critical_os_disk = line[7]
            send_mail = line[8]
            send_mail_to_list = line[9]
            send_sms = line[10]
            send_sms_to_list = line[11]
            server_id = 0
            tags = tags
            db_type = "os"
            port = ''
            used_rate_arr = used_rate.split("%")
            used_rate_int = int(used_rate_arr[0])

            if send_mail_to_list is None or send_mail_to_list.strip() == '':
                send_mail_to_list = mail_to_list_common
            if send_sms_to_list is None or send_sms_to_list.strip() == '':
                send_sms_to_list = sms_to_list_common

            if int(alarm_os_disk) == 1:
                if int(used_rate_int) >= int(threshold_critical_os_disk):
                    send_mail = func.update_send_mail_status(host, db_type, 'disk_usage(%s)' % (mounted), send_mail,
                                                             send_mail_max_count)
                    send_sms = func.update_send_sms_status(host, db_type, 'disk_usage(%s)' % (mounted), send_sms,
                                                           send_sms_max_count)
                    func.add_alarm(server_id, tags, host, port, create_time, db_type, 'disk_usage(%s)' % (mounted),
                                   used_rate, 'critical', 'disk %s usage reach %s' % (mounted, used_rate), send_mail,
                                   send_mail_to_list, send_sms, send_sms_to_list)
                    func.update_db_status('disk', 3, host, '', create_time, 'disk_usage(%s)' % (mounted), used_rate,
                                          'critical')
                elif int(used_rate_int) >= int(threshold_warning_os_disk):
                    send_mail = func.update_send_mail_status(host, db_type, 'disk_usage(%s)' % (mounted), send_mail,
                                                             send_mail_max_count)
                    send_sms = func.update_send_sms_status(host, db_type, 'disk_usage(%s)' % (mounted), send_sms,
                                                           send_sms_max_count)
                    func.add_alarm(server_id, tags, host, port, create_time, db_type, 'disk_usage(%s)' % (mounted),
                                   used_rate, 'warning', 'disk %s usage reach %s' % (mounted, used_rate), send_mail,
                                   send_mail_to_list, send_sms, send_sms_to_list)
                    func.update_db_status('disk', 2, host, '', create_time, 'disk_usage(%s)' % (mounted), used_rate,
                                          'warning')
                else:
                    func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'disk_usage(%s)' % (mounted),
                                     used_rate, 'disk %s usage ok' % (mounted), send_mail, send_mail_to_list, send_sms,
                                     send_sms_to_list)
                    func.update_db_status('disk', 1, host, '', create_time, 'disk_usage',
                                          'max(%s:%s)' % (mounted, used_rate), 'ok')
    else:
        pass


def get_alarm_os_network():
    sql = "select a.ip,a.if_descr,a.in_bytes,a.out_bytes,sum(in_bytes+out_bytes) sum_bytes,a.create_time,b.tags,b.alarm_os_network,b.threshold_warning_os_network,b.threshold_critical_os_network,b.send_mail,b.send_mail_to_list,b.send_sms,b.send_sms_to_list  from os_net a,db_servers_os b where a.ip=b.host group by ip,if_descr order by sum(in_bytes+out_bytes) asc;"
    result = func.mysql_query(sql)
    if result <> 0:
        for line in result:
            host = line[0]
            if_descr = line[1]
            in_bytes = line[2]
            out_bytes = line[3]
            sum_bytes = line[4]
            create_time = line[5]
            tags = line[6]
            alarm_os_network = line[7]
            threshold_warning_os_network = (line[8]) * 1024 * 1024
            threshold_critical_os_network = (line[9]) * 1024 * 1024
            send_mail = line[10]
            send_mail_to_list = line[11]
            send_sms = line[12]
            send_sms_to_list = line[13]
            server_id = 0
            tags = tags
            db_type = "os"
            port = ''

finally:
func.check_db_status(server_id, host, port, tags, 'oracle')
try:
    # get info by v$instance
    connect = 1
    instance_name = oracle.get_instance(conn, 'instance_name')
    instance_role = oracle.get_instance(conn, 'instance_role')
    database_role = oracle.get_database(conn, 'database_role')
    open_mode = oracle.get_database(conn, 'open_mode')
    protection_mode = oracle.get_database(conn, 'protection_mode')
    if database_role == 'PRIMARY':
        database_role_new = 'm'
        dg_stats = '-1'
        dg_delay = '-1'
    else:
        database_role_new = 's'
        # dg_stats = oracle.get_stats(conn)
        # dg_delay = oracle.get_delay(conn)
        dg_stats = '1'
        dg_delay = '1'
    instance_status = oracle.get_instance(conn, 'status')
    startup_time = oracle.get_instance(conn, 'startup_time')
    # print startup_time
    # startup_time = time.strftime('%Y-%m-%d %H:%M:%S',startup_time)
    # localtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    # uptime =  (localtime - startup_time).seconds
    # print uptime
    uptime = oracle.get_instance(conn, 'startup_time')
    version = oracle.get_instance(conn, 'version')
    instance_status = oracle.get_instance(conn, 'status')
    database_status = oracle.get_instance(conn, 'database_status')
    host_name = oracle.get_instance(conn, 'host_name')
    archiver = oracle.get_instance(conn, 'archiver')
    # get info by sql count
    session_total = oracle.get_sessions(conn)
    session_actives = oracle.get_actives(conn)
    session_waits = oracle.get_waits(conn)
    # get info by v$parameters
    parameters = oracle.get_parameters(conn)
    processes = parameters['processes']

    ##get info by v$parameters
    sysstat_0 = oracle.get_sysstat(conn)
    time.sleep(1)
    sysstat_1 = oracle.get_sysstat(conn)
    session_logical_reads_persecond = sysstat_1['session logical reads'] - sysstat_0['session logical reads']
    physical_reads_persecond = sysstat_1['physical reads'] - sysstat_0['physical reads']
    physical_writes_persecond = sysstat_1['physical writes'] - sysstat_0['physical writes']
    physical_read_io_requests_persecond = sysstat_1['physical write total IO requests'] - sysstat_0[
        'physical write total IO requests']
    physical_write_io_requests_persecond = sysstat_1['physical read IO requests'] - sysstat_0[
        'physical read IO requests']
    db_block_changes_persecond = sysstat_1['db block changes'] - sysstat_0['db block changes']
    os_cpu_wait_time = sysstat_0['OS CPU Qt wait time']
    logons_persecond = sysstat_1['logons cumulative'] - sysstat_0['logons cumulative']
    logons_current = sysstat_0['logons current']
    opened_cursors_persecond = sysstat_1['opened cursors cumulative'] - sysstat_0['opened cursors cumulative']
    opened_cursors_current = sysstat_0['opened cursors current']
    user_commits_persecond = sysstat_1['user commits'] - sysstat_0['user commits']
    user_rollbacks_persecond = sysstat_1['user rollbacks'] - sysstat_0['user rollbacks']
    user_calls_persecond = sysstat_1['user calls'] - sysstat_0['user calls']
    db_block_gets_persecond = sysstat_1['db block gets'] - sysstat_0['db block gets']
    # print session_logical_reads_persecond
    ##################### insert data to mysql server#############################
    sql = "insert into oracle_status(server_id,host,port,tags,connect,instance_name,instance_role,instance_status,database_role,open_mode,protection_mode,host_name,database_status,startup_time,uptime,version,archiver,session_total,session_actives,session_waits,dg_stats,dg_delay,processes,session_logical_reads_persecond,physical_reads_persecond,physical_writes_persecond,physical_read_io_requests_persecond,physical_write_io_requests_persecond,db_block_changes_persecond,os_cpu_wait_time,logons_persecond,logons_current,opened_cursors_persecond,opened_cursors_current,user_commits_persecond,user_rollbacks_persecond,user_calls_persecond,db_block_gets_persecond) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    param = (
    server_id, host, port, tags, connect, instance_name, instance_role, instance_status, database_role, open_mode,
    protection_mode, host_name, database_status, startup_time, uptime, version, archiver, session_total,
    session_actives, session_waits, dg_stats, dg_delay, processes, session_logical_reads_persecond,
    physical_reads_persecond, physical_writes_persecond, physical_read_io_requests_persecond,
    physical_write_io_requests_persecond, db_block_changes_persecond, os_cpu_wait_time, logons_persecond,
    logons_current, opened_cursors_persecond, opened_cursors_current, user_commits_persecond, user_rollbacks_persecond,
    user_calls_persecond, db_block_gets_persecond)
    func.mysql_exec(sql, param)
    func.update_db_status_init(database_role_new, version, host, port, tags)
    # check tablespace
    tablespace = oracle.get_tablespace(conn)
    if tablespace:
        for line in tablespace:
            sql = "insert into oracle_tablespace(server_id,host,port,tags,tablespace_name,total_size,used_size,avail_size,used_rate) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            param = (server_id, host, port, tags, line[0], line[1], line[2], line[3], line[4])
            func.mysql_exec(sql, param)

except Exception, e:
    logger.error(e)
    sys.exit(1)
finally:
    conn.close()


def main():
    func.mysql_exec(
        "insert into oracle_status_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from oracle_status;",
        '')
    func.mysql_exec('delete from oracle_status;', '')
    func.mysql_exec(
        "insert into oracle_tablespace_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from oracle_tablespace;",
        '')
    func.mysql_exec('delete from oracle_tablespace;', '')
    # get oracle servers list
    servers = func.mysql_query(
        "select id,host,port,dsn,username,password,tags from db_servers_oracle where is_delete=0 and monitor=1;")
    logger.info("check oracle controller start.")
    if servers:
        plist = []
        for row in servers:
            server_id = row[0]
            host = row[1]
            port = row[2]
            dsn = row[3]
            username = row[4]
            password = row[5]
            tags = row[6]
            p = Process(target=check_oracle, args=(host, port, dsn, username, password, server_id, tags))
            plist.append(p)
            p.start()
        # time.sleep(10)
        # for p in plist:
        #    p.terminate()
        for p in plist:
            p.join()
    else:
        logger.warning("check oracle: not found any servers")
    logger.info("check oracle controller finished.")


cmdline.execute("scrapy crawl pornHubSpider".split())
from multiprocessing import Process;

dbhost = func.get_config('monitor_server', 'host')
dbport = func.get_config('monitor_server', 'port')
dbuser = func.get_config('monitor_server', 'user')
dbpasswd = func.get_config('monitor_server', 'passwd')
dbname = func.get_config('monitor_server', 'dbname')


def check_os(ip, community, filter_os_disk, tags):
    func.mysql_exec(
        "insert into os_status_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from os_status where ip='%s';" % (
            ip), '')
    func.mysql_exec(
        "insert into os_disk_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from os_disk where ip='%s';" % (
            ip), '')
    func.mysql_exec(
        "insert into os_diskio_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from os_diskio where ip='%s';" % (
            ip), '')
    func.mysql_exec(
        "insert into os_net_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from os_net where ip='%s';" % (
            ip), '')
    func.mysql_exec("delete from os_status where ip='%s'" % (ip), '')
    func.mysql_exec("delete from os_disk where ip='%s'" % (ip), '')
    func.mysql_exec("delete from os_diskio where ip='%s'" % (ip), '')
    func.mysql_exec("delete from os_net where ip='%s'" % (ip), '')
    command = "sh check_os.sh"
    try:
        os.system("%s %s %s %s %s %s %s %s %s %s" % (
        command, ip, dbhost, dbport, dbuser, dbpasswd, dbname, community, filter_os_disk, tags))
        logger.info("%s:%s statspack complete." % (dbhost, dbport))

    except Exception, e:
        print
        e
        logger.info("%s:%s statspack error: %s" % (dbhost, dbport, e))
        sys.exit(1)
    finally:
        sys.exit(1)


def main():
    # get os servers list
    servers = func.mysql_query(
        "select host,community,filter_os_disk,tags from db_servers_os where is_delete=0 and monitor=1;")

    logger.info("check os controller started.")
    if servers:
        plist = []
        for row in servers:
            host = row[0]
            community = row[1]
            filter_os_disk = row[2]
            tags = row[3]
            if host <> '':
                thread.start_new_thread(check_os, (host, community, filter_os_disk, tags))
                time.sleep(1)
                # p = Process(target = check_os, args=(host,filter_os_disk))
                # plist.append(p)
                # p.start()
        # for p in plist:
        #    p.join()
    else:
        logger.warning("check os: not found any servers")
    logger.info("check os controller finished.")


logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("Python")
path = './include'
sys.path.insert(0, path)
import functions as func
from multiprocessing import Process;


def check_value(info, key):
    try:
        key_tmp = key
        key_tmp = info['%s' % (key)]
        if key_tmp == key:
            key_tmp = '-1'
            logger_msg = "check redis %s:%s : %s is not supported for this version" % (host, port, key)
            logger.warning(logger_msg)
    except:
        key_tmp = '-1'
        logger_msg = "check redis %s:%s : %s is not supported for this version" % (host, port, key)
        logger.waring(logger_msg)
        if send_mail_to_list is None or send_mail_to_list.strip() == '':
            send_mail_to_list = mail_to_list_common
        if send_sms_to_list is None or send_sms_to_list.strip() == '':
            send_sms_to_list = sms_to_list_common

        if int(alarm_os_network) == 1:
            if int(sum_bytes) >= int(threshold_critical_os_network):
                send_mail = func.update_send_mail_status(host, db_type, 'network(%s)' % (if_descr), send_mail,
                                                         send_mail_max_count)
                send_sms = func.update_send_sms_status(host, db_type, 'network(%s)' % (if_descr), send_sms,
                                                       send_sms_max_count)
                func.add_alarm(server_id, tags, host, port, create_time, db_type, 'network(%s)' % (if_descr),
                               'in:%s,out:%s' % (in_bytes, out_bytes), 'critical',
                               'network %s bytes reach %s' % (if_descr, sum_bytes), send_mail, send_mail_to_list,
                               send_sms, send_sms_to_list)
                func.update_db_status('network', 3, host, '', create_time, 'network(%s)' % (if_descr),
                                      'in:%s,out:%s' % (in_bytes, out_bytes), 'critical')
            elif int(sum_bytes) >= int(threshold_warning_os_network):
                send_mail = func.update_send_mail_status(host, db_type, 'network(%s)' % (if_descr), send_mail,
                                                         send_mail_max_count)
                send_sms = func.update_send_sms_status(host, db_type, 'network(%s)' % (if_descr), send_sms,
                                                       send_sms_max_count)
                func.add_alarm(server_id, tags, host, port, create_time, db_type, 'network(%s)' % (if_descr),
                               'in:%s,out:%s' % (in_bytes, out_bytes), 'warning',
                               'network %s bytes reach %s' % (if_descr, sum_bytes), send_mail, send_mail_to_list,
                               send_sms, send_sms_to_list)
                func.update_db_status('network', 2, host, '', create_time, 'network(%s)' % (if_descr),
                                      'in:%s,out:%s' % (in_bytes, out_bytes), 'warning')
            else:
                func.check_if_ok(server_id, tags, host, port, create_time, db_type, 'network(%s)' % (if_descr),
                                 'in:%s,out:%s' % (in_bytes, out_bytes), 'network %s bytes ok' % (if_descr), send_mail,
                                 send_mail_to_list, send_sms, send_sms_to_list)
                func.update_db_status('network', 1, host, '', create_time, 'network',
                                      'max(%s-in:%s,out:%s)' % (if_descr, in_bytes, out_bytes), 'ok')

else:
pass


def send_alarm():
    sql = "select tags,host,port,create_time,db_type,alarm_item,alarm_value,level,message,send_mail,send_mail_to_list,send_sms,send_sms_to_list,id alarm_id from alarm;"
    result = func.mysql_query(sql)
    if result <> 0:
        send_alarm_mail = func.get_option('send_alarm_mail')
        send_alarm_sms = func.get_option('send_alarm_sms')
        for line in result:
            tags = line[0]
            host = line[1]
            port = line[2]
            create_time = line[3]
            db_type = line[4]
            alarm_item = line[5]
            alarm_value = line[6]
            level = line[7]
            message = line[8]
            send_mail = line[9]
            send_mail_to_list = line[10]
            send_sms = line[11]
            send_sms_to_list = line[12]
            alarm_id = line[13]
            if port:
                server = host + ':' + port
            else:
                server = host
            if send_mail_to_list:
                mail_to_list = send_mail_to_list.split(';')
            else:
                send_mail = 0
            if send_sms_to_list:
                sms_to_list = send_sms_to_list.split(';')
            else:
                send_sms = 0
            if int(send_alarm_mail) == 1:
                if send_mail == 1:
                    mail_subject = '[' + level + '] ' + db_type + '-' + tags + '-' + server + ' ' + message + ' Time:' + create_time.strftime(
                        '%Y-%m-%d %H:%M:%S')
                    mail_content = """
                         Type: %s\n<br/>
                         Tags: %s\n<br/> 
                         Host: %s:%s\n<br/> 
                        Level: %s\n<br/>
                         Item: %s\n<br/>  
                        Value: %s\n<br/> 
                       Message: %s\n<br/> 

                    """ % (db_type, tags, host, port, level, alarm_item, alarm_value, message)
                    result = sendmail.send_mail(mail_to_list, mail_subject, mail_content)
                    if result:
                        send_mail_status = 1
                    else:
                        send_mail_status = 0
                else:
                    send_mail_status = 0
            else:
                send_mail_status = 0

            if int(send_alarm_sms) == 1:
                if send_sms == 1:
                    sms_msg = '[' + level + '] ' + db_type + '-' + tags + '-' + server + ' ' + message + ' Time:' + create_time.strftime(
                        '%Y-%m-%d %H:%M:%S')
                    send_sms_type = func.get_option('smstype')
                    if send_sms_type == 'fetion':
                        result = sendsms_fx.send_sms(sms_to_list, sms_msg, db_type, tags, host, port, level, alarm_item,
                                                     alarm_value, message)
                    else:
                        result = sendsms_api.send_sms(sms_to_list, sms_msg, db_type, tags, host, port, level,
                                                      alarm_item, alarm_value, message)
                    if result:
                        send_sms_status = 1
                    else:
                        send_sms_status = 0
                else:
                    send_sms_status = 0
            else:
                send_sms_status = 0
            try:
                sql = "insert into alarm_history(server_id,tags,host,port,create_time,db_type,alarm_item,alarm_value,level,message,send_mail,send_mail_to_list,send_sms,send_sms_to_list,send_mail_status,send_sms_status) select server_id,tags,host,port,create_time,db_type,alarm_item,alarm_value,level,message,send_mail,send_mail_to_list,send_sms,send_sms_to_list,%s,%s from alarm where id=%s;"
                param = (send_mail_status, send_sms_status, alarm_id)
                func.mysql_exec(sql, param)
            except Exception, e:
                print
                e
        func.mysql_exec("delete from alarm", '')
    else:
        pass


def check_send_alarm_sleep():
    send_mail_sleep_time = func.get_option('send_mail_sleep_time')
    send_sms_sleep_time = func.get_option('send_sms_sleep_time')
    if send_mail_sleep_time:
        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        format = "%Y-%m-%d %H:%M:%S"
        send_mail_sleep_time_format = "%d" % (int(send_mail_sleep_time))
        result = datetime.datetime(*time.strptime(now_time, format)[:6]) - datetime.timedelta(
            minutes=int(send_mail_sleep_time_format))
        sleep_alarm_time = result.strftime(format)
        sql = "delete from alarm_temp where alarm_type='mail' and create_time <= %s"
        param = (sleep_alarm_time)
        func.mysql_exec(sql, param)
    if send_sms_sleep_time:
        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        format = "%Y-%m-%d %H:%M:%S"
        send_sms_sleep_time_format = "%d" % (int(send_sms_sleep_time))
        result = datetime.datetime(*time.strptime(now_time, format)[:6]) - datetime.timedelta(
            minutes=int(send_sms_sleep_time_format))
        sleep_alarm_time = result.strftime(format)
        sql = "delete from alarm_temp where alarm_type='sms' and create_time <= %s"
        param = (sleep_alarm_time)
        func.mysql_exec(sql, param)


def main():
    logger.info("alarm controller started.")

    check_send_alarm_sleep()
    monitor_mysql = func.get_option('monitor_mysql')
    monitor_mongodb = func.get_option('monitor_mongodb')
    monitor_sqlserver = func.get_option('monitor_sqlserver')
    monitor_oracle = func.get_option('monitor_oracle')
    monitor_redis = func.get_option('monitor_redis')
    monitor_os = func.get_option('monitor_os')

    if monitor_mysql == "1":
        get_alarm_mysql_status()
        get_alarm_mysql_replcation()

    if monitor_oracle == "1":
        get_alarm_oracle_status()
        get_alarm_oracle_tablespace()

    if monitor_sqlserver == "1":
        get_alarm_sqlserver_status()

    if monitor_mongodb == "1":
        get_alarm_mongodb_status()

    if monitor_redis == "1":
        get_alarm_redis_status()
    if monitor_os == "1":
        get_alarm_os_status()
        get_alarm_os_disk()
        get_alarm_os_network()

    send_alarm()
    func.update_check_time()
    logger.info("alarm controller finished.")


if __name__ == '__main__':
    main()
import os
import sys
import string
import time
import datetime
import MySQLdb

path = './include'
sys.path.insert(0, path)
import functions as func
from multiprocessing import Process;


def admin_mysql_purge_binlog(host, port, user, passwd, binlog_store_days):
    datalist = []
    try:
        connect = MySQLdb.connect(host=host, user=user, passwd=passwd, port=int(port), connect_timeout=2,
                                  charset='utf8')
        cur = connect.cursor()
        connect.select_db('information_schema')
        master_thread = cur.execute("select * from information_schema.processlist where COMMAND = 'Binlog Dump';")
        datalist = []
        if master_thread >= 1:
            now = datetime.datetime.now()
            delta = datetime.timedelta(days=binlog_store_days)
            n_days = now - delta
            before_n_days = n_days.strftime('%Y-%m-%d %H:%M:%S')
            cur.execute("purge binary logs before  '%s'" % (before_n_days));
            print("mysql %s:%s binlog been purge" % (host, port))
    except MySQLdb.Error, e:
        pass
        print
        "Mysql Error %d: %s" % (e.args[0], e.args[1])


def main():
    user = func.get_config('mysql_db', 'username')
    passwd = func.get_config('mysql_db', 'password')
    servers = func.mysql_query(
        "select host,port,binlog_store_days from db_servers_mysql where is_delete=0 and monitor=1 and binlog_auto_purge=1;")
    if servers:
        print("%s: admin mysql purge binlog controller started." % (
        time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),));
        plist = []
        for row in servers:
            host = row[0]
            port = row[1]
            binlog_store_days = row[2]
            p = Process(target=admin_mysql_purge_binlog, args=(host, port, user, passwd, binlog_store_days))
            plist.append(p)
        for p in plist:
            p.start()
        time.sleep(60)
        for p in plist:
            p.terminate()
        for p in plist:
            p.join()
        print("%s: admin mysql purge binlog controller finished." % (
        time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),))


if __name__ == '__main__':
    main()
from scrapy import cmdline
import os
import sys
import string
import time
import datetime
import MySQLdb
import pymongo
import bson
import logging
import logging.config

logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("Python")
path = './include'
sys.path.insert(0, path)
import functions as func
from multiprocessing import Process;


def check_mongodb(host, port, user, passwd, server_id, tags):
    try:
        connect = pymongo.Connection(host, int(port))
        db = connect['admin']
        db.authenticate(user, passwd)
        serverStatus = connect.admin.command(bson.son.SON([('serverStatus', 1), ('repl', 2)]))
        time.sleep(1)
        serverStatus_2 = connect.admin.command(bson.son.SON([('serverStatus', 1), ('repl', 2)]))
        connect = 1
        ok = int(serverStatus['ok'])
        version = serverStatus['version']
        uptime = serverStatus['uptime']
        connections_current = serverStatus['connections']['current']
        connections_available = serverStatus['connections']['available']
        globalLock_activeClients = serverStatus['globalLock']['activeClients']['total']
        globalLock_currentQueue = serverStatus['globalLock']['currentQueue']['total']
        indexCounters_accesses = serverStatus['indexCounters']['accesses']
        indexCounters_hits = serverStatus['indexCounters']['hits']
        indexCounters_misses = serverStatus['indexCounters']['misses']
        indexCounters_resets = serverStatus['indexCounters']['resets']
        indexCounters_missRatio = serverStatus['indexCounters']['missRatio']
        # cursors_totalOpen = serverStatus['cursors']['totalOpen']
        # cursors_timeOut =  serverStatus['cursors']['timeOut']
        dur_commits = serverStatus['dur']['commits']
        dur_journaledMB = serverStatus['dur']['journaledMB']
        dur_writeToDataFilesMB = serverStatus['dur']['writeToDataFilesMB']
        dur_compression = serverStatus['dur']['compression']
        dur_commitsInWriteLock = serverStatus['dur']['commitsInWriteLock']
        dur_earlyCommits = serverStatus['dur']['earlyCommits']
        dur_timeMs_dt = serverStatus['dur']['timeMs']['dt']
        dur_timeMs_prepLogBuffer = serverStatus['dur']['timeMs']['prepLogBuffer']
        dur_timeMs_writeToJournal = serverStatus['dur']['timeMs']['writeToJournal']
        dur_timeMs_writeToDataFiles = serverStatus['dur']['timeMs']['writeToDataFiles']
        dur_timeMs_remapPrivateView = serverStatus['dur']['timeMs']['remapPrivateView']
        mem_bits = serverStatus['mem']['bits']
        mem_resident = serverStatus['mem']['resident']
        mem_virtual = serverStatus['mem']['virtual']
        mem_supported = serverStatus['mem']['supported']
        mem_mapped = serverStatus['mem']['mapped']
        mem_mappedWithJournal = serverStatus['mem']['mappedWithJournal']
        network_bytesIn_persecond = int(serverStatus_2['network']['bytesIn']) - int(serverStatus['network']['bytesIn'])
        network_bytesOut_persecond = int(serverStatus_2['network']['bytesOut']) - int(
            serverStatus['network']['bytesOut'])
        network_numRequests_persecond = int(serverStatus_2['network']['numRequests']) - int(
            serverStatus['network']['numRequests'])
        opcounters_insert_persecond = int(serverStatus_2['opcounters']['insert']) - int(
            serverStatus['opcounters']['insert'])
        opcounters_query_persecond = int(serverStatus_2['opcounters']['query']) - int(
            serverStatus['opcounters']['query'])
        opcounters_update_persecond = int(serverStatus_2['opcounters']['update']) - int(
            serverStatus['opcounters']['update'])
        opcounters_delete_persecond = int(serverStatus_2['opcounters']['delete']) - int(
            serverStatus['opcounters']['delete'])
        opcounters_command_persecond = int(serverStatus_2['opcounters']['command']) - int(
            serverStatus['opcounters']['command'])
        # replset
        try:
            repl = serverStatus['repl']
            setName = repl['setName']
            replset = 1
            if repl['secondary'] == True:
                repl_role = 'secondary'
                repl_role_new = 's'
            else:
                repl_role = 'master'
                repl_role_new = 'm'
        except:
            replset = 0
            repl_role = 'master'
            repl_role_new = 'm'
            pass
        ##################### insert data to mysql server#############################
        sql = "insert into mongodb_status(server_id,host,port,tags,connect,replset,repl_role,ok,uptime,version,connections_current,connections_available,globalLock_currentQueue,globalLock_activeClients,indexCounters_accesses,indexCounters_hits,indexCounters_misses,indexCounters_resets,indexCounters_missRatio,dur_commits,dur_journaledMB,dur_writeToDataFilesMB,dur_compression,dur_commitsInWriteLock,dur_earlyCommits,dur_timeMs_dt,dur_timeMs_prepLogBuffer,dur_timeMs_writeToJournal,dur_timeMs_writeToDataFiles,dur_timeMs_remapPrivateView,mem_bits,mem_resident,mem_virtual,mem_supported,mem_mapped,mem_mappedWithJournal,network_bytesIn_persecond,network_bytesOut_persecond,network_numRequests_persecond,opcounters_insert_persecond,opcounters_query_persecond,opcounters_update_persecond,opcounters_delete_persecond,opcounters_command_persecond) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        param = (server_id, host, port, tags, connect, replset, repl_role, ok, uptime, version, connections_current,
                 connections_available, globalLock_currentQueue, globalLock_activeClients, indexCounters_accesses,
                 indexCounters_hits, indexCounters_misses, indexCounters_resets, indexCounters_missRatio, dur_commits,
                 dur_journaledMB, dur_writeToDataFilesMB, dur_compression, dur_commitsInWriteLock, dur_earlyCommits,
                 dur_timeMs_dt, dur_timeMs_prepLogBuffer, dur_timeMs_writeToJournal, dur_timeMs_writeToDataFiles,
                 dur_timeMs_remapPrivateView, mem_bits, mem_resident, mem_virtual, mem_supported, mem_mapped,
                 mem_mappedWithJournal, network_bytesIn_persecond, network_bytesOut_persecond,
                 network_numRequests_persecond, opcounters_insert_persecond, opcounters_query_persecond,
                 opcounters_update_persecond, opcounters_delete_persecond, opcounters_command_persecond)
        func.mysql_exec(sql, param)
        role = 'm'
        func.update_db_status_init(repl_role_new, version, host, port, tags)
    except Exception, e:
        logger_msg = "check mongodb %s:%s : %s" % (host, port, e)
        logger.warning(logger_msg)
        try:
            connect = 0
            sql = "insert into mongodb_status(server_id,host,port,tags,connect) values(%s,%s,%s,%s,%s)"
            param = (server_id, host, port, tags, connect)
            func.mysql_exec(sql, param)
        except Exception, e:
            logger.error(e)
            sys.exit(1)
        finally:
            sys.exit(1)
    finally:
        func.check_db_status(server_id, host, port, tags, 'mongodb')
        sys.exit(1)


def main():
    func.mysql_exec(
        "insert into mongodb_status_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from mongodb_status;",
        '')
    func.mysql_exec('delete from mongodb_status;', '')
    # get mongodb servers list
    servers = func.mysql_query(
        'select id,host,port,username,password,tags from db_servers_mongodb where is_delete=0 and monitor=1;')
    logger.info("check mongodb controller started.")
    if servers:
        plist = []
        for row in servers:
            server_id = row[0]
            host = row[1]
            port = row[2]
            username = row[3]
            password = row[4]
            tags = row[5]
            p = Process(target=check_mongodb, args=(host, port, username, password, server_id, tags))
            plist.append(p)
            p.start()
        for p in plist:
            p.join()
    else:
        logger.warning("check mongodb: not found any servers")
    logger.info("check mongodb controller finished.")


if __name__ == '__main__':
    main()
    import os
import sys
import string
import time
import datetime
import MySQLdb
import pymongo
import bson
import logging
import logging.config

logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("Python")
path = './include'
sys.path.insert(0, path)
import functions as func
from multiprocessing import Process;


def check_mongodb(host, port, user, passwd, server_id, tags):
    try:
        func.mysql_exec(
            "insert into mongodb_status_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from mongodb_status where server_id='%s';" % (
                server_id), '')
        func.mysql_exec("delete from mongodb_status where server_id='%s';" % (server_id), '')
        # connect = pymongo.Connection(host,int(port))
        client = pymongo.MongoClient(host, int(port))
        db = client['admin']
        db.authenticate(user, passwd)
        serverStatus = client.admin.command(bson.son.SON([('serverStatus', 1), ('repl', 2)]))
        time.sleep(1)
        serverStatus_2 = client.admin.command(bson.son.SON([('serverStatus', 1), ('repl', 2)]))
        connect = 1
        ok = int(serverStatus['ok'])
        version = serverStatus['version']
        uptime = serverStatus['uptime']
        connections_current = serverStatus['connections']['current']
        connections_available = serverStatus['connections']['available']
        globalLock_activeClients = serverStatus['globalLock']['activeClients']['total']
        globalLock_currentQueue = serverStatus['globalLock']['currentQueue']['total']
        mem_bits = serverStatus['mem']['bits']
        mem_resident = serverStatus['mem']['resident']
        mem_virtual = serverStatus['mem']['virtual']
        mem_supported = serverStatus['mem']['supported']
        mem_mapped = serverStatus['mem']['mapped']
        mem_mappedWithJournal = serverStatus['mem']['mappedWithJournal']
        network_bytesIn_persecond = int(serverStatus_2['network']['bytesIn']) - int(serverStatus['network']['bytesIn'])
        network_bytesOut_persecond = int(serverStatus_2['network']['bytesOut']) - int(
            serverStatus['network']['bytesOut'])
        network_numRequests_persecond = int(serverStatus_2['network']['numRequests']) - int(
            serverStatus['network']['numRequests'])
        opcounters_insert_persecond = int(serverStatus_2['opcounters']['insert']) - int(
            serverStatus['opcounters']['insert'])
        opcounters_query_persecond = int(serverStatus_2['opcounters']['query']) - int(
            serverStatus['opcounters']['query'])
        opcounters_update_persecond = int(serverStatus_2['opcounters']['update']) - int(
            serverStatus['opcounters']['update'])
        opcounters_delete_persecond = int(serverStatus_2['opcounters']['delete']) - int(
            serverStatus['opcounters']['delete'])
        opcounters_command_persecond = int(serverStatus_2['opcounters']['command']) - int(
            serverStatus['opcounters']['command'])
        # replset
        try:
            repl = serverStatus['repl']
            setName = repl['setName']
            replset = 1
            if repl['secondary'] == True:
                repl_role = 'secondary'
                repl_role_new = 's'
            else:
                repl_role = 'master'
                repl_role_new = 'm'
        except:
            replset = 0
            repl_role = 'master'
            repl_role_new = 'm'
            pass
        ##################### insert data to mysql server#############################
        sql = "insert into mongodb_status(server_id,host,port,tags,connect,replset,repl_role,ok,uptime,version,connections_current,connections_available,globalLock_currentQueue,globalLock_activeClients,mem_bits,mem_resident,mem_virtual,mem_supported,mem_mapped,mem_mappedWithJournal,network_bytesIn_persecond,network_bytesOut_persecond,network_numRequests_persecond,opcounters_insert_persecond,opcounters_query_persecond,opcounters_update_persecond,opcounters_delete_persecond,opcounters_command_persecond) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        param = (server_id, host, port, tags, connect, replset, repl_role, ok, uptime, version, connections_current,
                 connections_available, globalLock_currentQueue, globalLock_activeClients, mem_bits, mem_resident,
                 mem_virtual, mem_supported, mem_mapped, mem_mappedWithJournal, network_bytesIn_persecond,
                 network_bytesOut_persecond, network_numRequests_persecond, opcounters_insert_persecond,
                 opcounters_query_persecond, opcounters_update_persecond, opcounters_delete_persecond,
                 opcounters_command_persecond)
        func.mysql_exec(sql, param)
        role = 'm'
        func.update_db_status_init(repl_role_new, version, host, port, tags)
    except Exception, e:
        logger_msg = "check mongodb %s:%s : %s" % (host, port, e)
        logger.warning(logger_msg)
        try:
            connect = 0
            sql = "insert into mongodb_status(server_id,host,port,tags,connect) values(%s,%s,%s,%s,%s)"
            param = (server_id, host, port, tags, connect)
            func.mysql_exec(sql, param)
        except Exception, e:
            logger.error(e)
            sys.exit(1)
        finally:
            sys.exit(1)
    finally:
        func.check_db_status(server_id, host, port, tags, 'mongodb')
        sys.exit(1)


def main():
    servers = func.mysql_query(
        'select id,host,port,username,password,tags from db_servers_mongodb where is_delete=0 and monitor=1;')
    logger.info("check mongodb controller started.")
    if servers:
        plist = []
        for row in servers:
            server_id = row[0]
            host = row[1]
            port = row[2]
            username = row[3]
            password = row[4]
            tags = row[5]
            p = Process(target=check_mongodb, args=(host, port, username, password, server_id, tags))
            plist.append(p)
            p.start()
        for p in plist:
            p.join()
    else:
        logger.warning("check mongodb: not found any servers")
    logger.info("check mongodb controller finished.")


if __name__ == '__main__':
    main()
    import os
import sys
import string
import time
import datetime
import MySQLdb
import logging
import logging.config

logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("Python")
path = './include'
sys.path.insert(0, path)
import functions as func
import Python_mysql as mysql
from multiprocessing import Process;


def check_mysql(host, port, username, password, server_id, tags):
    try:
        conn = MySQLdb.connect(host=host, user=username, passwd=password, port=int(port), connect_timeout=3,
                               charset='utf8')
        cur = conn.cursor()
        conn.select_db('information_schema')
        # cur.execute('flush hosts;')
        ############################# CHECK MYSQL ####################################################
        mysql_variables = func.get_mysql_variables(cur)
        mysql_status = func.get_mysql_status(cur)
        time.sleep(1)
        mysql_status_2 = func.get_mysql_status(cur)
        ############################# GET VARIABLES ###################################################
        version = func.get_item(mysql_variables, 'version')
        key_buffer_size = func.get_item(mysql_variables, 'key_buffer_size')
        sort_buffer_size = func.get_item(mysql_variables, 'sort_buffer_size')
        join_buffer_size = func.get_item(mysql_variables, 'join_buffer_size')
        max_connections = func.get_item(mysql_variables, 'max_connections')
        max_connect_errors = func.get_item(mysql_variables, 'max_connect_errors')
        open_files_limit = func.get_item(mysql_variables, 'open_files_limit')
        table_open_cache = func.get_item(mysql_variables, 'table_open_cache')
        max_tmp_tables = func.get_item(mysql_variables, 'max_tmp_tables')
        max_heap_table_size = func.get_item(mysql_variables, 'max_heap_table_size')
        max_allowed_packet = func.get_item(mysql_variables, 'max_allowed_packet')
        ############################# GET INNODB INFO ##################################################
        # innodb variables
        innodb_version = func.get_item(mysql_variables, 'innodb_version')
        innodb_buffer_pool_instances = func.get_item(mysql_variables, 'innodb_buffer_pool_instances')
        innodb_buffer_pool_size = func.get_item(mysql_variables, 'innodb_buffer_pool_size')
        innodb_doublewrite = func.get_item(mysql_variables, 'innodb_doublewrite')
        innodb_file_per_table = func.get_item(mysql_variables, 'innodb_file_per_table')
        innodb_flush_log_at_trx_commit = func.get_item(mysql_variables, 'innodb_flush_log_at_trx_commit')
        innodb_flush_method = func.get_item(mysql_variables, 'innodb_flush_method')
        innodb_force_recovery = func.get_item(mysql_variables, 'innodb_force_recovery')
        innodb_io_capacity = func.get_item(mysql_variables, 'innodb_io_capacity')
        innodb_read_io_threads = func.get_item(mysql_variables, 'innodb_read_io_threads')
        innodb_write_io_threads = func.get_item(mysql_variables, 'innodb_write_io_threads')
        # innodb status
        innodb_buffer_pool_pages_total = int(func.get_item(mysql_status, 'Innodb_buffer_pool_pages_total'))
        innodb_buffer_pool_pages_data = int(func.get_item(mysql_status, 'Innodb_buffer_pool_pages_data'))
        innodb_buffer_pool_pages_dirty = int(func.get_item(mysql_status, 'Innodb_buffer_pool_pages_dirty'))
        innodb_buffer_pool_pages_flushed = int(func.get_item(mysql_status, 'Innodb_buffer_pool_pages_flushed'))
        innodb_buffer_pool_pages_free = int(func.get_item(mysql_status, 'Innodb_buffer_pool_pages_free'))
        innodb_buffer_pool_pages_misc = int(func.get_item(mysql_status, 'Innodb_buffer_pool_pages_misc'))
        innodb_page_size = int(func.get_item(mysql_status, 'Innodb_page_size'))
        innodb_pages_created = int(func.get_item(mysql_status, 'Innodb_pages_created'))
        innodb_pages_read = int(func.get_item(mysql_status, 'Innodb_pages_read'))
        innodb_pages_written = int(func.get_item(mysql_status, 'Innodb_pages_written'))
        innodb_row_lock_current_waits = int(func.get_item(mysql_status, 'Innodb_row_lock_current_waits'))
        # innodb persecond info
        innodb_buffer_pool_read_requests_persecond = int(
            func.get_item(mysql_status_2, 'Innodb_buffer_pool_read_requests')) - int(
            func.get_item(mysql_status, 'Innodb_buffer_pool_read_requests'))
        innodb_buffer_pool_reads_persecond = int(func.get_item(mysql_status_2, 'Innodb_buffer_pool_reads')) - int(
            func.get_item(mysql_status, 'Innodb_buffer_pool_reads'))
        innodb_buffer_pool_write_requests_persecond = int(
            func.get_item(mysql_status_2, 'Innodb_buffer_pool_write_requests')) - int(
            func.get_item(mysql_status, 'Innodb_buffer_pool_write_requests'))
        innodb_buffer_pool_pages_flushed_persecond = int(
            func.get_item(mysql_status_2, 'Innodb_buffer_pool_pages_flushed')) - int(
            func.get_item(mysql_status, 'Innodb_buffer_pool_pages_flushed'))
        innodb_rows_deleted_persecond = int(func.get_item(mysql_status_2, 'Innodb_rows_deleted')) - int(
            func.get_item(mysql_status, 'Innodb_rows_deleted'))
        innodb_rows_inserted_persecond = int(func.get_item(mysql_status_2, 'Innodb_rows_inserted')) - int(
            func.get_item(mysql_status, 'Innodb_rows_inserted'))
        innodb_rows_read_persecond = int(func.get_item(mysql_status_2, 'Innodb_rows_read')) - int(
            func.get_item(mysql_status, 'Innodb_rows_read'))
        innodb_rows_updated_persecond = int(func.get_item(mysql_status_2, 'Innodb_rows_updated')) - int(
            func.get_item(mysql_status, 'Innodb_rows_updated'))
        ############################# GET STATUS ##################################################
        connect = 1
        uptime = func.get_item(mysql_status, 'Uptime')
        open_files = func.get_item(mysql_status, 'Open_files')
        open_tables = func.get_item(mysql_status, 'Open_tables')
        threads_connected = func.get_item(mysql_status, 'Threads_connected')
        threads_running = func.get_item(mysql_status, 'Threads_running')
        threads_created = func.get_item(mysql_status, 'Threads_created')
        threads_cached = func.get_item(mysql_status, 'Threads_cached')
        threads_waits = mysql.get_waits(conn)
        connections = func.get_item(mysql_status, 'Connections')
        aborted_clients = func.get_item(mysql_status, 'Aborted_clients')
        aborted_connects = func.get_item(mysql_status, 'Aborted_connects')
        key_blocks_not_flushed = func.get_item(mysql_status, 'Key_blocks_not_flushed')
        key_blocks_unused = func.get_item(mysql_status, 'Key_blocks_unused')
        key_blocks_used = func.get_item(mysql_status, 'Key_blocks_used')
        ############################# GET STATUS PERSECOND ##################################################
        connections_persecond = int(func.get_item(mysql_status_2, 'Connections')) - int(
            func.get_item(mysql_status, 'Connections'))
        bytes_received_persecond = (int(func.get_item(mysql_status_2, 'Bytes_received')) - int(
            func.get_item(mysql_status, 'Bytes_received'))) / 1024
        bytes_sent_persecond = (int(func.get_item(mysql_status_2, 'Bytes_sent')) - int(
            func.get_item(mysql_status, 'Bytes_sent'))) / 1024
        com_select_persecond = int(func.get_item(mysql_status_2, 'Com_select')) - int(
            func.get_item(mysql_status, 'Com_select'))
        com_insert_persecond = int(func.get_item(mysql_status_2, 'Com_insert')) - int(
            func.get_item(mysql_status, 'Com_insert'))
        com_update_persecond = int(func.get_item(mysql_status_2, 'Com_update')) - int(
            func.get_item(mysql_status, 'Com_update'))
        com_delete_persecond = int(func.get_item(mysql_status_2, 'Com_delete')) - int(
            func.get_item(mysql_status, 'Com_delete'))
        com_commit_persecond = int(func.get_item(mysql_status_2, 'Com_commit')) - int(
            func.get_item(mysql_status, 'Com_commit'))
        com_rollback_persecond = int(func.get_item(mysql_status_2, 'Com_rollback')) - int(
            func.get_item(mysql_status, 'Com_rollback'))
        questions_persecond = int(func.get_item(mysql_status_2, 'Questions')) - int(
            func.get_item(mysql_status, 'Questions'))
        queries_persecond = int(func.get_item(mysql_status_2, 'Queries')) - int(func.get_item(mysql_status, 'Queries'))
        transaction_persecond = (int(func.get_item(mysql_status_2, 'Com_commit')) + int(
            func.get_item(mysql_status_2, 'Com_rollback'))) - (int(func.get_item(mysql_status, 'Com_commit')) + int(
            func.get_item(mysql_status, 'Com_rollback')))
        created_tmp_disk_tables_persecond = int(func.get_item(mysql_status_2, 'Created_tmp_disk_tables')) - int(
            func.get_item(mysql_status, 'Created_tmp_disk_tables'))
        created_tmp_files_persecond = int(func.get_item(mysql_status_2, 'Created_tmp_files')) - int(
            func.get_item(mysql_status, 'Created_tmp_files'))
        created_tmp_tables_persecond = int(func.get_item(mysql_status_2, 'Created_tmp_tables')) - int(
            func.get_item(mysql_status, 'Created_tmp_tables'))
        table_locks_immediate_persecond = int(func.get_item(mysql_status_2, 'Table_locks_immediate')) - int(
            func.get_item(mysql_status, 'Table_locks_immediate'))
        table_locks_waited_persecond = int(func.get_item(mysql_status_2, 'Table_locks_waited')) - int(
            func.get_item(mysql_status, 'Table_locks_waited'))
        key_read_requests_persecond = int(func.get_item(mysql_status_2, 'Key_read_requests')) - int(
            func.get_item(mysql_status, 'Key_read_requests'))
        key_reads_persecond = int(func.get_item(mysql_status_2, 'Key_reads')) - int(
            func.get_item(mysql_status, 'Key_reads'))
        key_write_requests_persecond = int(func.get_item(mysql_status_2, 'Key_write_requests')) - int(
            func.get_item(mysql_status, 'Key_write_requests'))
        key_writes_persecond = int(func.get_item(mysql_status_2, 'Key_writes')) - int(
            func.get_item(mysql_status, 'Key_writes'))
        ############################# GET MYSQL HITRATE ##################################################
        if (string.atof(func.get_item(mysql_status, 'Qcache_hits')) + string.atof(
                func.get_item(mysql_status, 'Com_select'))) <> 0:
            query_cache_hitrate = string.atof(func.get_item(mysql_status, 'Qcache_hits')) / (
                        string.atof(func.get_item(mysql_status, 'Qcache_hits')) + string.atof(
                    func.get_item(mysql_status, 'Com_select')))
            query_cache_hitrate = "%9.2f" % query_cache_hitrate
        else:
            query_cache_hitrate = 0
        if string.atof(func.get_item(mysql_status, 'Connections')) <> 0:
            thread_cache_hitrate = 1 - string.atof(func.get_item(mysql_status, 'Threads_created')) / string.atof(
                func.get_item(mysql_status, 'Connections'))
            thread_cache_hitrate = "%9.2f" % thread_cache_hitrate
        else:
            thread_cache_hitrate = 0
        if string.atof(func.get_item(mysql_status, 'Key_read_requests')) <> 0:
            key_buffer_read_rate = 1 - string.atof(func.get_item(mysql_status, 'Key_reads')) / string.atof(
                func.get_item(mysql_status, 'Key_read_requests'))
            key_buffer_read_rate = "%9.2f" % key_buffer_read_rate
        else:
            key_buffer_read_rate = 0
        if string.atof(func.get_item(mysql_status, 'Key_write_requests')) <> 0:
            key_buffer_write_rate = 1 - string.atof(func.get_item(mysql_status, 'Key_writes')) / string.atof(
                func.get_item(mysql_status, 'Key_write_requests'))
            key_buffer_write_rate = "%9.2f" % key_buffer_write_rate
        else:
            key_buffer_write_rate = 0

        if (string.atof(func.get_item(mysql_status, 'Key_blocks_used')) + string.atof(
                func.get_item(mysql_status, 'Key_blocks_unused'))) <> 0:
            key_blocks_used_rate = string.atof(func.get_item(mysql_status, 'Key_blocks_used')) / (
                        string.atof(func.get_item(mysql_status, 'Key_blocks_used')) + string.atof(
                    func.get_item(mysql_status, 'Key_blocks_unused')))
            key_blocks_used_rate = "%9.2f" % key_blocks_used_rate
        else:
            key_blocks_used_rate = 0
        if (string.atof(func.get_item(mysql_status, 'Created_tmp_disk_tables')) + string.atof(
                func.get_item(mysql_status, 'Created_tmp_tables'))) <> 0:
            created_tmp_disk_tables_rate = string.atof(func.get_item(mysql_status, 'Created_tmp_disk_tables')) / (
                        string.atof(func.get_item(mysql_status, 'Created_tmp_disk_tables')) + string.atof(
                    func.get_item(mysql_status, 'Created_tmp_tables')))
            created_tmp_disk_tables_rate = "%9.2f" % created_tmp_disk_tables_rate
        else:
            created_tmp_disk_tables_rate = 0
        if string.atof(max_connections) <> 0:
            connections_usage_rate = string.atof(threads_connected) / string.atof(max_connections)
            connections_usage_rate = "%9.2f" % connections_usage_rate
        else:
            connections_usage_rate = 0
        if string.atof(open_files_limit) <> 0:
            open_files_usage_rate = string.atof(open_files) / string.atof(open_files_limit)
            open_files_usage_rate = "%9.2f" % open_files_usage_rate
        else:
            open_files_usage_rate = 0
        if string.atof(table_open_cache) <> 0:
            open_tables_usage_rate = string.atof(open_tables) / string.atof(table_open_cache)
            open_tables_usage_rate = "%9.2f" % open_tables_usage_rate
        else:
            open_tables_usage_rate = 0

        # repl
        slave_status = cur.execute('show slave status;')
        if slave_status <> 0:
            role = 'slave'
            role_new = 's'
        else:
            role = 'master'
            role_new = 'm'
        ############################# INSERT INTO SERVER ##################################################
        sql = "insert into mysql_status(server_id,host,port,tags,connect,role,uptime,version,max_connections,max_connect_errors,open_files_limit,table_open_cache,max_tmp_tables,max_heap_table_size,max_allowed_packet,open_files,open_tables,threads_connected,threads_running,threads_waits,threads_created,threads_cached,connections,aborted_clients,aborted_connects,connections_persecond,bytes_received_persecond,bytes_sent_persecond,com_select_persecond,com_insert_persecond,com_update_persecond,com_delete_persecond,com_commit_persecond,com_rollback_persecond,questions_persecond,queries_persecond,transaction_persecond,created_tmp_tables_persecond,created_tmp_disk_tables_persecond,created_tmp_files_persecond,table_locks_immediate_persecond,table_locks_waited_persecond,key_buffer_size,sort_buffer_size,join_buffer_size,key_blocks_not_flushed,key_blocks_unused,key_blocks_used,key_read_requests_persecond,key_reads_persecond,key_write_requests_persecond,key_writes_persecond,innodb_version,innodb_buffer_pool_instances,innodb_buffer_pool_size,innodb_doublewrite,innodb_file_per_table,innodb_flush_log_at_trx_commit,innodb_flush_method,innodb_force_recovery,innodb_io_capacity,innodb_read_io_threads,innodb_write_io_threads,innodb_buffer_pool_pages_total,innodb_buffer_pool_pages_data,innodb_buffer_pool_pages_dirty,innodb_buffer_pool_pages_flushed,innodb_buffer_pool_pages_free,innodb_buffer_pool_pages_misc,innodb_page_size,innodb_pages_created,innodb_pages_read,innodb_pages_written,innodb_row_lock_current_waits,innodb_buffer_pool_pages_flushed_persecond,innodb_buffer_pool_read_requests_persecond,innodb_buffer_pool_reads_persecond,innodb_buffer_pool_write_requests_persecond,innodb_rows_read_persecond,innodb_rows_inserted_persecond,innodb_rows_updated_persecond,innodb_rows_deleted_persecond,query_cache_hitrate,thread_cache_hitrate,key_buffer_read_rate,key_buffer_write_rate,key_blocks_used_rate,created_tmp_disk_tables_rate,connections_usage_rate,open_files_usage_rate,open_tables_usage_rate) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        param = (server_id, host, port, tags, connect, role, uptime, version, max_connections, max_connect_errors,
                 open_files_limit, table_open_cache, max_tmp_tables, max_heap_table_size, max_allowed_packet,
                 open_files, open_tables, threads_connected, threads_running, threads_waits, threads_created,
                 threads_cached, connections, aborted_clients, aborted_connects, connections_persecond,
                 bytes_received_persecond, bytes_sent_persecond, com_select_persecond, com_insert_persecond,
                 com_update_persecond, com_delete_persecond, com_commit_persecond, com_rollback_persecond,
                 questions_persecond, queries_persecond, transaction_persecond, created_tmp_tables_persecond,
                 created_tmp_disk_tables_persecond, created_tmp_files_persecond, table_locks_immediate_persecond,
                 table_locks_waited_persecond, key_buffer_size, sort_buffer_size, join_buffer_size,
                 key_blocks_not_flushed, key_blocks_unused, key_blocks_used, key_read_requests_persecond,
                 key_reads_persecond, key_write_requests_persecond, key_writes_persecond, innodb_version,
                 innodb_buffer_pool_instances, innodb_buffer_pool_size, innodb_doublewrite, innodb_file_per_table,
                 innodb_flush_log_at_trx_commit, innodb_flush_method, innodb_force_recovery, innodb_io_capacity,
                 innodb_read_io_threads, innodb_write_io_threads, innodb_buffer_pool_pages_total,
                 innodb_buffer_pool_pages_data, innodb_buffer_pool_pages_dirty, innodb_buffer_pool_pages_flushed,
                 innodb_buffer_pool_pages_free, innodb_buffer_pool_pages_misc, innodb_page_size, innodb_pages_created,
                 innodb_pages_read, innodb_pages_written, innodb_row_lock_current_waits,
                 innodb_buffer_pool_pages_flushed_persecond, innodb_buffer_pool_read_requests_persecond,
                 innodb_buffer_pool_reads_persecond, innodb_buffer_pool_write_requests_persecond,
                 innodb_rows_read_persecond, innodb_rows_inserted_persecond, innodb_rows_updated_persecond,
                 innodb_rows_deleted_persecond, query_cache_hitrate, thread_cache_hitrate, key_buffer_read_rate,
                 key_buffer_write_rate, key_blocks_used_rate, created_tmp_disk_tables_rate, connections_usage_rate,
                 open_files_usage_rate, open_tables_usage_rate)
        func.mysql_exec(sql, param)
        func.update_db_status_init(role_new, version, host, port, tags)
        # check mysql process
        processlist = cur.execute(
            "select * from information_schema.processlist where DB !='information_schema' and command !='Sleep';")
        if processlist:
            for line in cur.fetchall():
                sql = "insert into mysql_processlist(server_id,host,port,tags,pid,p_user,p_host,p_db,command,time,status,info) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                param = (
                server_id, host, port, tags, line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7])
                func.mysql_exec(sql, param)
        # check mysql connected
        connected = cur.execute(
            "select SUBSTRING_INDEX(host,':',1) as connect_server, user connect_user,db connect_db, count(SUBSTRING_INDEX(host,':',1)) as connect_count  from information_schema.processlist where db is not null and db!='information_schema' and db !='performance_schema' group by connect_server ;");
        if connected:
            for line in cur.fetchall():
                sql = "insert into mysql_connected(server_id,host,port,tags,connect_server,connect_user,connect_db,connect_count) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                param = (server_id, host, port, tags, line[0], line[1], line[2], line[3])
                func.mysql_exec(sql, param)
        # check mysql replication
        master_thread = cur.execute(
            "select * from information_schema.processlist where COMMAND = 'Binlog Dump' or COMMAND = 'Binlog Dump GTID';")
        slave_status = cur.execute('show slave status;')
        datalist = []
        if master_thread >= 1:
            datalist.append(int(1))
            if slave_status <> 0:
                datalist.append(int(1))
            else:
                datalist.append(int(0))
        else:
            datalist.append(int(0))
            if slave_status <> 0:
                datalist.append(int(1))
            else:
                datalist.append(int(0))
        if slave_status <> 0:
            gtid_mode = cur.execute(
                "select * from information_schema.global_variables where variable_name='gtid_mode';")
            result = cur.fetchone()
            if result:
                gtid_mode = result[1]
            else:
                gtid_mode = 'OFF'
            datalist.append(gtid_mode)
            read_only = cur.execute(
                "select * from information_schema.global_variables where variable_name='read_only';")
            result = cur.fetchone()
            datalist.append(result[1])
            slave_info = cur.execute('show slave status;')
            result = cur.fetchone()
            master_server = result[1]
            master_port = result[3]
            slave_io_run = result[10]
            slave_sql_run = result[11]
            delay = result[32]
            current_binlog_file = result[9]
            current_binlog_pos = result[21]
            master_binlog_file = result[5]
            master_binlog_pos = result[6]
            datalist.append(master_server)
            datalist.append(master_port)
            datalist.append(slave_io_run)
            datalist.append(slave_sql_run)
            datalist.append(delay)
            datalist.append(current_binlog_file)
            datalist.append(current_binlog_pos)
            datalist.append(master_binlog_file)
            datalist.append(master_binlog_pos)
            datalist.append(0)
        elif master_thread >= 1:
            gtid_mode = cur.execute(
                "select * from information_schema.global_variables where variable_name='gtid_mode';")
            result = cur.fetchone()
            if result:
                gtid_mode = result[1]
            else:
                gtid_mode = 'OFF'
            datalist.append(gtid_mode)
            read_only = cur.execute(
                "select * from information_schema.global_variables where variable_name='read_only';")
            result = cur.fetchone()
            datalist.append(result[1])
            datalist.append('---')
            datalist.append('---')
            datalist.append('---')
            datalist.append('---')
            datalist.append('---')
            datalist.append('---')
            datalist.append('---')
            master = cur.execute('show master status;')
            master_result = cur.fetchone()
            datalist.append(master_result[0])
            datalist.append(master_result[1])
            binlog_file = cur.execute('show master logs;')
            binlogs = 0
            if binlog_file:
                for row in cur.fetchall():
                    binlogs = binlogs + row[1]
                datalist.append(binlogs)
        else:
            datalist = []
        result = datalist
        if result:
            sql = "insert into mysql_replication(server_id,tags,host,port,is_master,is_slave,gtid_mode,read_only,master_server,master_port,slave_io_run,slave_sql_run,delay,current_binlog_file,current_binlog_pos,master_binlog_file,master_binlog_pos,master_binlog_space) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            param = (
            server_id, tags, host, port, result[0], result[1], result[2], result[3], result[4], result[5], result[6],
            result[7], result[8], result[9], result[10], result[11], result[12], result[13])
            func.mysql_exec(sql, param)
        cur.close()
        exit
    except MySQLdb.Error, e:
        logger_msg = "check mysql %s:%s failure: %d %s" % (host, port, e.args[0], e.args[1])
        logger.warning(logger_msg)
        logger_msg = "check mysql %s:%s failure: sleep 3 seconds and check again." % (host, port)
        logger.warning(logger_msg)
        time.sleep(3)
        try:
            conn = MySQLdb.connect(host=host, user=username, passwd=password, port=int(port), connect_timeout=3,
                                   charset='utf8')
            cur = conn.cursor()
            conn.select_db('information_schema')
        except MySQLdb.Error, e:
            logger_msg = "check mysql second %s:%s failure: %d %s" % (host, port, e.args[0], e.args[1])
            logger.warning(logger_msg)
            connect = 0
            sql = "insert into mysql_status(server_id,host,port,tags,connect) values(%s,%s,%s,%s,%s)"
            param = (server_id, host, port, tags, connect)
            func.mysql_exec(sql, param)

    try:
        func.check_db_status(server_id, host, port, tags, 'mysql')
    except Exception, e:
        logger.error(e)
        sys.exit(1)


def main():
    func.mysql_exec(
        "insert into mysql_status_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from mysql_status",
        '')
    func.mysql_exec('delete from mysql_status;', '')
    func.mysql_exec(
        "insert into mysql_replication_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from mysql_replication",
        '')
    func.mysql_exec('delete from mysql_replication;', '')
    # get mysql servers list
    servers = func.mysql_query(
        'select id,host,port,username,password,tags from db_servers_mysql where is_delete=0 and monitor=1;')
    logger.info("check mysql controller started.")
    if servers:
        plist = []
        for row in servers:
            server_id = row[0]
            host = row[1]
            port = row[2]
            username = row[3]
            password = row[4]
            tags = row[5]
            # thread.start_new_thread(check_mysql, (host,port,user,passwd,server_id,application_id))
            # time.sleep(1)
            p = Process(target=check_mysql, args=(host, port, username, password, server_id, tags))
            plist.append(p)
        for p in plist:
            p.start()
        time.sleep(10)
        for p in plist:
            p.terminate()
        for p in plist:
            p.join()

    else:
        logger.warning("check mysql: not found any servers")
    logger.info("check mysql controller finished.")


if __name__ == '__main__':
    main()
    import functions as func
from multiprocessing import Process;


def check_mysql_bigtable(host, port, username, password, server_id, tags, bigtable_size):
    try:
        conn = MySQLdb.connect(host=host, user=username, passwd=password, port=int(port), connect_timeout=2,
                               charset='utf8')
        curs = conn.cursor()
        conn.select_db('information_schema')
        try:
            bigtable = curs.execute(
                "SELECT table_schema as 'DB',table_name as 'TABLE',CONCAT(ROUND(( data_length + index_length ) / ( 1024 * 1024 ), 2), '') 'TOTAL' , table_comment as COMMENT FROM information_schema.TABLES ORDER BY data_length + index_length DESC ;");
            if bigtable:
                for row in curs.fetchall():
                    datalist = []
                    for r in row:
                        datalist.append(r)
                    result = datalist
                    if result:
                        table_size = float(string.atof(result[2]))
                        if table_size >= int(bigtable_size):
                            sql = "insert into mysql_bigtable(server_id,host,port,tags,db_name,table_name,table_size,table_comment) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                            param = (server_id, host, port, tags, result[0], result[1], result[2], result[3])
                            func.mysql_exec(sql, param)
        except:
            pass
        finally:
            curs.close()
            conn.close()
            sys.exit(1)
    except MySQLdb.Error, e:
        pass
        print
        "Mysql Error %d: %s" % (e.args[0], e.args[1])


def main():
    func.mysql_exec(
        "insert into mysql_bigtable_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),8) from mysql_bigtable",
        '')
    func.mysql_exec('delete from mysql_bigtable;', '')
    # get mysql servers list
    servers = func.mysql_query(
        'select id,host,port,username,password,tags,bigtable_size from db_servers_mysql where is_delete=0 and monitor=1 and bigtable_monitor=1;')
    if servers:
        print("%s: check mysql bigtable controller started." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),));
        plist = []
        for row in servers:
            server_id = row[0]
            host = row[1]
            port = row[2]
            username = row[3]
            password = row[4]
            tags = row[5]
            bigtable_size = row[6]
            p = Process(target=check_mysql_bigtable,
                        args=(host, port, username, password, server_id, tags, bigtable_size))
            plist.append(p)
        for p in plist:
            p.start()
        time.sleep(15)
        for p in plist:
            p.terminate()
        for p in plist:
            p.join()
        print("%s: check mysql bigtable controller finished." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),))


def check_oracle(host, port, dsn, username, password, server_id, tags):
    url = host + ':' + port + '/' + dsn
    try:
        conn = cx_Oracle.connect(username, password, url)  # connection
    except Exception, e:
        logger_msg = "check oracle %s : %s" % (url, str(e).strip('\n'))
        logger.warning(logger_msg)
        try:
            connect = 0
            sql = "insert into oracle_status(server_id,host,port,tags,connect) values(%s,%s,%s,%s,%s)"
            param = (server_id, host, port, tags, connect)
            func.mysql_exec(sql, param)
        except Exception, e:
            logger.error(str(e).strip('\n'))
            sys.exit(1)
        finally:
            sys.exit(1)
    finally:
        func.check_db_status(server_id, host, port, tags, 'oracle')
    try:
        # get info by v$instance
        connect = 1
        instance_name = oracle.get_instance(conn, 'instance_name')
        instance_role = oracle.get_instance(conn, 'instance_role')
        database_role = oracle.get_database(conn, 'database_role')
        open_mode = oracle.get_database(conn, 'open_mode')
        protection_mode = oracle.get_database(conn, 'protection_mode')
        if database_role == 'PRIMARY':
            database_role_new = 'm'
            dg_stats = '-1'
            dg_delay = '-1'
        else:
            database_role_new = 's'
            # dg_stats = oracle.get_stats(conn)
            # dg_delay = oracle.get_delay(conn)
            dg_stats = '1'
            dg_delay = '1'
        instance_status = oracle.get_instance(conn, 'status')
        startup_time = oracle.get_instance(conn, 'startup_time')
        # print startup_time
        # startup_time = time.strftime('%Y-%m-%d %H:%M:%S',startup_time)
        # localtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        # uptime =  (localtime - startup_time).seconds
        # print uptime
        uptime = oracle.get_instance(conn, 'startup_time')
        version = oracle.get_instance(conn, 'version')
        instance_status = oracle.get_instance(conn, 'status')
        database_status = oracle.get_instance(conn, 'database_status')
        host_name = oracle.get_instance(conn, 'host_name')
        archiver = oracle.get_instance(conn, 'archiver')
        # get info by sql count
        session_total = oracle.get_sessions(conn)
        session_actives = oracle.get_actives(conn)
        session_waits = oracle.get_waits(conn)
        # get info by v$parameters
        parameters = oracle.get_parameters(conn)
        processes = parameters['processes']

        ##get info by v$parameters
        sysstat_0 = oracle.get_sysstat(conn)
        time.sleep(1)
        sysstat_1 = oracle.get_sysstat(conn)
        session_logical_reads_persecond = sysstat_1['session logical reads'] - sysstat_0['session logical reads']
        physical_reads_persecond = sysstat_1['physical reads'] - sysstat_0['physical reads']
        physical_writes_persecond = sysstat_1['physical writes'] - sysstat_0['physical writes']
        physical_read_io_requests_persecond = sysstat_1['physical write total IO requests'] - sysstat_0[
            'physical write total IO requests']
        physical_write_io_requests_persecond = sysstat_1['physical read IO requests'] - sysstat_0[
            'physical read IO requests']
        db_block_changes_persecond = sysstat_1['db block changes'] - sysstat_0['db block changes']
        os_cpu_wait_time = sysstat_0['OS CPU Qt wait time']
        logons_persecond = sysstat_1['logons cumulative'] - sysstat_0['logons cumulative']
        logons_current = sysstat_0['logons current']
        opened_cursors_persecond = sysstat_1['opened cursors cumulative'] - sysstat_0['opened cursors cumulative']
        opened_cursors_current = sysstat_0['opened cursors current']
        user_commits_persecond = sysstat_1['user commits'] - sysstat_0['user commits']
        user_rollbacks_persecond = sysstat_1['user rollbacks'] - sysstat_0['user rollbacks']
        user_calls_persecond = sysstat_1['user calls'] - sysstat_0['user calls']
        db_block_gets_persecond = sysstat_1['db block gets'] - sysstat_0['db block gets']
        # print session_logical_reads_persecond
        ##################### insert data to mysql server#############################
        sql = "insert into ond,os_cpu_wait_time,logons_persecond,logons_current,opened_cursors_persecond,opened_cursors_current,user_commits_persecond,user_rollbacks_persecond,user_calls_persecond,db_block_gets_persecond) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        param = (
        server_id, host, port, tags, connect, instance_name, instance_role, instance_status, database_role, open_mode,
        protection_mode, host_name, database_status, startup_time, uptime, version, archiver, session_total,
        session_actives, session_waits, dg_stats, dg_delay, processes, session_logical_reads_persecond,
        physical_reads_persecond, physical_writes_persecond, physical_read_io_requests_persecond,
        physical_write_io_requests_persecond, db_block_changes_persecond, os_cpu_wait_time, logons_persecond,
        logons_current, opened_cursors_persecond, opened_cursors_current, user_commits_persecond,
        user_rollbacks_persecond, user_calls_persecond, db_block_gets_persecond)
        func.mysql_exec(sql, param)
        func.update_db_status_init(database_role_new, version, host, port, tags)
        # check tablespace
        tablespace = oracle.get_tablespace(conn)
        if tablespace:
            for line in tablespace:
                sql = "insert into oracle_tablespace(server_id,host,port,tags,tablespace_name,total_size,used_size,avail_size,used_rate) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                param = (server_id, host, port, tags, line[0], line[1], line[2], line[3], line[4])
                func.mysql_exec(sql, param)

    except Exception, e:
        logger.error(e)
        sys.exit(1)
    finally:
        conn.close()


def main():
    func.mysql_exec(
        "insert into oracle_status_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from oracle_status;",
        '')
    func.mysql_exec('delete from oracle_status;', '')
    func.mysql_exec(
        "insert into oracle_tablespace_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from oracle_tablespace;",
        '')
    func.mysql_exec('delete from oracle_tablespace;', '')
    # get oracle servers list
    servers = func.mysql_query(
        "select id,host,port,dsn,username,password,tags from db_servers_oracle where is_delete=0 and monitor=1;")
    logger.info("check oracle controller start.")
    if servers:
        plist = []
        for row in servers:
            server_id = row[0]
            host = row[1]
            port = row[2]
            dsn = row[3]
            username = row[4]
            password = row[5]
            tags = row[6]
            p = Process(target=check_oracle, args=(host, port, dsn, username, password, server_id, tags))
            plist.append(p)
            p.start()
        # time.sleep(10)
        # for p in plist:
        #    p.terminate()
        for p in plist:
            p.join()
    else:
        logger.warning("check oracle: not found any servers")
    logger.info("check oracle controller finished.")


cmdline.execute("scrapy crawl pornHubSpider".split())
from multiprocessing import Process;

dbhost = func.get_config('monitor_server', 'host')
dbport = func.get_config('monitor_server', 'port')
dbuser = func.get_config('monitor_server', 'user')
dbpasswd = func.get_config('monitor_server', 'passwd')
dbname = func.get_config('monitor_server', 'dbname')


def check_os(ip, community, filter_os_disk, tags):
    func.mysql_exec(
        "insert into os_status_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from os_status where ip='%s';" % (
            ip), '')
    func.mysql_exec(
        "insert into os_disk_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from os_disk where ip='%s';" % (
            ip), '')
    func.mysql_exec(
        "insert into os_diskio_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from os_diskio where ip='%s';" % (
            ip), '')
    func.mysql_exec(
        "insert into os_net_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from os_net where ip='%s';" % (
            ip), '')
    func.mysql_exec("delete from os_status where ip='%s'" % (ip), '')
    func.mysql_exec("delete from os_disk where ip='%s'" % (ip), '')
    func.mysql_exec("delete from os_diskio where ip='%s'" % (ip), '')
    func.mysql_exec("delete from os_net where ip='%s'" % (ip), '')
    command = "sh check_os.sh"
    try:
        os.system("%s %s %s %s %s %s %s %s %s %s" % (
        command, ip, dbhost, dbport, dbuser, dbpasswd, dbname, community, filter_os_disk, tags))
        logger.info("%s:%s statspack complete." % (dbhost, dbport))

    except Exception, e:
        print
        e
        logger.info("%s:%s statspack error: %s" % (dbhost, dbport, e))
        sys.exit(1)
    finally:
        sys.exit(1)


def main():
    # get os servers list
    servers = func.mysql_query(
        "select host,community,filter_os_disk,tags from db_servers_os where is_delete=0 and monitor=1;")

    logger.info("check os controller started.")
    if servers:
        plist = []
        for row in servers:
            host = row[0]
            community = row[1]
            filter_os_disk = row[2]
            tags = row[3]
            if host <> '':
                thread.start_new_thread(check_os, (host, community, filter_os_disk, tags))
                time.sleep(1)
                # p = Process(target = check_os, args=(host,filter_os_disk))
                # plist.append(p)
                # p.start()
        # for p in plist:
        #    p.join()
    else:
        logger.warning("check os: not found any servers")
    logger.info("check os controller finished.")


logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("Python")
path = './include'
sys.path.insert(0, path)
import functions as func
from multiprocessing import Process;


def check_value(info, key):
    try:
        key_tmp = key
        key_tmp = info['%s' % (key)]
        if key_tmp == key:
            key_tmp = '-1'
            logger_msg = "check redis %s:%s : %s is not supported for this version" % (host, port, key)
            logger.warning(logger_msg)
    except:
        key_tmp = '-1'
        logger_msg = "check redis %s:%s : %s is not supported for this version" % (host, port, key)
        logger.waring(logger_msg)
    finally:
        return key_tmp


def check_redis(host, port, passwd, server_id, tags):
    try:
        r = redis.StrictRedis(host=host, port=int(port), password=passwd, db=0, socket_timeout=3, charset='utf-8')
        info = r.info()
        time.sleep(1)
        info_2 = r.info()
        # Server
        redis_version = info['redis_version']
        redis_git_sha1 = info['redis_git_sha1']
        redis_git_dirty = info['redis_git_dirty']
        arch_bits = info['arch_bits']
        multiplexing_api = info['multiplexing_api']
        gcc_version = info['gcc_version']
        process_id = info['process_id']
        uptime_in_seconds = info['uptime_in_seconds']
        uptime_in_days = info['uptime_in_days']
        lru_clock = info['lru_clock']
        os = check_value(info, 'os')
        redis_mode = check_value(info, 'redis_mode')
        hz = check_value(info, 'hz')
        run_id = check_value(info, 'run_id')
        tcp_port = check_value(info, 'tcp_port')
        # Clients
        connected_clients = info['connected_clients']
        client_longest_output_list = info['client_longest_output_list']
        client_biggest_input_buf = info['client_biggest_input_buf']
        blocked_clients = info['blocked_clients']
        # Memory
        used_memory = info['used_memory']
        used_memory_human = info['used_memory_human']
        used_memory_rss = info['used_memory_rss']
        used_memory_peak = info['used_memory_peak']
        used_memory_peak_human = info['used_memory_peak_human']
        used_memory_lua = check_value(info, 'used_memory_lua')
        mem_fragmentation_ratio = info['mem_fragmentation_ratio']
        mem_allocator = info['mem_allocator']
        # Persistence
        loading = info['loading']
        rdb_changes_since_last_save = check_value(info, 'rdb_changes_since_last_save')
        rdb_bgsave_in_progress = check_value(info, 'rdb_bgsave_in_progress')
        rdb_last_save_time = check_value(info, 'rdb_last_save_time')
        rdb_last_bgsave_status = check_value(info, 'rdb_last_bgsave_status')
        rdb_last_bgsave_time_sec = check_value(info, 'rdb_last_bgsave_time_sec')
        rdb_current_bgsave_time_sec = check_value(info, 'rdb_current_bgsave_time_sec')
        aof_enabled = check_value(info, 'aof_enabled')
        aof_rewrite_in_progress = check_value(info, 'aof_rewrite_in_progress')
        aof_rewrite_scheduled = check_value(info, 'aof_rewrite_scheduled')
        aof_last_rewrite_time_sec = check_value(info, 'aof_last_rewrite_time_sec')
        aof_current_rewrite_time_sec = check_value(info, 'aof_current_rewrite_time_sec')
        aof_last_bgrewrite_status = check_value(info, 'aof_last_bgrewrite_status')
        # Stats
        total_connections_received = check_value(info, 'total_connections_received')
        total_commands_processed = check_value(info, 'total_commands_processed')
        current_commands_processed = int(info_2['total_commands_processed'] - info['total_commands_processed'])
        instantaneous_ops_per_sec = check_value(info, 'instantaneous_ops_per_sec')
        rejected_connections = check_value(info, 'rejected_connections')
        expired_keys = info['expired_keys']
        evicted_keys = info['evicted_keys']
        keyspace_hits = info['keyspace_hits']
        keyspace_misses = info['keyspace_misses']
        pubsub_channels = info['pubsub_channels']
        pubsub_patterns = info['pubsub_patterns']
        latest_fork_usec = info['latest_fork_usec']
        # Replication
        role = info['role']
        connected_slaves = info['connected_slaves']
        # CPU
        used_cpu_sys = info['used_cpu_sys']
        used_cpu_user = info['used_cpu_user']
        used_cpu_sys_children = info['used_cpu_sys_children']
        used_cpu_user_children = info['used_cpu_user_children']
        # replication
        if role == 'slave':
            # print info
            master_host = info['master_host']
            master_port = info['master_port']
            master_link_status = info['master_link_status']
            master_last_io_seconds_ago = info['master_last_io_seconds_ago']
            master_sync_in_progress = info['master_sync_in_progress']
            # slave_repl_offset = info['slave_repl_offset']
            slave_priority = check_value(info, 'slave_priority')
            slave_read_only = check_value(info, 'slave_read_only')
            master_server_id = func.mysql_query(
                "SELECT id FROM db_servers_redis WHERE host='%s' AND port='%s' limit 1;" % (master_host, master_port))
            master_server_id = master_server_id[0][0]
            role_new = 's'
        else:
            master_host = '-1'
            master_port = '-1'
            slave_priority = '-1'
            slave_read_only = '-1'
            master_server_id = '-1'
            role_new = 'm'
        # add redis_status
        connect = 1
        sql = "insert into
        func.mysql_exec(sql, param)
        # add redis_replication