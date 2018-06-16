


import csv
import datetime


#global variables
log_list = open("log.csv", "r")
session_logging = csv.reader(log_list)

inactivity_period = open("inactivity_period.txt", "r")
inactivity_period_int = inactivity_period.read()



starting_timestamp = None
starting_timestamp = None
session_list = []

sessionization_output = open("sessionization.txt", "w")
    
def session_logging_function(log_list, inactivity_period):
    """main function that creates index, dictionary and then calculates time, and tracks sessions"""
    starting_timestamp = None
    session_list = []
    for n in session_logging:
        if session_logging.line_num == 1:
            session_tracker = n
            ip_address_index = session_tracker.index("ip")
            datestamp_index = session_tracker.index("date")
            timestamp_index = session_tracker.index("time")
            cik_index = session_tracker.index("cik")
            accession_index = session_tracker.index("accession")
            extention_index = session_tracker.index("extention")
        else:
            if starting_timestamp == None:
                sessionization_dic = {}
                sessionization_dic["elapsed_time"] = None
                sessionization_dic["ip"] = n[ip_address_index]
                sessionization_dic["starting_datestamp"] = n[datestamp_index]
                sessionization_dic["starting_timestamp"] = n[timestamp_index]
                sessionization_dic["ending_datestamp"] = n[datestamp_index]
                sessionization_dic["ending_timestamp"] = n[timestamp_index]
                sessionization_dic["counter"] = 1
                date_and_time_format = n[datestamp_index] + " " + n[timestamp_index]
                starting_timestamp = date_and_time_format
                session_list.append(sessionization_dic)
            else:
                date_and_time = n[datestamp_index] + " " + n[timestamp_index]
                is_ip_address_solo = False
                for i in range(len(session_list)):  
                    record = session_list[i]
                    
                    if record["ip"] == n[ip_address_index]:
                        is_ip_address_solo = True
                        d_t_format_1 = convert_time(record["ending_datestamp"], record["ending_timestamp"])
                        d_t_format_2 = convert_time(n[datestamp_index], n[timestamp_index])
                        if (d_t_format_2 - d_t_format_1).total_seconds() <= float(inactivity_period_int):
                            session_list[i]["ending_datestamp"] = n[datestamp_index]
                            session_list[i]["ending_timestamp"] = n[timestamp_index]
                            session_list[i]["counter"] += 1
                        else:
                            sessionization_dic = {}
                            sessionization_dic["elapsed_time"] = None
                            sessionization_dic["ip"] = n[ip_address_index]
                            sessionization_dic["starting_datestamp"] = n[datestamp_index]
                            sessionization_dic["starting_timestamp"] = n[timestamp_index]
                            sessionization_dic["ending_datestamp"] = n[datestamp_index]
                            sessionization_dic["ending_timestamp"] = n[timestamp_index]
                            sessionization_dic["counter"] = 1
                            session_list.append(sessionization_dic)
                            d_t_format_2 = convert_time(record["starting_datestamp"], record["starting_timestamp"])
                            record["elapsed_time"] = timedelta(d_t_format_2, d_t_format_1)
                            d_and_t_output = record["ip"] + "," + record["starting_datestamp"] + " " + record["starting_timestamp"] + "," + record["ending_datestamp"] + " " + record["ending_timestamp"] + "," + record["elapsed_time"] + "," + str(record["counter"])
                            create_output(d_and_t_output)
                            del session_list[i]
                          
                if not is_ip_address_solo:
                    sessionization_dic = {}
                    sessionization_dic["elapsed_time"] = None
                    sessionization_dic["ip"] = n[ip_address_index]
                    sessionization_dic["starting_datestamp"] = n[datestamp_index]
                    sessionization_dic["starting_timestamp"] = n[timestamp_index]
                    sessionization_dic["ending_datestamp"] = n[datestamp_index]
                    sessionization_dic["ending_timestamp"] = n[timestamp_index]
                    sessionization_dic["counter"] = 1
                    session_list.append(sessionization_dic)
                if starting_timestamp != date_and_time_format:
                    sessions = []
                    session_list_tmp = []
                    starting_timestamp = date_and_time_format
                    for i in range(len(session_list)):
                        record = session_list[i]
                        ending_datestamp_time = convert_time(record["ending_datestamp"], record["ending_timestamp"])
                        starting_datestamp_time = datetime.datetime.strptime(starting_timestamp, "%Y-%m-%d %H:%M:%S")
                        if timedelta(starting_datestamp_time, ending_datestamp_time) <= (inactivity_period_int):
                            sessions.append(i)
                        else:
                            start_date_time = convert_time(record["starting_datestamp"], record["starting_timestamp"])
                            end_date_time = convert_time(record["ending_datestamp"], record["ending_timestamp"])
                            record["elapsed_time"] = timedelta(ending_datestamp_time, starting_datestamp_time)
                            d_and_t_output = record["ip"] + "," + record["starting_datestamp"] + " " + record["starting_timestamp"] + "," + record["ending_datestamp"] + " " + record["ending_timestamp"] + "," + record["elapsed_time"] + "," + str(record["counter"]) + "\n"
                            create_output(d_and_t_output)
                    for i in range(len(sessions)):
                        session_list_tmp.append(session_list[sessions[i]])
                    session_list = session_list_tmp
                   

    for i in range(len(session_list)):
        record = session_list[i]
        start_date_time = convert_time(record["starting_datestamp"], record["starting_timestamp"])
        start_date_time = record["starting_datestamp"] + " " + record["starting_timestamp"]
        end_date_time = record["ending_datestamp"] + " " + record["ending_timestamp"]
        starting_datestamp_time = datetime.datetime.strptime(start_date_time, "%Y-%m-%d %H:%M:%S")
        ending_datestamp_time = datetime.datetime.strptime(end_date_time, "%Y-%m-%d %H:%M:%S")
        record["elapsed_time"] = timedelta(ending_datestamp_time, starting_datestamp_time)
        d_and_t_output = record["ip"] + "," + record["starting_datestamp"] + " " + record["starting_timestamp"] + "," + record["ending_datestamp"] + " " + record["ending_timestamp"] + "," + record["elapsed_time"] + "," + str(record["counter"]) + "\n"
        sessionization_output.write(d_and_t_output)


def convert_time(date, time):
    date_and_time_formated = ""
    d_and_t = (date + " " + time)
    date_and_time_formated = datetime.datetime.strptime(d_and_t, "%Y-%m-%d %H:%M:%S")
    return date_and_time_formated


def timedelta(time1, time2):
    """calculates elapsed time"""
    time_elapsed = str((time1 - time2).total_seconds() + 1)
    return time_elapsed

def create_output(output):
    """creates output"""
    sessionization_output.write(output)
    sessionization_output.write("\n")
    

session_logging_function(log_list, inactivity_period)
