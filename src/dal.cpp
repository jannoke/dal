//============================================================================
// Name        : datapanel apache logger (dal)
// Author      : Taavi Sannik <taavi@kood.ee>
// Version     : 1.2
// Copyright   : DataCode OY 2008
// Description : Datapanel apache logger and bandwidth counter
//============================================================================


#include "dal.h"

namespace dal {

void wait_input() {
	while(std::cin.eof() && !std::cin.fail() && std::cin.good()) {
		usleep(500000);
	}
}

bool file_exists(std::string *filename) {
	struct stat file_stat;

	return stat(filename->c_str(), &file_stat) == 0;
}

void writelog(int level, const char * format, ...) {
	va_list arg;
	char * type;
	char frmt[256];
	char line[256];

	switch(level) {
		case LOG_DEBUG:
			type=(char *)LOG_DEBUG_LABEL; 	break;
		default:
			level = LOG_INFO;
		case LOG_INFO:
			type=(char *)LOG_INFO_LABEL; 	break;
		case LOG_WARNING:
			type=(char *)LOG_WARNING_LABEL; break;
		case LOG_ERROR:
			type=(char *)LOG_ERROR_LABEL; 	break;
		case LOG_FATAL:
			type=(char *)LOG_FATAL_LABEL; 	break;
	}

	va_start(arg, format);
	vsprintf(frmt, format, arg);
	va_end(arg);

	sprintf(line, LOG_FORMAT, type, frmt);

	if (level >= LOG_MIN_FILE_LEVEL && dal_loghandler != NULL && dal_loghandler->is_open()) {
		char date[28];
		time_t timestamp;
		struct tm* time_s;

		time(&timestamp);
		time_s = localtime(&timestamp);

		strftime(date, 28, "[%c] ", time_s);
		*dal_loghandler << date << line << std::endl;
	}

	if (level >= LOG_MIN_ERR_LEVEL) {
		std::cerr << line << std::endl;
	} else if (level >= LOG_MIN_OUT_LEVEL) {
		std::cout << line << std::endl;
	}
}

void db_error() {
	writelog(LOG_WARNING, "Database error: %s (%d)", sqlite3_errmsg(conn), sqlite3_errcode(conn));
}
void init_months() {
	months["Jan"] = 1;
	months["Feb"] = 2;
	months["Mar"] = 3;
	months["Apr"] = 4;
	months["May"] = 5;
	months["Jun"] = 6;
	months["Jul"] = 7;
	months["Aug"] = 8;
	months["Sep"] = 9;
	months["Oct"] = 10;
	months["Nov"] = 11;
	months["Dec"] = 12;
}

int monthtoint(string month) {
	if (months.count(month) == 0) return 0;
	return months[month];
}

int timetostr(time_t timestamp, bool short_date=false) {
	char date[10];
	struct tm* time_s;

	time_s = localtime(&timestamp);

	strftime(date, 10, short_date ? "%Y%m" : "%Y%m%d", time_s);

	return atoi(date);
}

int get_today(bool short_date=false) {
	time_t timestamp;

	time(&timestamp);

	return timetostr(timestamp, short_date);
}

int init_data() {
	sqlite3_stmt *stmt;
	struct domain_data d_data;
	int res;

	sqlite3_prepare(conn, "SELECT domain_id, domainname, basepath, uid, gid, type FROM `virtualhosts`",
							-1, &stmt, NULL);

	avail_domains.clear();

	while(true) {
		res = sqlite3_step(stmt);

		if (res == SQLITE_DONE) break;

		if (res != SQLITE_ROW) {
			db_error();
			return -1;
		}

		d_data.domain_id 	= sqlite3_column_int(stmt, 0);
		d_data.domainname.assign((const char *)sqlite3_column_text(stmt, 1));
		d_data.basepath.assign((const char *)sqlite3_column_text(stmt, 2));
		d_data.uid 			= sqlite3_column_int(stmt, 3);
		d_data.gid 			= sqlite3_column_int(stmt, 4);
		d_data.type			= sqlite3_column_int(stmt, 5);

		if (d_data.type & TYPE_SSL) d_data.domainname.append(SSL_SUFFIX);

		memset(&d_data.handles, 0, sizeof(domain_loghandles));

		avail_domains[d_data.domainname] = d_data;
	}

	sqlite3_finalize(stmt);

	d_data.domain_id 	= 0;
	d_data.domainname.assign("default");
	d_data.basepath.assign("");
	d_data.uid 			= 0;
	d_data.gid 			= 0;
	d_data.type			= 0;

	d_data.handles.handle 	 = new std::fstream();
	d_data.handles.handle->open(default_logfile.c_str(), std::fstream::out|std::fstream::app);

	d_data.handles.user_logfile = new std::string("");
	d_data.handles.logfile 		= new std::string(default_logfile);

	avail_domains[d_data.domainname] = d_data;

	if (!d_data.handles.handle->is_open()) {
		writelog(LOG_ERROR, "failed to open default log file %s", default_logfile.c_str());
		return -1;
	}

#ifdef DEBUG_DOMAIN_LIST
	std::map<std::string, struct domain_data>::iterator iter;
	for(iter=avail_domains.begin(); iter!=avail_domains.end(); ++iter) {
		writelog(LOG_DEBUG, "%s = %d", iter->first.c_str(), iter->second.domain_id);
	}
#endif

	global_logstream = new std::fstream();
	global_logstream->open(global_logfile.c_str(), std::fstream::out|std::fstream::app);

	return 0;
}

/**
 * Checks if we received a "fatal" signal during commit
 */
void check_death(bool from_rollback=false) {
	if (want_down) {
		if (from_rollback) {
			writelog(LOG_WARNING, "Last commit data lost (commit failed)");
		}
		else writelog(LOG_INFO, "Shutting down after successful commit");

		exit(1);
	}
	is_commiting = false;
}

void rollback_bw_commit() {
	sqlite3_stmt *stmt;
	int res;

	writelog(LOG_WARNING, "Doing rollback for last commit");

	if(sqlite3_prepare(conn, "ROLLBACK", -1, &stmt, NULL) != SQLITE_OK) {
		writelog(LOG_WARNING, "Preparing rollback failed");
		db_error();
		check_death(true);
		return;
	}

	res = sqlite3_step(stmt);

	sqlite3_finalize(stmt);

	if (res != SQLITE_DONE) {
		writelog(LOG_WARNING, "Rollback failed");
		db_error();
	}

	check_death(true);

	if (commit_requests >= commit_hard_limit) {
	    writelog(LOG_ERROR, "DAL has failed to update bandwidth data for last %d requests - dropping data to avoid memory leak",
				commit_hard_limit);
	    commit_buffer.clear();
	    commit_requests = 0;
	}
}

void commit_bandwidth() {
    std::map<int, struct bandwidth_data>::iterator iter;
    sqlite3_stmt *update_stmt, *insert_stmt, *stmt;
    struct bandwidth_data *data;
    int res;

    if (commit_requests == 0) return;

    writelog(LOG_DEBUG, "Starting to commit");
    is_commiting = true;

	if(sqlite3_prepare(conn, "BEGIN", -1, &stmt, NULL) != SQLITE_OK) {
		writelog(LOG_WARNING, "Failed to prepare commit begin");
		db_error();
		return;
	}

	res = sqlite3_step(stmt);

	sqlite3_finalize(stmt);

	if (res != SQLITE_DONE) {
		writelog(LOG_WARNING, "Commit begin failed");
		db_error();
		return;
	}

	/*	Prepare statements */
	if(sqlite3_prepare(conn, "INSERT INTO `bandwidth`(rcvd, sent, time, domain_id, date, count) VALUES(?,?,?,?,?,?)",
								-1, &insert_stmt, NULL) != SQLITE_OK) {
		writelog(LOG_WARNING, "Failed to prepare bandwidth insertion query");
		db_error();
		return;
	}

	if(sqlite3_prepare(conn, "UPDATE `bandwidth` SET rcvd = rcvd + ?, sent = sent + ?, time = time + ?, count = count + ? "
								"WHERE id = ?",
								-1, &update_stmt, NULL) != SQLITE_OK) {
		writelog(LOG_WARNING, "Failed to prepare bandwidth update query");
		db_error();
		return;
	}

	/* Start insertion/update */
    for( iter = commit_buffer.begin(); iter != commit_buffer.end(); ++iter ) {
    	data = &iter->second;

    	if (data->id == 0) {
    		stmt = insert_stmt;

    		sqlite3_bind_double(stmt, 1, data->rcvd);
    		sqlite3_bind_double(stmt, 2, data->sent);
    		sqlite3_bind_int(stmt, 3, data->time);
    		sqlite3_bind_int(stmt, 4, data->domain_id);
    		sqlite3_bind_int(stmt, 5, data->date);
    		sqlite3_bind_int(stmt, 6, data->count);
    	}
    	else {
    		stmt = update_stmt;

    		sqlite3_bind_double(stmt, 1, data->rcvd);
    		sqlite3_bind_double(stmt, 2, data->sent);
    		sqlite3_bind_int(stmt, 3, data->time);
    		sqlite3_bind_int(stmt, 4, data->count);
    		sqlite3_bind_int(stmt, 5, data->id);
    	}

    	res = sqlite3_step(stmt);

    	sqlite3_reset(stmt);

    	if (res != SQLITE_DONE) {
		if (sqlite3_errcode(conn) == 19) {
		    writelog(LOG_ERROR, "BUG: Tried to create duplicate data");
		    rollback_bw_commit();
		    commit_buffer.clear();
		    commit_requests = 0;
		    return;
		}
		
    		writelog(LOG_WARNING, "Bandwidth update/insert failed");
    		db_error();
    		rollback_bw_commit();
    		return;
    	}

    	if (data->id == 0) {
    		data->id = sqlite3_last_insert_rowid(conn);
    		bw_cache[data->domain_id][data->date] = data->id;
    		writelog(LOG_DEBUG, "created cache id %d for domain %d", data->id, data->domain_id);
    	}
    }

    sqlite3_finalize(update_stmt);
    sqlite3_finalize(insert_stmt);

	if(sqlite3_prepare(conn, "COMMIT", -1, &stmt, NULL) != SQLITE_OK) {
		writelog(LOG_WARNING, "Failed to prepare commit query");
		db_error();
		rollback_bw_commit();
		return;
	}

	res = sqlite3_step(stmt);

	sqlite3_finalize(stmt);

	if (res != SQLITE_DONE) {
		writelog(LOG_WARNING, "Commit failed");
		db_error();
		rollback_bw_commit();
		return;
	}

	/* Truncate commit cache and reset counter */
    commit_buffer.clear();
	commit_requests = 0;

	writelog(LOG_DEBUG, "Commit successful");

	check_death();
}

void update_bandwidth(struct bandwidth_data *data) {
	sqlite3_stmt *stmt = NULL;
	int res;

	if (data->domain_id == 0) {
		writelog(LOG_DEBUG, "not updating data for empty domain");
		return;
	}

	if (bw_cache.count(data->domain_id) == 0 && bw_cache[data->domain_id].count(data->date) == 0) {
		if(sqlite3_prepare(conn, "SELECT id FROM `bandwidth` WHERE domain_id = ? AND date = ? LIMIT 1",
								-1, &stmt, NULL) != SQLITE_OK) {
			db_error();
			return;
		}

		sqlite3_bind_int(stmt, 1, data->domain_id);
		sqlite3_bind_int(stmt, 2, data->date);

		res = sqlite3_step(stmt);

		if (res == SQLITE_ROW) {
			data->id = sqlite3_column_int(stmt, 0);
			bw_cache[data->domain_id][data->date] = data->id;
			writelog(LOG_DEBUG, "found cache id %d for domain %d", data->id, data->domain_id);
		}

		sqlite3_finalize(stmt);

		if (res != SQLITE_DONE && res != SQLITE_ROW) {
			db_error();
			return;
		}
	}
	else {
		writelog(LOG_DEBUG, "cache (%d) exists for domain %d (date %d)", data->id, data->domain_id, data->date);
		data->id = bw_cache[data->domain_id][data->date];
	}

	/* Try to add this to commit buffer or update it */
	if (commit_buffer.count(data->domain_id) == 0) {
		memcpy(&commit_buffer[data->domain_id], data, sizeof(bandwidth_data));
	}
	else {
		commit_buffer[data->domain_id].rcvd += data->rcvd;
		commit_buffer[data->domain_id].sent += data->sent;
		commit_buffer[data->domain_id].time += data->time;
		commit_buffer[data->domain_id].count++;
	}

	++commit_requests;

	if (++commit_requests >= commit_count) {
		commit_bandwidth();
	}
}

void write_apache_log(struct line_data *data, struct domain_data *d_data) {

	if (d_data->domainname != "default" &&
			(d_data->handles.handle == NULL || d_data->handles.user_handle == NULL)) {
		/*	Need to open log handles */
		std::stringstream *namestream;
		string logpath, user_logpath, cmd;

		/*	Build up logfile names */
		namestream = new std::stringstream();
		*namestream << log_root << "/" << d_data->gid << "/" << data->domain << "/"
					<< data->domain << "_" << log_type << "_"
					<< get_today(log_type == "error") << ".log";
		*namestream >> logpath;
		delete namestream;

		namestream = new std::stringstream();
		*namestream << d_data->basepath << "/" << d_data->gid << "/logs/" << data->domain << "_" << log_type << ".log";
		*namestream >> user_logpath;
		delete namestream;

		/*	Open log handles */
		d_data->handles.handle 	 = new std::fstream();
		d_data->handles.user_handle = new std::fstream();

		d_data->handles.handle->open(logpath.c_str(), std::fstream::out|std::fstream::app);
		d_data->handles.user_handle->open(user_logpath.c_str(), std::fstream::out|std::fstream::app);

		if (!d_data->handles.handle->is_open()) {
			writelog(LOG_WARNING, "failed to open log file %s", logpath.c_str());
			return;
		}

		if (!d_data->handles.user_handle->is_open()) {
			writelog(LOG_WARNING, "failed to open user log file %s", user_logpath.c_str());
			return;
		}
		else {
			chown(user_logpath.c_str(), d_data->uid, d_data->gid);
			chmod(user_logpath.c_str(), userlog_perm);
		}

		/* Store logfile names in case we ever need them */
		d_data->handles.user_logfile 	= new string(user_logpath);
		d_data->handles.logfile 		= new string(logpath);
	}

	/*	Write log lines */

	if (d_data->handles.handle->is_open() && d_data->handles.handle->good()) {
		*d_data->handles.handle << data->line << std::endl;
	}

	if (d_data->domainname != "default" && d_data->handles.user_handle->is_open() && d_data->handles.user_handle->good()) {
		*d_data->handles.user_handle << data->line << std::endl;
	}

	if (global_logstream->is_open() && global_logstream->good()) {
		*global_logstream << "[" << data->domain << "] " << data->line << std::endl;
	}
}

time_t parse_common_time(string *word) {
	struct tm tp;

	if (word->length() != 28) return -1;

	tp.tm_mday 	 = atoi(word->substr( 1,2).c_str());
	tp.tm_mon 	 = monthtoint(word->substr(4,3))-1;
	tp.tm_year 	 = atoi(word->substr( 8,4).c_str())-1900;
	tp.tm_hour 	 = atoi(word->substr(13,2).c_str());
	tp.tm_min 	 = atoi(word->substr(16,2).c_str());
	tp.tm_sec	 = atoi(word->substr(19,2).c_str());

	return mktime(&tp);
}

time_t parse_error_time(string *word) {
	struct tm tp;

	if (word->length() != 24) return -1;

	tp.tm_mon 	 = monthtoint(word->substr(4,3))-1;
	tp.tm_mday 	 = atoi(word->substr( 8,2).c_str());
	tp.tm_hour 	 = atoi(word->substr(11,2).c_str());
	tp.tm_min 	 = atoi(word->substr(14,2).c_str());
	tp.tm_sec	 = atoi(word->substr(17,2).c_str());
	tp.tm_year 	 = atoi(word->substr(20,4).c_str())-1900;

	return mktime(&tp);
}

int parse_common_line(string *line, struct line_data *data) {
	string word;
	int pos;
	unsigned int prev_pos, element;

	data->type 	= 0;
	prev_pos 	= 0;
	element 	= 0;
	pos 		= 0;
	while(element < 5) {
		pos = line->find(" ", prev_pos);

		if (pos < 0) return -1;

		word = line->substr(prev_pos, pos-prev_pos);

		switch(element) {
			case 0: if (word == "on") data->type   = TYPE_SSL; break;
			case 1: data->sent   = atoi(word.c_str()); break;
			case 2: data->rcvd   = atoi(word.c_str()); break;
			case 3: data->domain.assign(word.c_str()); break;
			case 4:
				pos = line->find(" ", pos+1);
				word = line->substr(prev_pos, pos-prev_pos);
				data->time = parse_common_time(&word);
				break;
		}

		element++;
		prev_pos = pos + 1;
		word.clear();
	}

	data->line = line->substr(prev_pos, line->length()-prev_pos);

	if (data->type & TYPE_SSL) {
		data->domain.append(SSL_SUFFIX);
	}

	return 0;
}

int parse_error_line(string *line, struct line_data *data) {
	std::stringstream linestream;
	string word;
	int pos;

	if (line->compare(0, 1, "[") == 0 && line->compare(25, 1, "]") == 0) {
		word = line->substr(1,24);

		data->time = parse_error_time(&word);

		pos = line->find("]", 28);
		if (pos < 0) return -1;

		data->domain.assign(line->substr(28, pos-28));
		data->line.assign(line->c_str());
		data->line.erase(27, data->domain.length()+2);

		data->sent	= 0;
		data->rcvd	= 0;
		return 0;
	}
	return -2;
}

void do_cleanup() {
	/*	First we close all open log handles	*/
	std::map<std::string, struct domain_data>::iterator iter;

	for(iter=avail_domains.begin(); iter!=avail_domains.end(); ++iter) {
		/*	Currently we never rotate the default log	*/
		if (iter->second.domainname == "default") continue;

		if (iter->second.handles.handle != NULL) {
			iter->second.handles.handle->close();

			delete iter->second.handles.handle;
			iter->second.handles.handle = NULL;
		}

		if (iter->second.handles.user_handle != NULL) {
			iter->second.handles.user_handle->close();

			delete iter->second.handles.user_handle;
			iter->second.handles.user_handle = NULL;

			/* We also need to remove user's logfile, because we need it's location
			 * No data should be lost, because logrotate uses the non-client location
			 * for creating archives and statistics */
			if (iter->second.handles.user_logfile != NULL && iter->second.handles.user_logfile->length() > 0) {
				if (unlink(iter->second.handles.user_logfile->c_str()) != 0) {
					writelog(LOG_WARNING, "failed to delete user logfile %s", iter->second.handles.user_logfile->c_str());
				}

				delete iter->second.handles.user_logfile;
				iter->second.handles.user_logfile = NULL;
			}
		}

		if (iter->second.handles.logfile != NULL) {
			delete iter->second.handles.logfile;
			iter->second.handles.logfile = NULL;
		}
	}

	if (global_logstream != NULL) {
	    global_logstream->close();
	    global_logstream->open(global_logfile.c_str(), std::fstream::out);
	}

	/* Clear bandwidth cache as we need to create new bandwidth structs */
	bw_cache.clear();

	commit_bandwidth();
}

void run() {
	std::string line;
	struct bandwidth_data bw_data;
	struct line_data data;
	unsigned int current_date = 0;
	int result = 0;
	int sleep_time = 0;
	int i = 0;
	struct domain_data *d_data;

	init_months();
	
	do {
	    result = init_data();
	    
	    if (result < 0) {
		i++;
		
		sleep_time = i * FAILURE_SLEEP_MULTIPLIER;
		
		if (sleep_time > FAILURE_SLEEP_MAX) {
		    sleep_time = FAILURE_SLEEP_MAX;
		}
		
		writelog(LOG_WARNING, "General failure detected - sleeping for %d seconds", sleep_time);
		sleep(sleep_time);
	    }
	}
	while(result < 0);

	writelog(LOG_INFO, "Datapanel apache logger (DAL) build %s %s", __DATE__, __TIME__);

	for(;;) {
		wait_input();

		if (std::cin.fail() || !std::cin.good()) {
		    writelog(LOG_WARNING, "input pipe seems to be broken - exiting");
		    commit_bandwidth();
		    return;
		}

		std::getline(std::cin, line);

		if (line.length() > 0) {
#ifdef DEBUG_ENABLED
			std::cout << line << std::endl;
#endif

			if (log_type == "error") result = parse_error_line (&line, &data);
			else 					 result = parse_common_line(&line, &data);

			if (result < 0) {
				data.domain = "default";
				data.line.assign(line.c_str());
			}

			if (avail_domains.count(data.domain) == 0) {
				writelog(LOG_WARNING, "lookup failed for domain %s", data.domain.c_str());
				data.domain = "default";
				data.line.assign(line.c_str());
			}

			if (avail_domains.count(data.domain) == 0) {
				writelog(LOG_ERROR, "DEFAULT lookup not working!");
				continue;
			}

			d_data 				= &avail_domains[data.domain];
			bw_data.domain_id   = d_data->domain_id;
			bw_data.id   		= 0;
			bw_data.sent 		= data.sent;
			bw_data.rcvd 		= data.rcvd;
			bw_data.time 		= 0;
			bw_data.date 		= timetostr(data.time);
			bw_data.count		= 1;

			/**
			 * We need to check two dates. The actual date and the date that the
			 * log line has, because we must be able to parse old logfiles without
			 * counting them as today. But we still need to distribute the lines to
			 * today's date files, because old log files are probably already packed
			 * by logrotate.
			 */

			if (result == 0 && bw_data.date > current_date) {
				writelog(LOG_INFO, "date is now %d", bw_data.date);

				/**
				 * This actually does some unnecessary closing of log handles but currently
				 * it's acceptable
				 */
				do_cleanup();

				current_date = bw_data.date;
			}

			write_apache_log(&data, d_data);

			writelog(LOG_DEBUG, "domain=%s sent=%d rcvd=%d uid=%d gid=%d date=%d",
								data.domain.c_str(), data.rcvd, data.sent, d_data->uid, d_data->gid,
								bw_data.date);

			if (log_type != "error") update_bandwidth(&bw_data);
		}
	}
}

int init_conn() {
	if (sqlite3_open(sqlite_db.c_str(), &conn)) {
		writelog(LOG_ERROR, "failed to open database: %s", sqlite3_errmsg(conn));
		conn = NULL;
		return 1;
	}

	return 0;
}

void read_config() {
	log_root 		= config.read<string>("log_root");
	log_type 		= config.read<string>("log_type");
	default_logfile	= config.read<string>("apache_logfile");
	dal_logfile		= config.read<string>("logfile");
	sqlite_db		= config.read<string>("sqlite_db");
	commit_count	= config.read<unsigned int>("commit_count", DEFAULT_COMMIT_COUNT);
	commit_hard_limit	= config.read<unsigned int>("commit_hard_limit", DEFAULT_COMMIT_HARD_LIMIT);
	global_logfile	= config.read<string>("global_logfile");

	std::istringstream s(config.read<string>("userlog_perm"));
	s >> std::oct >> userlog_perm;

	conn = NULL;

	dal_loghandler = new std::fstream();
	dal_loghandler->open(dal_logfile.c_str(), std::fstream::out|std::fstream::app);
}

void init_config(int argc, char *argv[]) {
	string config_file;
	std::ifstream in;

	if (argc == 2) {
		config_file.assign(argv[1]);
	}
	else {
		config_file.assign(DEFAULT_CONFIG);
	}

	in.open(config_file.c_str());

	if (!in) throw ConfigFile::file_not_found(config_file);

	in >> config;
}

void signal_handler(int sig) {
	if (is_commiting && !want_down) {
		writelog(LOG_INFO, "Received SIGINT or SIGTERM while commiting - waiting for complete");
		want_down = true;
	}
	else {
		if (want_down) {
			writelog(LOG_WARNING, "Forced shutdown - possible data corruption");
		}
		else {
			writelog(LOG_DEBUG, "Received SIGINT or SIGTERM - commiting");
			commit_bandwidth();
		}

		exit(1);
	}
}

} // end namespace

using namespace dal;

int main(int argc, char *argv[]) {
	want_down    = false;
	is_commiting = false;

	try {
		init_config(argc, argv);
		read_config();
		init_conn();

		signal(SIGINT, signal_handler);
		signal(SIGTERM, signal_handler);

		run();

		return EXIT_SUCCESS;
	}
	catch(std::exception& e) {
		writelog(LOG_FATAL, e.what());
	}
	catch(ConfigFile::key_not_found& e) {
		writelog(LOG_FATAL, "missing configuration parameter `%s`", e.key.c_str());
	}
	catch(ConfigFile::file_not_found& e) {
		writelog(LOG_FATAL, "failed to open configuration file `%s`", e.filename.c_str());
	}

	return EXIT_FAILURE;
}
