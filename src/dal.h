#ifndef DPAPLOGGER_H_
#define DPAPLOGGER_H_

#include <iostream>
#include <sqlite3.h>
#include <map>
#include <time.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include "includes/ConfigFile/ConfigFile.h"
#include "config.h"

namespace dal {

ConfigFile config;
sqlite3 *conn;
std::string log_root, log_type, default_logfile, sqlite_db, dal_logfile, global_logfile;
__mode_t userlog_perm;
std::map<std::string, int> months;
std::map<int, std::map<int, int> > bw_cache;
std::map<int, struct bandwidth_data> commit_buffer;
std::map<std::string, struct domain_data> avail_domains;
std::fstream *dal_loghandler = NULL;
std::fstream *global_logstream = NULL;
unsigned int commit_requests, commit_count, commit_hard_limit;
bool want_down, is_commiting;

/* This is the MySQL database's vhost_bw table structure */
struct bandwidth_data {
	unsigned int 	id;
	unsigned int 	domain_id;
	unsigned int 	date;
	unsigned long 	rcvd;
	unsigned long 	sent;
	unsigned int    time;
	unsigned int	count;
};

/* Contains information about a domain (virtualhost) log files and
 * handles */
struct domain_loghandles {
	std::fstream *user_handle;
	std::fstream *handle;
	std::string 	*user_logfile;
	std::string 	*logfile;
};

struct domain_data {
	unsigned int 	domain_id;
	string 			domainname;
	string 			basepath;
	uid_t 			uid;
	gid_t 			gid;
	unsigned int	type;
	
	struct domain_loghandles handles;
};

struct line_data {
	unsigned long 	sent;
	unsigned long 	rcvd;
	unsigned int	type;
	
	time_t time;
	string domain;
	string line;
};

void 	wait_input();
bool 	file_exists(string *filename);
void 	writelog(int level, const char * format, ...);
void 	db_error();
void 	init_months();
int 	monthtoint(string month);
int 	timetostr(time_t timestamp, bool short_date);
int 	get_today();
int 	init_data();
void 	update_bandwidth(struct bandwidth_data *data);
void 	write_apache_log(struct line_data *data, struct domain_data *d_data);
time_t 	parse_common_time(string *word);
time_t 	parse_error_time(string *word);
int 	parse_common_line(string *line, struct line_data *data);
int 	parse_error_line(string *line, struct line_data *data);
void 	run();
int 	init_conn();
void 	read_config();
void 	init_config(int argc, char *argv[]);
} // end namespace

using namespace dal;
int 	main(int argc, char *argv[]);

#endif /*DPAPLOGGER_H_*/
