#!/usr/bin/env python

import processingfw.pfwdb as pfwdb 
import re
import sys
import datetime


def print_header():
    print "tid, name, label, status, infotable, start_time, end_time"

def get_start_time(x):
    maxdate = datetime.datetime.now()
    return x['start_time'] or maxdate

def recurs_dump(tasks, tids, indent=''):
    tlist = [tasks[t] for t in tids]
    for taskd in sorted(tlist, key=lambda x:get_start_time(x), reverse=False):
        print indent, taskd['id'], taskd['root_task_id'], taskd['name'], taskd['label'], taskd['status'], taskd['info_table'],
        if taskd['start_time'] is None:
            print "    -  -     :  :  ",
        else:
            print taskd['start_time'].strftime("%Y-%m-%d %H:%M:%S"), 
        if taskd['end_time'] is None:
            print "    -  -     :  :  "
        else:
            print taskd['end_time'].strftime("%Y-%m-%d %H:%M:%S")
        
        if len(taskd['children']) > 0:
            recurs_dump(tasks, taskd['children'], indent+'    ')

run = sys.argv[1]
m = re.search("(\S+)_r([^p]+)p([^_]+)", run)
if m is None:
    print "Error:  cannot parse run", run
    sys.exit(1)

unitname = m.group(1)
reqnum = m.group(2)
attnum = m.group(3)

print "unitname =", unitname
print "reqnum =", reqnum
print "attnum =", attnum

dbh = pfwdb.PFWDB()

# get the run info
attinfo = dbh.get_attempt_info(reqnum, unitname, attnum)
attid = attinfo['task_id']
print "attempt task id = ", attid
    

sql = """WITH alltasks (id, parent_task_id, root_task_id, lvl, name, info_table, start_time, end_time, status, exec_host,label) AS (SELECT c.id, c.parent_task_id, c.root_task_id, 1, c.name, c.info_table, c.start_time, c.end_time, c.status, c.exec_host,c.label FROM task c WHERE c.id=%s UNION ALL SELECT r.id, r.parent_task_id, r.root_task_id, a.lvl+1, r.name, r.info_table, r.start_time, r.end_time, r.status, r.exec_host,r.label FROM alltasks a INNER JOIN task r ON r.parent_task_id = a.id) SELECT id, parent_task_id, root_task_id, lvl, name, info_table, start_time, end_time, status, exec_host, label FROM alltasks ORDER BY lvl ASC""" % (attid)

#sql = "select * from task where root_task_id=%d order by id" % attid

print sql
curs = dbh.cursor()
curs.execute(sql)
desc = [d[0].lower() for d in curs.description]
print desc
tasks = {}
for line in curs:
    d = dict(zip(desc, line)) 
    d['children'] = []
    tasks[d['id']] = d
    if d['parent_task_id'] is not None:
        tasks[d['parent_task_id']]['children'].append(d['id'])


#for tid, taskd in sorted(tasks.items()):
#    print tid, taskd

print_header()
recurs_dump(tasks, [attid], '')
